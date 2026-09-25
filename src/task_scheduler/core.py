"""Core scheduler: dispatcher (fair-share + backfill), runner, recovery, janitor."""
from __future__ import annotations

import asyncio
import glob
import hashlib
import json
import os
import re
import secrets
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import Config
from .db import DB, TERMINAL

PHASE_EXIT = {"setup": "setup_exit", "command": "exit_code"}
VERDICT_TAIL_BYTES = 256 * 1024
KILL_GRACE_S = 10.0
MONITOR_INTERVAL_S = 2.0
DISPATCH_INTERVAL_S = 5.0
JANITOR_INTERVAL_S = 3600.0


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime | None = None) -> str:
    return (dt or utcnow()).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def new_task_id() -> str:
    return f"t_{int(time.time() * 1000):x}{secrets.token_hex(8)}"


def proc_stat(pid: int) -> dict | None:
    """Parse /proc/{pid}/stat -> {'state','pgrp','starttime'} or None."""
    try:
        data = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    try:
        fields = data.rsplit(")", 1)[1].split()
        return {
            "state": fields[0],
            "pgrp": int(fields[2]),
            "starttime": int(fields[19]),
        }
    except (IndexError, ValueError):
        return None


def pg_members(pgid: int) -> list[int]:
    members = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        st = proc_stat(int(entry.name))
        if st and st["pgrp"] == pgid:
            members.append(int(entry.name))
    return members


class Scheduler:
    def __init__(self, config: Config, db: DB):
        self.cfg = config
        self.db = db
        self.degraded: set[str] = set()
        self.wake = asyncio.Event()
        self.started_at = iso()
        self._tasks: list[asyncio.Task] = []
        self._stopping = False
        # Popen handles for processes spawned this run — kept so we can reap
        # them (poll()); without this, killed tasks linger as zombies.
        self._procs: dict[str, subprocess.Popen] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        # Dedicated pool for blocking work (dispatch admits, finalize, janitor,
        # systemd calls). stop() shuts it down so in-flight threads can't touch
        # the DB after close.
        self._executor = ThreadPoolExecutor(max_workers=6,
                                            thread_name_prefix="sched")
        # Serialize git ops per source repo — concurrent fetch/worktree-add on
        # the same repo can collide on ref locks.
        self._repo_locks: dict[str, threading.Lock] = {}
        self._repo_locks_mu = threading.Lock()
        self.cgroups = False

    def _repo_lock(self, src: Path) -> threading.Lock:
        with self._repo_locks_mu:
            return self._repo_locks.setdefault(str(src), threading.Lock())

    async def _thread(self, fn, *args):
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, fn, *args)

    # ------------------------------------------------------------------ paths

    def task_dir(self, task_id: str) -> Path:
        return self.cfg.data_dir / "tasks" / task_id

    # ------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self.cgroups = self._detect_cgroups()
        if self.cfg.enforce_cgroups and not self.cgroups:
            self.degraded.add("no_cgroup_enforcement")
            print("[scheduler] systemd-run scopes unavailable — "
                  "cores/mem_mb are unenforced hints", file=sys.stderr)
        (self.cfg.data_dir / "tasks").mkdir(parents=True, exist_ok=True)
        self.recover()
        self._tasks = [
            asyncio.create_task(self._dispatch_loop()),
            asyncio.create_task(self._monitor_loop()),
            asyncio.create_task(self._janitor_loop()),
        ]

    async def stop(self) -> None:
        self._stopping = True
        self.wake.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        # Wait for in-flight thread work before the caller closes the DB —
        # cancelled tasks don't interrupt an already-running worker.
        await asyncio.get_running_loop().run_in_executor(
            None, lambda: self._executor.shutdown(
                wait=True, cancel_futures=True))

    def _detect_cgroups(self) -> bool:
        """Probe whether systemd-run --user --scope works here."""
        if not self.cfg.enforce_cgroups or not shutil.which("systemd-run"):
            return False
        try:
            r = subprocess.run(
                ["systemd-run", "--user", "--scope", "--collect", "true"],
                capture_output=True, timeout=15,
            )
            return r.returncode == 0
        except (OSError, subprocess.SubprocessError):
            return False

    def _scope_unit(self, task_id: str, phase: str) -> str:
        return f"sched-{task_id}-{phase}"

    def _systemd_stop(self, task_id: str, phase: str) -> None:
        try:
            subprocess.run(
                ["systemctl", "--user", "stop",
                 f"{self._scope_unit(task_id, phase)}.scope"],
                capture_output=True, timeout=60,
            )
        except (OSError, subprocess.SubprocessError):
            pass

    # ------------------------------------------------------------- dispatcher

    async def _dispatch_loop(self) -> None:
        while not self._stopping:
            try:
                # In a worker thread — git fetch/clone in _admit must not
                # stall the event loop.
                await self._thread(self.dispatch_once)
            except Exception as e:  # keep the loop alive
                print(f"[dispatch] error: {e}", file=sys.stderr)
            self.wake.clear()
            try:
                await asyncio.wait_for(self.wake.wait(), DISPATCH_INTERVAL_S)
            except asyncio.TimeoutError:
                pass

    def _usage(self) -> tuple[int, int, dict[str, int]]:
        running = self.db.running_tasks()
        used_slots = sum(t["cores"] for t in running)
        used_mem = sum(t["mem_mb"] for t in running)
        by_project: dict[str, int] = {}
        for t in running:
            by_project[t["project"]] = by_project.get(t["project"], 0) + t["cores"]
        return used_slots, used_mem, by_project

    def dispatch_once(self) -> None:
        if "low_disk" in self.degraded:
            return
        queued = self.db.queued_tasks()
        if not queued:
            return

        used_slots, used_mem, running_by_proj = self._usage()
        free_slots = self.cfg.slots - used_slots
        free_mem = self.cfg.mem_slots_mb - used_mem

        competing = {t["project"] for t in queued}
        total_w = sum(
            (self.cfg.project(p).weight if self.cfg.project(p) else 1.0)
            for p in competing
        )
        share = {
            p: max(
                1,
                int(
                    self.cfg.slots
                    * (self.cfg.project(p).weight if self.cfg.project(p) else 1.0)
                    / total_w
                ),
            )
            for p in competing
        }

        admitted: set[str] = set()
        for enforce_share in (True, False):  # pass 1: fair share; pass 2: backfill
            for task in queued:
                if task["id"] in admitted:
                    continue
                if task["cores"] > free_slots or task["mem_mb"] > free_mem:
                    continue
                proj = task["project"]
                if enforce_share and (
                    running_by_proj.get(proj, 0) + task["cores"] > share[proj]
                ):
                    continue
                try:
                    self._admit(task)
                except Exception as e:
                    detail = getattr(e, "stderr", "") or str(e)
                    detail = (detail or str(e)).strip()[:500]
                    self.db.update_task(
                        task["id"], status="failed", ended_at=iso(),
                        error=f"admit failed: {detail}",
                    )
                    self.db.add_event(task["id"], "queued", "failed", detail)
                    admitted.add(task["id"])
                    continue
                admitted.add(task["id"])
                free_slots -= task["cores"]
                free_mem -= task["mem_mb"]
                running_by_proj[proj] = running_by_proj.get(proj, 0) + task["cores"]

    # ----------------------------------------------------------------- runner

    def _spawn_phase(self, task: dict, phase: str, command: str) -> None:
        """Write the phase stub script and spawn it via setsid."""
        td = self.task_dir(task["id"])
        td.mkdir(parents=True, exist_ok=True)
        exitfile = PHASE_EXIT[phase]
        workdir = Path(task["workdir"])
        logcap = f"{shlex.quote(sys.executable)} -m task_scheduler.logcap"
        script = td / f"run_{phase}.sh"
        script.write_text(
            "#!/usr/bin/env bash\n"
            f"echo $$ > {shlex.quote(str(td / 'pgid'))}\n"
            f"cd {shlex.quote(str(workdir))} || {{"
            f" echo 'workdir unavailable: {workdir}' >> {shlex.quote(str(td / 'stderr.log'))};"
            f" echo 127 > {shlex.quote(str(td / exitfile))}; exit 127; }}\n"
            f"echo '===== {phase} phase =====' >> {shlex.quote(str(td / 'stdout.log'))}\n"
            f"bash -c {shlex.quote(command)}"
            f" > >({logcap} {shlex.quote(str(td / 'stdout.log'))} {self.cfg.log_max_bytes})"
            f" 2> >({logcap} {shlex.quote(str(td / 'stderr.log'))} {self.cfg.log_max_bytes})\n"
            f"echo $? > {shlex.quote(str(td / exitfile))}\n"
        )
        script.chmod(0o755)

        env = dict(os.environ)
        env.update(json.loads(task["env_json"] or "{}"))
        # Datasets resolved at admit inject their dir via the named env var.
        for s in json.loads(task["datasets_json"] or "[]"):
            if s.get("env") and s.get("resolved_dir"):
                env[s["env"]] = s["resolved_dir"]
        env["SCHED_DATASETS_DIR"] = str(self.datasets_root())
        # Host tools (uv, cargo, …) live outside the service's minimal PATH —
        # prepend configured dirs so tasks can call them by name.
        extra = [str(Path(p).expanduser()) for p in self.cfg.task_path_extra]
        env["PATH"] = ":".join([*extra, env.get("PATH", "")])
        secret_env = td / "secret_env.json"
        if secret_env.exists():
            try:
                env.update(json.loads(secret_env.read_text()))
            except (OSError, json.JSONDecodeError):
                pass
        runner_log = open(td / "runner.log", "ab")

        argv = ["setsid", "bash", str(script)]
        if self.cgroups:
            # The scope IS the isolation boundary — no setsid (it would fork
            # if already a group leader, breaking pgid/leader tracking).
            # Scopes default to KillMode=control-group: SIGTERM then SIGKILL
            # of every descendant on stop.
            argv = [
                "systemd-run", "--user", "--scope", "--collect",
                f"--unit={self._scope_unit(task['id'], phase)}",
                f"--property=CPUQuota={task['cores'] * 100}%",
                f"--property=MemoryMax={task['mem_mb'] * 1024 * 1024}",
                "--property=MemorySwapMax=0",
                "bash", str(script),
            ]
        proc = subprocess.Popen(
            argv,
            cwd=td,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=runner_log,
            stderr=subprocess.STDOUT,
        )
        runner_log.close()  # child holds its own dup of the fd
        self._procs[task["id"]] = proc
        st = proc_stat(proc.pid) or {}
        self.db.update_task(
            task["id"],
            status="running",
            phase=phase,
            pgid=proc.pid,
            leader_pid=proc.pid,
            leader_start=st.get("starttime"),
            phase_started_at=iso(),
            started_at=task["started_at"] or iso(),
            intended_status=None,
        )
        self.db.add_event(task["id"], task["status"], "running", f"phase={phase}")

    # -------------------------------------------------------------- repo mode

    def _repo_src_dir(self, spec: dict) -> Path:
        """Local path of the repo used as worktree source."""
        if "url" in spec:
            key = hashlib.sha256(spec["url"].encode()).hexdigest()[:16]
            return self.cfg.data_dir / "repos" / key
        return Path(spec["path"])

    def _cached_clone(self, url: str) -> Path:
        """Maintain a cached clone in repos_dir for url-mode tasks."""
        repos = self.cfg.data_dir / "repos"
        repos.mkdir(parents=True, exist_ok=True)
        key = hashlib.sha256(url.encode()).hexdigest()[:16]
        dest = repos / key
        if not dest.exists():
            subprocess.run(
                ["git", "clone", url, str(dest)],
                check=True, capture_output=True, text=True, timeout=1800,
            )
        else:
            subprocess.run(
                ["git", "-C", str(dest), "fetch", "--all", "--tags", "--prune"],
                check=True, capture_output=True, text=True, timeout=1800,
            )
        return dest

    def _materialize_worktree(self, task: dict) -> None:
        """Create a detached git worktree at the pinned ref for a repo task."""
        spec = json.loads(task["repo_source"])
        td = self.task_dir(task["id"])
        td.mkdir(parents=True, exist_ok=True)
        wt = td / "worktree"
        # Lock covers fetch + worktree-add so a prune/repack mid-add can't
        # yank the ref out from under us.
        src_key = self._repo_src_dir(spec)
        with self._repo_lock(src_key):
            src = (self._cached_clone(spec["url"]) if "url" in spec
                   else Path(spec["path"]))
            subprocess.run(
                ["git", "-C", str(src), "worktree", "add", "--detach",
                 str(wt), task["repo_ref"]],
                check=True, capture_output=True, text=True, timeout=300,
            )
        wd = wt / spec["subdir"] if spec.get("subdir") else wt
        self.db.update_task(task["id"], workdir=str(wd))
        task["workdir"] = str(wd)

    def _cleanup_worktree(self, task: dict) -> None:
        if not task["repo_source"]:
            return
        try:
            spec = json.loads(task["repo_source"])
        except json.JSONDecodeError:
            return
        src = self._repo_src_dir(spec)
        wt = self.task_dir(task["id"]) / "worktree"
        with self._repo_lock(src):
            for args in (["worktree", "remove", "--force", str(wt)],
                         ["worktree", "prune"]):
                try:
                    subprocess.run(
                        ["git", "-C", str(src), *args],
                        capture_output=True, timeout=120,
                    )
                except (OSError, subprocess.SubprocessError):
                    pass

    def datasets_root(self) -> Path:
        return self.cfg.data_dir / "datasets"

    def resolve_dataset(self, name: str, version: str | None) -> Path:
        """Resolve a dataset name+version to its directory."""
        root = self.datasets_root() / name
        if not version or version == "latest":
            link = root / "latest"
            if link.is_symlink():
                t = root / os.readlink(link)
                if t.is_dir():
                    return t
            raise FileNotFoundError(f"dataset {name}: no versions uploaded")
        d = root / version
        if not d.is_dir():
            raise FileNotFoundError(f"dataset {name}@{version} not found")
        return d

    def _materialize_datasets(self, task: dict) -> None:
        """Resolve dataset specs at admit; fail fast if any are missing."""
        specs = json.loads(task["datasets_json"] or "[]")
        if not specs:
            return
        for s in specs:
            d = self.resolve_dataset(s["name"], s.get("version"))
            s["resolved_version"] = d.name
            s["resolved_dir"] = str(d)
        js = json.dumps(specs)
        self.db.update_task(task["id"], datasets_json=js)
        task["datasets_json"] = js

    def _admit(self, task: dict) -> None:
        if task["repo_source"]:
            self._materialize_worktree(task)
        self._materialize_datasets(task)
        phase = "setup" if task["setup_command"] else "command"
        command = task["setup_command"] if phase == "setup" else task["command"]
        self._spawn_phase(task, phase, command)

    def _phase_timeout(self, task: dict) -> float:
        if task["phase"] == "setup":
            return task["setup_timeout_seconds"] or self.cfg.setup_timeout_seconds
        return task["timeout_seconds"]

    # ---------------------------------------------------------------- monitor

    async def _monitor_loop(self) -> None:
        while not self._stopping:
            await asyncio.sleep(MONITOR_INTERVAL_S)
            try:
                for task in self.db.running_tasks():
                    await self._check_running(task)
            except Exception as e:
                print(f"[monitor] error: {e}", file=sys.stderr)

    async def _check_running(self, task: dict) -> None:
        # Reap the child if we spawned it this run — a zombie still shows up
        # in /proc and would look alive forever.
        proc = self._procs.get(task["id"])
        if proc is not None:
            proc.poll()
        td = self.task_dir(task["id"])
        phase = task["phase"] or "command"
        exitfile = td / PHASE_EXIT[phase]

        if exitfile.exists():
            try:
                code = int(exitfile.read_text().strip())
            except ValueError:
                code = None
            if phase == "setup":
                if code == 0:
                    await self._thread(
                        self._spawn_phase, task, "command", task["command"])
                else:
                    await self._thread(
                        self._finalize, task, code, "failed")
            else:
                await self._thread(self._finalize, task, code)
            return

        deadline = parse_ts(task["phase_started_at"]) + timedelta(
            seconds=self._phase_timeout(task)
        )
        if utcnow() > deadline and not task["intended_status"]:
            await self._kill(task, "timeout")
            return

        if not self._leader_alive(task):
            if task["intended_status"]:
                await self._thread(
                    self._finalize, task, self._read_exit(task),
                    task["intended_status"])
            else:
                await self._thread(
                    self._finalize, task, self._read_exit(task), "failed",
                    "leader process died without writing exit status")

    def _read_exit(self, task: dict) -> int | None:
        for name in PHASE_EXIT.values():
            f = self.task_dir(task["id"]) / name
            if f.exists():
                try:
                    return int(f.read_text().strip())
                except ValueError:
                    return None
        return None

    def _leader_alive(self, task: dict) -> bool:
        pid = task["leader_pid"]
        if not pid:
            return False
        st = proc_stat(pid)
        if not st or st["state"] == "Z":
            return False
        if task["leader_start"] is not None and st["starttime"] != task["leader_start"]:
            return False
        return True

    # ------------------------------------------------------------------- kill

    async def _kill(self, task: dict, target_status: str) -> None:
        """Persist intent, then TERM -> grace -> KILL the process group."""
        task = self.db.get_task(task["id"]) or task
        if task["status"] != "running" or task["intended_status"]:
            return
        self.db.add_event(task["id"], "running", target_status, "signal sent")
        self.db.update_task(task["id"], intended_status=target_status)

        pgid = task["pgid"]
        phase = task["phase"] or "command"
        if self.cgroups:
            # Scope stop SIGTERMs the whole cgroup — descendants included,
            # even ones that escaped the process group via setsid — then
            # SIGKILLs after TimeoutStopUSec.
            await self._thread(self._systemd_stop, task["id"], phase)

        if self._leader_alive(task):
            try:
                os.killpg(pgid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass

        for _ in range(int(KILL_GRACE_S * 5)):
            await asyncio.sleep(0.2)
            td = self.task_dir(task["id"])
            if (td / PHASE_EXIT.get(phase, "exit_code")).exists():
                break
            if not self._leader_alive(task) and not pg_members(pgid):
                break

        for pid in pg_members(pgid):
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

    def cancel(self, task: dict) -> dict:
        """Called from the API. Returns {'ok': bool, 'message': str}."""
        if task["status"] == "queued":
            self.db.update_task(task["id"], status="cancelled", ended_at=iso())
            self.db.add_event(task["id"], "queued", "cancelled", "cancelled by user")
            return {"ok": True, "message": "cancelled"}
        if task["status"] == "running":
            asyncio.get_running_loop().create_task(self._kill(task, "cancelled"))
            return {"ok": True, "message": "kill signalled"}
        return {"ok": False, "message": f"task already {task['status']}"}

    # --------------------------------------------------------------- finalize

    def _sweep_pgroup(self, task: dict) -> None:
        """Kill leftover members of the task's process group (leaked children)."""
        if self.cgroups:
            for ph in ("setup", "command"):
                self._systemd_stop(task["id"], ph)
        pgid = task["pgid"]
        if not pgid:
            return
        floor = (task["leader_start"] or 0) - 2
        for pid in pg_members(pgid):
            st = proc_stat(pid)
            if st is None or st["starttime"] < floor:
                continue  # guard against pgid reuse
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

    def _scan_verdict(self, task: dict, td: Path) -> str | None:
        pattern = task["verdict_pattern"]
        if not pattern:
            return None
        log = td / "stdout.log"
        if not log.exists():
            return None
        try:
            with open(log, "rb") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                f.seek(max(0, size - VERDICT_TAIL_BYTES))
                tail = f.read().decode("utf-8", errors="replace")
        except OSError:
            return None
        try:
            matches = re.findall(pattern, tail)
        except re.error:
            return None
        if not matches:
            return None
        m = matches[-1]
        v = m if isinstance(m, str) else m[0]
        return v[:256]  # verdicts are short tokens; cap pathological matches

    def _collect_artifacts(self, task: dict, td: Path) -> tuple[list[str], list[str]]:
        patterns = json.loads(task["artifact_patterns_json"] or "[]")
        if not patterns:
            return [], []
        workdir = Path(task["workdir"]).resolve()
        adir = td / "artifacts"
        adir.mkdir(parents=True, exist_ok=True)
        collected, skipped = [], []
        total = 0
        for pat in patterns:
            for match in sorted(glob.glob(str(workdir / pat), recursive=True)):
                p = Path(match)
                try:
                    rp = p.resolve()
                except OSError:
                    continue
                if not rp.is_file():
                    continue
                if not str(rp).startswith(str(workdir) + os.sep):
                    skipped.append(f"{pat}: escapes workdir")
                    continue
                size = rp.stat().st_size
                if size > self.cfg.artifact_file_max_bytes:
                    skipped.append(f"{rp.name}: exceeds per-file cap")
                    continue
                if total + size > self.cfg.artifact_total_max_bytes:
                    skipped.append(f"{rp.name}: exceeds total cap")
                    continue
                rel = rp.relative_to(workdir)
                dest = adir / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                try:
                    shutil.copy2(rp, dest)
                except OSError:
                    continue
                total += size
                collected.append(str(rel))
        return collected, skipped

    def _finalize(
        self,
        task: dict,
        code: int | None,
        forced_status: str | None = None,
        error: str | None = None,
    ) -> None:
        td = self.task_dir(task["id"])
        self._procs.pop(task["id"], None)
        self._sweep_pgroup(task)
        status = forced_status or ("succeeded" if code == 0 else "failed")
        verdict = self._scan_verdict(task, td)
        artifacts, skipped = self._collect_artifacts(task, td)
        self._cleanup_worktree(task)
        err = error
        if skipped:
            err = (err + "; " if err else "") + "artifacts skipped: " + "; ".join(skipped)
        self.db.update_task(
            task["id"],
            status=status,
            exit_code=code,
            ended_at=iso(),
            verdict=verdict,
            artifacts_json=json.dumps(artifacts),
            intended_status=None,
            error=err,
        )
        self.db.add_event(task["id"], "running", status, f"exit_code={code}")
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self.wake.set)
        else:
            self.wake.set()

    # --------------------------------------------------------------- recovery

    def recover(self) -> None:
        """Reconcile tasks left in 'running' across a service restart."""
        for task in self.db.running_tasks():
            td = self.task_dir(task["id"])
            phase = task["phase"] or "command"
            exitfile = td / PHASE_EXIT[phase]

            # 1. Completed while we were down — never report these 'lost'.
            if exitfile.exists():
                try:
                    code = int(exitfile.read_text().strip())
                except ValueError:
                    code = None
                if phase == "setup" and code == 0:
                    self._spawn_phase(task, "command", task["command"])
                else:
                    forced = "failed" if phase == "setup" else None
                    self._finalize(task, code, forced)
                continue

            # 2. Deadline expired during downtime — downtime doesn't launder timeouts.
            deadline = parse_ts(task["phase_started_at"] or task["started_at"]) + timedelta(
                seconds=self._phase_timeout(task)
            )
            if utcnow() > deadline:
                self.db.update_task(task["id"], intended_status="timeout")
                self.db.add_event(task["id"], "running", "timeout", "expired during downtime")
                for pid in pg_members(task["pgid"] or -1):
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except (ProcessLookupError, PermissionError):
                        pass
                self._finalize(task, self._read_exit(task), "timeout")
                continue

            # 3. Leader alive with verified identity — resume monitoring.
            if self._leader_alive(task):
                continue

            # 4. We killed it but crashed before finalizing — honor the intent.
            if task["intended_status"]:
                self._finalize(task, self._read_exit(task), task["intended_status"])
                continue

            # 5. Gone.
            self.db.update_task(task["id"], status="lost", ended_at=iso(),
                                error="process not found after scheduler restart")
            self.db.add_event(task["id"], "running", "lost", "pgid dead, no exit file")

    # ---------------------------------------------------------------- janitor

    async def _janitor_loop(self) -> None:
        while not self._stopping:
            try:
                await self._thread(self._janitor_once)
            except Exception as e:
                print(f"[janitor] error: {e}", file=sys.stderr)
            await asyncio.sleep(JANITOR_INTERVAL_S)

    def _janitor_once(self) -> None:
        # Disk watermark — pause admissions when low.
        try:
            st = os.statvfs(self.cfg.data_dir)
            free_gb = (st.f_bavail * st.f_frsize) / (1024**3)
            if free_gb < self.cfg.min_free_gb:
                self.degraded.add("low_disk")
            else:
                self.degraded.discard("low_disk")
        except OSError:
            pass

        # Retention: drop terminal tasks + their dirs past the window.
        cutoff = iso(utcnow() - timedelta(days=self.cfg.retention_days))
        old = self.db.query(
            "SELECT id, repo_source FROM tasks WHERE status IN ('succeeded','failed','timeout','cancelled','lost')"
            " AND ended_at < ?",
            (cutoff,),
        )
        prune_srcs: set[str] = set()
        for row in old:
            if row["repo_source"]:
                try:
                    prune_srcs.add(str(self._repo_src_dir(json.loads(row["repo_source"]))))
                except json.JSONDecodeError:
                    pass
            shutil.rmtree(self.task_dir(row["id"]), ignore_errors=True)
            self.db.delete_task(row["id"])

        # Prune stale worktree admin entries — cached clones and any repo that
        # served as a worktree source for a just-deleted task.
        prune_srcs.update(
            str(d) for d in (self.cfg.data_dir / "repos").glob("*") if d.is_dir()
        )
        for src in prune_srcs:
            with self._repo_lock(Path(src)):
                try:
                    subprocess.run(
                        ["git", "-C", src, "worktree", "prune"],
                        capture_output=True, timeout=120,
                    )
                except (OSError, subprocess.SubprocessError):
                    pass

        # Artifact retention may be shorter than record retention.
        art_days = self.cfg.artifact_retention_days
        if art_days is not None and art_days < self.cfg.retention_days:
            acut = iso(utcnow() - timedelta(days=art_days))
            rows = self.db.query(
                "SELECT id FROM tasks WHERE artifacts_json IS NOT NULL "
                "AND artifacts_json != '[]' AND ended_at < ?",
                (acut,),
            )
            for row in rows:
                shutil.rmtree(self.task_dir(row["id"]) / "artifacts",
                              ignore_errors=True)
                self.db.update_task(row["id"], artifacts_json="[]")
