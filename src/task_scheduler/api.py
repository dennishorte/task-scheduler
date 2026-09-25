"""FastAPI HTTP layer: auth, validation, endpoints."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import secrets
import shutil
import tarfile
import tempfile
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from . import __version__
from .config import Config, load_config
from .core import Scheduler, iso, new_task_id, parse_ts, utcnow
from .db import DB, TERMINAL

KNOWN_FIELDS = {
    "project", "command", "workdir", "repo", "workdir_subdir", "setup_command",
    "env", "secret_env", "cores", "mem_mb", "est_seconds", "timeout_seconds",
    "labels", "idempotency_key", "artifact_patterns", "verdict_pattern",
    "datasets",
}
DATASET_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
STATIC_DIR = Path(__file__).parent / "static"


def api_error(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(
        status_code=status, detail={"code": code, "message": message}
    )


def task_public(task: dict) -> dict:
    """Serialize a DB row to the API shape."""
    out = dict(task)
    for jf in ("env_json", "labels_json", "artifact_patterns_json", "artifacts_json"):
        key = jf.removesuffix("_json")
        try:
            out[key] = json.loads(out.pop(jf) or ("[]" if "patterns" in jf or "labels" in jf or "artifacts" in jf else "{}"))
        except (json.JSONDecodeError, TypeError):
            out[key] = None
    out.pop("payload_hash", None)
    out.pop("leader_pid", None)
    out.pop("leader_start", None)
    try:
        out["secret_env_keys"] = json.loads(out.get("secret_env_keys") or "[]")
    except json.JSONDecodeError:
        out["secret_env_keys"] = []
    rs = out.pop("repo_source", None)
    if rs:
        try:
            spec = json.loads(rs)
            spec["ref"] = out.get("repo_ref")
            out["repo"] = spec
        except (json.JSONDecodeError, TypeError):
            pass
    try:
        out["datasets"] = json.loads(out.pop("datasets_json") or "[]")
    except (json.JSONDecodeError, TypeError):
        out["datasets"] = []
    return out


def validate_task(data: dict, cfg: Config, in_batch: bool,
                  default_est: float | None = None) -> tuple[dict | None, list[str]]:
    """Validate + normalize a submit payload. Returns (row|None, errors)."""
    errors: list[str] = []
    if not isinstance(data, dict):
        return None, ["task must be a JSON object"]

    unknown = set(data) - KNOWN_FIELDS
    if unknown:
        errors.append(f"unknown fields: {sorted(unknown)}")

    project = data.get("project")
    if not isinstance(project, str) or not project:
        errors.append("project: required string")

    command = data.get("command")
    if not isinstance(command, str) or not command.strip():
        errors.append("command: required non-empty string")

    workdir = data.get("workdir")
    repo = data.get("repo")
    resolved_workdir = None
    repo_source = None
    repo_ref = None
    if workdir:
        errors.append(
            "workdir mode removed; submit repo:{url, ref} "
            "(file:// URLs work for local repos)")
    if repo:
        if not isinstance(repo, dict):
            errors.append("repo must be an object")
        else:
            url, path, ref = repo.get("url"), repo.get("path"), repo.get("ref")
            if path:
                errors.append(
                    "repo.path removed; use repo.url "
                    "(file:// works for local repos)")
            elif not isinstance(url, str) or not url.strip():
                errors.append("repo.url must be a non-empty string")
            else:
                repo_source = {"url": url.strip()}
            if not isinstance(ref, str) or not ref.strip():
                errors.append("repo.ref: required string (branch, tag, or commit)")
            else:
                repo_ref = ref.strip()
    elif not workdir:
        errors.append("repo:{url, ref} is required")

    subdir = data.get("workdir_subdir")
    if subdir is not None:
        if not isinstance(subdir, str) or os.path.isabs(subdir) or ".." in subdir.split("/"):
            errors.append("workdir_subdir must be a relative path without '..'")
        elif repo_source is not None:
            repo_source["subdir"] = subdir

    setup = data.get("setup_command")
    if setup is not None and not isinstance(setup, str):
        errors.append("setup_command must be a string")

    env = data.get("env", {})
    if not isinstance(env, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in env.items()
    ):
        errors.append("env must be an object of string:string")

    secret_env = data.get("secret_env")
    if secret_env is not None and not (
        isinstance(secret_env, dict) and all(
            isinstance(k, str) and isinstance(v, str)
            for k, v in secret_env.items()
        )
    ):
        errors.append("secret_env must be an object of string:string")

    cores = data.get("cores", 1)
    if not isinstance(cores, int) or cores < 1:
        errors.append("cores must be an integer >= 1")
    elif cores > cfg.slots:
        errors.append(f"cores ({cores}) exceeds pool slots ({cfg.slots})")

    proj_cfg = cfg.project(project) if isinstance(project, str) else None
    mem_default = (proj_cfg.default_mem_mb if proj_cfg else None) or cfg.default_mem_mb
    mem_mb = data.get("mem_mb", mem_default)
    if not isinstance(mem_mb, int) or mem_mb < 1:
        errors.append("mem_mb must be an integer >= 1")
    elif mem_mb > cfg.mem_slots_mb:
        errors.append(f"mem_mb ({mem_mb}) exceeds memory budget ({cfg.mem_slots_mb})")

    est = data.get("est_seconds")
    if est is None:
        est_eff = float(default_est or cfg.default_est_seconds)
    elif not isinstance(est, (int, float)) or est < 0:
        errors.append("est_seconds must be a non-negative number")
        est_eff = float(default_est or cfg.default_est_seconds)
    else:
        est_eff = float(est)

    timeout = data.get("timeout_seconds")
    if timeout is not None and (not isinstance(timeout, (int, float)) or timeout < 0):
        errors.append("timeout_seconds must be a non-negative number")
        timeout = None
    if timeout is not None:
        # Explicit values are honored — only the ceiling applies.
        timeout_eff = min(float(timeout), cfg.max_timeout_seconds)
    else:
        timeout_eff = min(max(3.0 * est_eff, cfg.timeout_floor_seconds),
                          cfg.max_timeout_seconds)

    labels = data.get("labels", [])
    if not isinstance(labels, list) or not all(isinstance(l, str) for l in labels):
        errors.append("labels must be a list of strings")

    idem = data.get("idempotency_key")
    if in_batch and not idem:
        errors.append("idempotency_key is required on batch submits")
    if idem is not None and not isinstance(idem, str):
        errors.append("idempotency_key must be a string")

    datasets = data.get("datasets", [])
    if not isinstance(datasets, list):
        errors.append("datasets must be a list")
    else:
        for i, d in enumerate(datasets):
            if not isinstance(d, dict):
                errors.append(f"datasets[{i}]: must be an object")
                continue
            name = d.get("name")
            if not isinstance(name, str) or not DATASET_NAME_RE.match(name or ""):
                errors.append(f"datasets[{i}].name: required ({DATASET_NAME_RE.pattern})")
            ver = d.get("version", "latest")
            if not isinstance(ver, str) or not DATASET_NAME_RE.match(ver or ""):
                errors.append(f"datasets[{i}].version: invalid")
            ev = d.get("env")
            if ev is not None and not (
                isinstance(ev, str) and ENV_NAME_RE.match(ev)
            ):
                errors.append(f"datasets[{i}].env: invalid env var name")

    patterns = data.get("artifact_patterns", [])
    if not isinstance(patterns, list) or not all(isinstance(p, str) for p in patterns):
        errors.append("artifact_patterns must be a list of strings")
    else:
        for p in patterns:
            if os.path.isabs(p) or ".." in p.split("/"):
                errors.append(f"artifact_pattern escapes workdir: {p}")

    verdict = data.get("verdict_pattern", proj_cfg.verdict_pattern if proj_cfg else None)
    if verdict is not None:
        if not isinstance(verdict, str):
            errors.append("verdict_pattern must be a string")
        else:
            try:
                re.compile(verdict)
            except re.error as e:
                errors.append(f"verdict_pattern invalid regex: {e}")

    if errors:
        return None, errors

    payload = {k: v for k, v in data.items() if k != "idempotency_key"}
    payload_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()

    return {
        "id": new_task_id(),
        "project": project,
        "status": "queued",
        "phase": None,
        "command": command,
        "workdir": resolved_workdir,
        "repo_ref": repo_ref,
        "repo_source": json.dumps(repo_source) if repo_source else None,
        "env_json": json.dumps(env),
        "cores": cores,
        "mem_mb": mem_mb,
        "est_seconds": est_eff,
        "timeout_seconds": timeout_eff,
        "setup_command": setup,
        "setup_timeout_seconds": cfg.setup_timeout_seconds,
        "labels_json": json.dumps(labels),
        "idempotency_key": idem,
        "payload_hash": payload_hash,
        "secret_env_keys": json.dumps(sorted(secret_env)) if secret_env else None,
        "artifact_patterns_json": json.dumps(patterns),
        "datasets_json": json.dumps(datasets) if datasets else None,
        "artifacts_json": None,
        "verdict_pattern": verdict,
        "verdict": None,
        "exit_code": None,
        "pgid": None,
        "leader_pid": None,
        "leader_start": None,
        "intended_status": None,
        "submitted_at": iso(),
        "started_at": None,
        "phase_started_at": None,
        "ended_at": None,
        "error": None,
    }, []


def learned_est_seconds(db: DB, project: str, labels: list[str]) -> float | None:
    """Median wall-clock of recent succeeded tasks — same label first, then
    project-wide. Returns None with < 3 samples."""
    from statistics import median

    def med(rows: list[dict]) -> float | None:
        ds = []
        for r in rows:
            try:
                ds.append((parse_ts(r["ended_at"]) -
                           parse_ts(r["started_at"])).total_seconds())
            except Exception:
                pass
        return float(median(ds)) if len(ds) >= 3 else None

    base = ("SELECT started_at, ended_at FROM tasks WHERE project = ? "
            "AND status = 'succeeded' AND started_at IS NOT NULL "
            "AND ended_at IS NOT NULL {extra} ORDER BY ended_at DESC LIMIT 20")
    if labels:
        m = med(db.query(
            base.format(extra="AND labels_json LIKE ?"),
            (project, f'%"{labels[0]}"%')))
        if m is not None:
            return m
    return med(db.query(base.format(extra=""), (project,)))


def create_app(config_path: str | Path) -> FastAPI:
    cfg = load_config(config_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        cfg.data_dir.mkdir(parents=True, exist_ok=True)
        db = DB(cfg.data_dir / "scheduler.db")
        sched = Scheduler(cfg, db)
        await sched.start()
        app.state.cfg = cfg
        app.state.db = db
        app.state.sched = sched
        yield
        await sched.stop()
        db.close()

    app = FastAPI(title="task-scheduler", version=__version__, lifespan=lifespan)
    bearer = HTTPBearer(auto_error=False)

    @app.exception_handler(HTTPException)
    async def http_exc_handler(_req: Request, exc: HTTPException):
        detail = exc.detail if isinstance(exc.detail, dict) else {"code": "error", "message": str(exc.detail)}
        return JSONResponse(status_code=exc.status_code, content={"error": detail})

    # ------------------------------------------------------------------ auth

    def principal(cred: HTTPAuthorizationCredentials | None = Depends(bearer)):
        if cred is None:
            raise api_error(401, "unauthorized", "missing bearer token")
        token = cred.credentials
        if cfg.admin_token and token == cfg.admin_token:
            return ("admin", None)
        for name, p in cfg.projects.items():
            if token == p.token:
                return ("project", name)
        raise api_error(401, "unauthorized", "invalid token")

    def scope_check(princ, project: str):
        if princ[0] == "project" and princ[1] != project:
            raise api_error(403, "forbidden", f"token scoped to project '{princ[1]}'")

    def scoped_task(request: Request, task_id: str) -> dict:
        db: DB = request.app.state.db
        task = db.get_task(task_id)
        if not task:
            # Allow unambiguous id prefixes — agents/CLIs often work with
            # truncated ids.
            rows = db.query("SELECT * FROM tasks WHERE id LIKE ?",
                            (task_id + "%",))
            if len(rows) == 1:
                task = rows[0]
            elif len(rows) > 1:
                raise api_error(409, "conflict",
                                f"id prefix '{task_id}' is ambiguous")
        if not task:
            raise api_error(404, "not_found", f"no task {task_id}")
        scope_check(request.state.princ, task["project"])
        return task

    # ---------------------------------------------------------------- submit

    @app.post("/v1/tasks")
    async def submit(request: Request, princ=Depends(principal)):
        request.state.princ = princ
        try:
            body = await request.json()
        except Exception:
            raise api_error(400, "validation", "body must be JSON")

        db: DB = request.app.state.db
        is_batch = isinstance(body, dict) and "tasks" in body
        items = body["tasks"] if is_batch else [body]
        if not isinstance(items, list) or not items:
            raise api_error(400, "validation", "tasks must be a non-empty list")
        if len(items) > cfg.max_batch_size:
            raise api_error(400, "validation", f"batch exceeds max_batch_size={cfg.max_batch_size}")

        rows, all_errors = [], {}
        secrets: dict[str, dict] = {}
        for i, item in enumerate(items):
            default_est = None
            if isinstance(item, dict) and item.get("est_seconds") is None:
                labels = item.get("labels")
                default_est = learned_est_seconds(
                    db, item.get("project", ""),
                    labels if isinstance(labels, list) else [])
            row, errors = validate_task(item, cfg, in_batch=is_batch,
                                        default_est=default_est)
            if errors:
                all_errors[i] = errors
                continue
            scope_check(princ, row["project"])
            rows.append((i, row))
            if item.get("secret_env"):
                secrets[row["id"]] = item["secret_env"]
        if all_errors:
            raise api_error(400, "validation", json.dumps({"items": all_errors}))

        results, created = [], 0
        new_rows = []
        for i, row in rows:
            key = row["idempotency_key"]
            if key:
                existing = db.by_idempotency_key(row["project"], key)
                if existing:
                    if existing["payload_hash"] == row["payload_hash"]:
                        results.append({"index": i, "task_id": existing["id"],
                                        "status": existing["status"], "replayed": True})
                        continue
                    raise api_error(
                        409, "conflict",
                        f"idempotency_key '{key}' already used with a different payload",
                    )
            new_rows.append(row)
            results.append({"index": i, "task_id": row["id"], "status": "queued",
                            "replayed": False})
            created += 1

        if new_rows:
            db.insert_tasks_atomic(new_rows)
            for row in new_rows:
                db.add_event(row["id"], None, "queued", "submitted")
                sec = secrets.get(row["id"])
                if sec:
                    td = request.app.state.sched.task_dir(row["id"])
                    td.mkdir(parents=True, exist_ok=True)
                    sf = td / "secret_env.json"
                    sf.write_text(json.dumps(sec))
                    sf.chmod(0o600)
            request.app.state.sched.wake.set()

        results.sort(key=lambda r: r["index"])
        status_code = 201 if created else 200
        return JSONResponse(status_code=status_code, content={"tasks": results})

    # ------------------------------------------------------------------ list

    @app.get("/v1/tasks")
    async def list_tasks(request: Request, princ=Depends(principal),
                       status: str | None = None, project: str | None = None,
                       label: str | None = None, limit: int = 100,
                       cursor: str | None = None):
        request.state.princ = princ
        db: DB = request.app.state.db
        limit = max(1, min(limit, 500))
        where, params = [], []
        if princ[0] == "project":
            where.append("project = ?")
            params.append(princ[1])
        elif project:
            where.append("project = ?")
            params.append(project)
        if status:
            where.append("status = ?")
            params.append(status)
        if label:
            where.append("labels_json LIKE ?")
            params.append(f'%"{label}"%')
        if cursor:
            try:
                ts, tid = base64.urlsafe_b64decode(cursor).decode().rsplit("|", 1)
            except Exception:
                raise api_error(400, "validation", "bad cursor")
            where.append("(submitted_at < ? OR (submitted_at = ? AND id < ?))")
            params += [ts, ts, tid]
        sql = "SELECT * FROM tasks"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY submitted_at DESC, id DESC LIMIT ?"
        rows = db.query(sql, (*params, limit + 1))
        next_cursor = None
        if len(rows) > limit:
            rows = rows[:limit]
            last = rows[-1]
            next_cursor = base64.urlsafe_b64encode(
                f"{last['submitted_at']}|{last['id']}".encode()
            ).decode()
        return {"tasks": [task_public(t) for t in rows], "next_cursor": next_cursor}

    # ------------------------------------------------------------------- wait
    # NOTE: /v1/tasks/wait must be registered BEFORE /v1/tasks/{task_id}
    # or 'wait' is captured as a task id.

    @app.get("/v1/tasks/wait")
    async def wait_batch(request: Request, princ=Depends(principal),
                         label: str | None = None, timeout: float = 60):
        request.state.princ = princ
        if not label:
            raise api_error(400, "validation", "label is required for batch wait")
        db: DB = request.app.state.db
        deadline = asyncio.get_event_loop().time() + min(timeout, 300)
        where = ["labels_json LIKE ?"]
        params = [f'%"{label}"%']
        if princ[0] == "project":
            where.append("project = ?"); params.append(princ[1])
        while True:
            rows = db.query(
                f"SELECT status FROM tasks WHERE {' AND '.join(where)}", params)
            remaining = sum(1 for r in rows if r["status"] not in TERMINAL)
            by_status: dict[str, int] = {}
            for r in rows:
                by_status[r["status"]] = by_status.get(r["status"], 0) + 1
            if remaining == 0:
                return {"all_terminal": True, "terminal": len(rows),
                        "remaining": 0, "by_status": by_status}
            if asyncio.get_event_loop().time() >= deadline:
                return {"all_terminal": False, "terminal": len(rows) - remaining,
                        "remaining": remaining, "by_status": by_status,
                        "wait_expired": True}
            await asyncio.sleep(1)

    # ----------------------------------------------------------------- detail

    @app.get("/v1/tasks/{task_id}")
    async def get_task(request: Request, task_id: str, princ=Depends(principal)):
        request.state.princ = princ
        task = scoped_task(request, task_id)
        out = task_public(task)
        if task["status"] == "queued":
            db: DB = request.app.state.db
            ahead = db.one(
                "SELECT COUNT(*) AS n FROM tasks WHERE status='queued' AND "
                "(est_seconds > ? OR (est_seconds = ? AND submitted_at < ?))",
                (task["est_seconds"], task["est_seconds"], task["submitted_at"]),
            )
            used_slots, used_mem, _ = request.app.state.sched._usage()
            out["queue_position"] = ahead["n"] + 1
            out["free_slots"] = cfg.slots - used_slots
            out["free_mem_mb"] = cfg.mem_slots_mb - used_mem
        out["events"] = request.app.state.db.events_for(task_id)
        return out

    # ----------------------------------------------------------------- cancel

    @app.post("/v1/tasks/cancel")
    async def cancel_batch(request: Request, princ=Depends(principal)):
        request.state.princ = princ
        body = await request.json() if request.headers.get("content-length") else {}
        db: DB = request.app.state.db
        sched: Scheduler = request.app.state.sched
        ids = body.get("ids") or []
        label = body.get("label")
        where, params = ["status IN ('queued','running')"], []
        if princ[0] == "project":
            where.append("project = ?"); params.append(princ[1])
        elif body.get("project"):
            where.append("project = ?"); params.append(body["project"])
        if ids:
            where.append(f"id IN ({','.join('?' * len(ids))})")
            params += ids
        if label:
            where.append("labels_json LIKE ?"); params.append(f'%"{label}"%')
        if not ids and not label:
            raise api_error(400, "validation", "provide ids or label")
        rows = db.query(f"SELECT * FROM tasks WHERE {' AND '.join(where)}", params)
        results = [sched.cancel(t) | {"task_id": t["id"]} for t in rows]
        return {"results": results}

    @app.post("/v1/tasks/{task_id}/cancel")
    async def cancel_one(request: Request, task_id: str, princ=Depends(principal)):
        request.state.princ = princ
        task = scoped_task(request, task_id)
        res = request.app.state.sched.cancel(task)
        if not res["ok"]:
            raise api_error(409, "conflict", res["message"])
        return res

    # ------------------------------------------------------------------- wait

    async def _wait_task(db: DB, task_id: str, timeout: float) -> dict:
        deadline = asyncio.get_event_loop().time() + min(timeout, 300)
        while True:
            task = db.get_task(task_id)
            if task["status"] in TERMINAL:
                return task_public(task)
            if asyncio.get_event_loop().time() >= deadline:
                out = task_public(task)
                out["wait_expired"] = True
                return out
            await asyncio.sleep(1)

    @app.get("/v1/tasks/{task_id}/wait")
    async def wait_one(request: Request, task_id: str, timeout: float = 60,
                       princ=Depends(principal)):
        request.state.princ = princ
        scoped_task(request, task_id)
        return await _wait_task(request.app.state.db, task_id, timeout)

    # ------------------------------------------------------------------- logs

    @app.get("/v1/tasks/{task_id}/log")
    async def get_log(request: Request, task_id: str, stream: str = "stdout",
                      offset: int = 0, tail: int | None = None,
                      princ=Depends(principal)):
        request.state.princ = princ
        scoped_task(request, task_id)
        if stream not in ("stdout", "stderr"):
            raise api_error(400, "validation", "stream must be stdout|stderr")
        path = request.app.state.sched.task_dir(task_id) / f"{stream}.log"
        if not path.exists():
            return {"data": "", "offset": 0, "next_offset": 0, "size": 0}
        size = path.stat().st_size
        with open(path, "rb") as f:
            if tail is not None:
                offset = max(0, size - tail)
            offset = min(offset, size)
            f.seek(offset)
            data = f.read().decode("utf-8", errors="replace")
        return {"data": data, "offset": offset, "next_offset": size, "size": size}

    # ------------------------------------------------------------------ files

    def _safe_resolve(root: Path, rel: str) -> Path:
        root = root.resolve()
        p = (root / rel).resolve()
        if p != root and not str(p).startswith(str(root) + os.sep):
            raise api_error(403, "forbidden", "path escapes root")
        return p

    @app.get("/v1/tasks/{task_id}/files")
    async def list_files(request: Request, task_id: str, root: str = "artifacts",
                         prefix: str = "", princ=Depends(principal)):
        request.state.princ = princ
        task = scoped_task(request, task_id)
        sched: Scheduler = request.app.state.sched
        if root == "artifacts":
            base = sched.task_dir(task_id) / "artifacts"
        elif root == "workdir":
            base = Path(task["workdir"]) if task["workdir"] else None
            if base is None:
                raise api_error(400, "validation", "task has no workdir")
        else:
            raise api_error(400, "validation", "root must be artifacts|workdir")
        if not base.exists():
            return {"files": []}
        start = _safe_resolve(base, prefix) if prefix else base.resolve()
        files, capped = [], False
        it = start.rglob("*") if start.is_dir() else iter([start])
        for p in it:
            if len(files) >= 1000:
                capped = True
                break
            if p.is_file():
                st = p.stat()
                files.append({"path": str(p.relative_to(base.resolve())),
                              "size": st.st_size,
                              "mtime": iso(
                                  datetime.fromtimestamp(
                                      st.st_mtime, timezone.utc))})
        files.sort(key=lambda f: f["path"])
        return {"files": files, "capped": capped}

    @app.get("/v1/tasks/{task_id}/files/{path:path}")
    async def get_file(request: Request, task_id: str, path: str,
                       root: str = "artifacts", princ=Depends(principal)):
        request.state.princ = princ
        task = scoped_task(request, task_id)
        sched: Scheduler = request.app.state.sched
        if root == "artifacts":
            base = sched.task_dir(task_id) / "artifacts"
        elif root == "workdir":
            base = Path(task["workdir"]) if task["workdir"] else None
            if base is None:
                raise api_error(400, "validation", "task has no workdir")
        else:
            raise api_error(400, "validation", "root must be artifacts|workdir")
        p = _safe_resolve(base, path)
        if not p.is_file():
            raise api_error(404, "not_found", f"no file {path}")
        return FileResponse(p)

    # -------------------------------------------------------------- datasets
    # Named, versioned read-only data bundles shared across tasks. Upload a
    # tar archive; tasks reference {"name", "version"?, "env"?} and the
    # resolved directory is injected as an env var at admit time.

    def datasets_root() -> Path:
        return cfg.data_dir / "datasets"

    def dataset_versions(name: str) -> list[str]:
        root = datasets_root() / name
        if not root.is_dir():
            return []
        return sorted(
            d.name for d in root.iterdir()
            if d.is_dir() and not d.is_symlink() and not d.name.startswith(".")
        )

    def dataset_latest(name: str) -> str | None:
        link = datasets_root() / name / "latest"
        try:
            return os.readlink(link)
        except OSError:
            return None

    def dir_bytes(path: Path) -> int:
        total = 0
        for p in path.rglob("*"):
            if p.is_file():
                total += p.stat().st_size
        return total

    @app.post("/v1/datasets/{name}", status_code=201)
    async def upload_dataset(request: Request, name: str,
                             version: str | None = None,
                             replace: bool = False,
                             princ=Depends(principal)):
        request.state.princ = princ
        if not DATASET_NAME_RE.match(name):
            raise api_error(400, "validation",
                            f"bad dataset name ({DATASET_NAME_RE.pattern})")
        version = version or f"v{int(time.time())}"
        if not DATASET_NAME_RE.match(version):
            raise api_error(400, "validation", "bad version string")
        root = datasets_root() / name
        dest = root / version
        if dest.exists() and not replace:
            raise api_error(409, "conflict",
                          f"dataset {name}@{version} exists (use ?replace=1)")
        root.mkdir(parents=True, exist_ok=True)

        # Stream the archive to disk first — bundles can be ~GBs.
        fd, archive = tempfile.mkstemp(dir=root, prefix=".upload-")
        total = 0
        try:
            with os.fdopen(fd, "wb") as f:
                async for chunk in request.stream():
                    total += len(chunk)
                    if total > cfg.max_dataset_bytes:
                        raise api_error(413, "too_large",
                                        f"dataset exceeds {cfg.max_dataset_bytes} bytes")
                    f.write(chunk)
            tmpdir = root / f".extracting-{secrets.token_hex(6)}"
            tmpdir.mkdir()
            try:
                with tarfile.open(archive, "r|*") as tf:
                    tf.extractall(tmpdir, filter="data")
            except (tarfile.TarError, EOFError) as e:
                shutil.rmtree(tmpdir, ignore_errors=True)
                raise api_error(400, "validation",
                                f"not a tar(.gz/.bz2/.xz) archive: {e}")
            if dest.exists():
                shutil.rmtree(dest)
            tmpdir.rename(dest)
        finally:
            os.unlink(archive)

        latest = root / "latest"
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        latest.symlink_to(version)
        return {"name": name, "version": version, "bytes": total}

    @app.get("/v1/datasets")
    async def list_datasets(request: Request, princ=Depends(principal)):
        request.state.princ = princ
        root = datasets_root()
        out = []
        if root.is_dir():
            for d in sorted(root.iterdir()):
                if not d.is_dir() or d.name.startswith("."):
                    continue
                out.append({
                    "name": d.name,
                    "versions": dataset_versions(d.name),
                    "latest": dataset_latest(d.name),
                    "bytes": dir_bytes(d),
                })
        return {"datasets": out, "root": str(root)}

    @app.get("/v1/datasets/{name}/{version}/manifest")
    async def dataset_manifest(request: Request, name: str, version: str,
                               princ=Depends(principal)):
        request.state.princ = princ
        sched: Scheduler = request.app.state.sched
        try:
            d = sched.resolve_dataset(name, version)
        except FileNotFoundError as e:
            raise api_error(404, "not_found", str(e))
        mf = d / "manifest.json"
        if not mf.is_file():
            raise api_error(404, "not_found", "no manifest.json in bundle")
        return FileResponse(mf)

    @app.delete("/v1/datasets/{name}/{version}")
    async def delete_dataset(request: Request, name: str, version: str,
                             princ=Depends(principal)):
        request.state.princ = princ
        sched: Scheduler = request.app.state.sched
        try:
            d = sched.resolve_dataset(name, version)
        except FileNotFoundError as e:
            raise api_error(404, "not_found", str(e))
        db: DB = request.app.state.db
        rp = str(d.resolve())
        for t in db.running_tasks():
            for s in json.loads(t["datasets_json"] or "[]"):
                if s.get("resolved_dir") == rp:
                    raise api_error(409, "conflict",
                                    f"in use by running task {t['id']}")
        shutil.rmtree(d)
        if dataset_latest(name) == version:
            link = datasets_root() / name / "latest"
            link.unlink(missing_ok=True)
            rest = dataset_versions(name)
            if rest:
                link.symlink_to(rest[-1])
        if not dataset_versions(name):
            shutil.rmtree(datasets_root() / name, ignore_errors=True)
        return {"ok": True}

    # ----------------------------------------------------------------- status

    @app.get("/v1/queue")
    async def queue(request: Request, princ=Depends(principal)):
        request.state.princ = princ
        db: DB = request.app.state.db
        sched: Scheduler = request.app.state.sched
        used_slots, used_mem, _ = sched._usage()
        counts = db.query(
            "SELECT project, status, COUNT(*) AS n, SUM(cores) AS slot_sum "
            "FROM tasks WHERE status IN ('queued','running') GROUP BY project, status"
        )
        projects: dict[str, dict] = {}
        for c in counts:
            p = projects.setdefault(c["project"], {"queued": 0, "running": 0, "running_slots": 0})
            p[c["status"]] = c["n"]
            if c["status"] == "running":
                p["running_slots"] = c["slot_sum"] or 0
        uptime = (utcnow() - parse_ts(sched.started_at)).total_seconds()
        return {
            "slots": {"total": cfg.slots, "used": used_slots,
                      "free": cfg.slots - used_slots},
            "mem_mb": {"total": cfg.mem_slots_mb, "used": used_mem,
                       "free": cfg.mem_slots_mb - used_mem},
            "projects": projects,
            "uptime_seconds": int(uptime),
            "degraded": sorted(sched.degraded),
            "timestamp": iso(),
        }

    @app.get("/v1/health")
    async def health(request: Request):
        sched: Scheduler | None = getattr(request.app.state, "sched", None)
        return {"ok": True, "version": __version__,
                "degraded": sorted(sched.degraded) if sched else []}

    # --------------------------------------------------------------------- UI

    if cfg.ui_enabled:
        @app.get("/", include_in_schema=False)
        async def ui_index():
            return FileResponse(STATIC_DIR / "index.html")

        @app.get("/ui/{path:path}", include_in_schema=False)
        async def ui_spa(path: str):
            return FileResponse(STATIC_DIR / "index.html")

    return app
