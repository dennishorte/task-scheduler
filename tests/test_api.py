"""End-to-end tests against a live app (TestClient + real subprocesses)."""
from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tarfile
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from task_scheduler.api import create_app

H_ADMIN = {"Authorization": "Bearer admintok"}
H_PROJ = {"Authorization": "Bearer testtok"}
H_OTHER = {"Authorization": "Bearer othertok"}
TERMINAL = {"succeeded", "failed", "timeout", "cancelled", "lost"}


@pytest.fixture()
def client(tmp_path):
    cfg = tmp_path / "scheduler.toml"
    cfg.write_text(
        f"""
bind = "127.0.0.1"
data_dir = "{tmp_path}/data"
slots = 4
mem_slots_mb = 8192
default_mem_mb = 256
timeout_floor_seconds = 1
max_timeout_seconds = 3600
min_free_gb = 0
retention_days = 30
task_path_extra = ["{tmp_path}/toolbin"]
admin_token = "admintok"
ui_enabled = false

[projects.testproj]
token = "testtok"

[projects.other]
token = "othertok"
"""
    )
    tool = tmp_path / "toolbin" / "faketool"
    tool.parent.mkdir()
    tool.write_text("#!/bin/sh\necho tool-ran\n")
    tool.chmod(0o755)
    app = create_app(cfg)
    with TestClient(app, raise_server_exceptions=True) as c:
        (tmp_path / "fixture").mkdir()
        _, _, sha = make_git_repo(tmp_path / "fixture")
        c.repo = {"url": f"file://{tmp_path}/fixture/repo", "ref": sha}
        yield c


def submit(client, task, headers=H_PROJ):
    if "workdir" not in task and "repo" not in task:
        task = task | {"repo": client.repo}
    return client.post("/v1/tasks", json=task, headers=headers)


def wait_terminal(client, tid, headers=H_PROJ, timeout=30):
    r = client.get(f"/v1/tasks/{tid}/wait?timeout={timeout}", headers=headers)
    assert r.status_code == 200, r.text
    return r.json()


def test_submit_echo_succeeds(client):
    r = submit(client, {"project": "testproj", "command": "echo hello"})
    assert r.status_code == 201, r.text
    tid = r.json()["tasks"][0]["task_id"]
    t = wait_terminal(client, tid)
    assert t["status"] == "succeeded"
    assert t["exit_code"] == 0
    log = client.get(f"/v1/tasks/{tid}/log?stream=stdout", headers=H_PROJ).json()
    assert "hello" in log["data"]


def test_failing_task(client):
    r = submit(client, {"project": "testproj", "command": "exit 3"})
    tid = r.json()["tasks"][0]["task_id"]
    t = wait_terminal(client, tid)
    assert t["status"] == "failed"
    assert t["exit_code"] == 3


def test_timeout_enforced(client):
    r = submit(client, {"project": "testproj", "command": "sleep 60",
                        "timeout_seconds": 2})
    tid = r.json()["tasks"][0]["task_id"]
    t = wait_terminal(client, tid, timeout=30)
    assert t["status"] == "timeout"


def test_cancel_queued_and_running(client):
    # Fill all 4 slots so the next task queues.
    ids = []
    for _ in range(5):
        r = submit(client, {"project": "testproj", "command": "sleep 30"})
        ids.append(r.json()["tasks"][0]["task_id"])
    st = []
    for _ in range(40):  # poll until dispatch settles (cgroup spawns are slow)
        st = [client.get(f"/v1/tasks/{i}", headers=H_PROJ).json()["status"]
              for i in ids]
        if st.count("running") == 4 and st.count("queued") == 1:
            break
        time.sleep(0.5)
    assert st.count("running") == 4 and st.count("queued") == 1
    queued_id = ids[st.index("queued")]
    r = client.post(f"/v1/tasks/{queued_id}/cancel", headers=H_PROJ)
    assert r.status_code == 200
    assert client.get(f"/v1/tasks/{queued_id}", headers=H_PROJ).json()["status"] == "cancelled"
    # Cancel the rest (running) via label-free batch by ids.
    r = client.post("/v1/tasks/cancel", json={"ids": ids}, headers=H_PROJ)
    assert r.status_code == 200
    for i in ids:
        t = wait_terminal(client, i, timeout=20)
        assert t["status"] in ("cancelled", "timeout")


def test_batch_atomic_reject(client):
    r = client.post("/v1/tasks", json={"tasks": [
        {"project": "testproj", "command": "echo a", "repo": client.repo,
         "idempotency_key": "b1"},
        {"project": "testproj", "command": "echo b", "workdir": "/etc"},
    ]}, headers=H_PROJ)
    assert r.status_code == 400
    assert client.get("/v1/tasks", headers=H_PROJ).json()["tasks"] == []


def test_batch_submit_and_wait_by_label(client):
    tasks = [{"project": "testproj", "command": "echo ok", "repo": client.repo,
              "idempotency_key": f"wave-{i}", "labels": ["batch:wave1"]}
             for i in range(3)]
    r = client.post("/v1/tasks", json={"tasks": tasks}, headers=H_PROJ)
    assert r.status_code == 201
    assert len(r.json()["tasks"]) == 3
    res = client.get("/v1/tasks/wait?label=batch:wave1&timeout=20",
                     headers=H_PROJ).json()
    assert res["all_terminal"] and res["by_status"].get("succeeded") == 3


def test_idempotency(client):
    task = {"project": "testproj", "command": "echo x", "repo": client.repo,
            "idempotency_key": "k1"}
    r1 = submit(client, task)
    tid1 = r1.json()["tasks"][0]["task_id"]
    r2 = submit(client, task)
    assert r2.status_code == 200
    assert r2.json()["tasks"][0]["task_id"] == tid1
    assert r2.json()["tasks"][0]["replayed"] is True
    # same key, different payload → 409
    r3 = submit(client, task | {"command": "echo y"})
    assert r3.status_code == 409


def test_artifacts(client):
    cmd = "echo pngdata > out.png && echo csv > res.csv"
    r = submit(client, {"project": "testproj", "command": cmd,
                        "artifact_patterns": ["out.png", "res.csv"]})
    tid = r.json()["tasks"][0]["task_id"]
    wait_terminal(client, tid)
    files = client.get(f"/v1/tasks/{tid}/files", headers=H_PROJ).json()["files"]
    names = {f["path"] for f in files}
    assert {"out.png", "res.csv"} <= names
    dl = client.get(f"/v1/tasks/{tid}/files/out.png?root=artifacts", headers=H_PROJ)
    assert dl.status_code == 200 and b"pngdata" in dl.content


def test_auth_scoping(client):
    r = submit(client, {"project": "testproj", "command": "echo s"})
    tid = r.json()["tasks"][0]["task_id"]
    # other project cannot see it
    assert client.get(f"/v1/tasks/{tid}", headers=H_OTHER).status_code == 403
    # bad token
    assert client.get(f"/v1/tasks/{tid}",
                      headers={"Authorization": "Bearer nope"}).status_code == 401
    # project token cannot submit for another project
    r = submit(client, {"project": "other", "command": "echo x"})
    assert r.status_code == 403
    # admin can
    r = submit(client, {"project": "other", "command": "echo x"}, headers=H_ADMIN)
    assert r.status_code == 201


def test_verdict_scan(client):
    r = submit(client, {
        "project": "testproj",
        "command": "echo MATCH; echo D1_MISMATCH",
        "verdict_pattern": "MATCH|D1_MISMATCH"})
    tid = r.json()["tasks"][0]["task_id"]
    t = wait_terminal(client, tid)
    assert t["verdict"] == "D1_MISMATCH"  # last match wins


def test_setup_phase(client):
    r = submit(client, {
        "project": "testproj",
        "setup_command": "echo 42 > marker.txt",
        "command": "cat marker.txt"})
    tid = r.json()["tasks"][0]["task_id"]
    t = wait_terminal(client, tid)
    assert t["status"] == "succeeded"
    assert "42" in stdout_of(client, tid)  # setup ran in the same worktree


def test_queue_endpoint(client):
    q = client.get("/v1/queue", headers=H_ADMIN).json()
    assert q["slots"]["total"] == 4
    assert "uptime_seconds" in q


def test_legacy_modes_rejected(client):
    r = submit(client, {"project": "testproj", "command": "echo x",
                        "workdir": "/tmp"})
    assert r.status_code == 400
    r = submit(client, {"project": "testproj", "command": "echo x",
                        "repo": {"path": "/tmp", "ref": "abc123"}})
    assert r.status_code == 400


def test_files_path_escape_blocked(client):
    r = submit(client, {"project": "testproj", "command": "echo x > f.txt"})
    tid = r.json()["tasks"][0]["task_id"]
    wait_terminal(client, tid)
    r = client.get(f"/v1/tasks/{tid}/files/..%2F..%2Fetc%2Fpasswd?root=workdir",
                   headers=H_ADMIN)
    assert r.status_code in (403, 404)


# ------------------------------------------------------------------ repo mode


def make_git_repo(path: Path) -> tuple[Path, str, str]:
    """Repo with two commits differing in x.txt. Returns (repo, sha_v1, sha_v2)."""
    repo = path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-b", "main", str(repo)],
                   check=True, capture_output=True)

    def git(*args):
        return subprocess.run(
            ["git", "-C", str(repo), "-c", "user.email=t@t",
             "-c", "user.name=t", *args],
            check=True, capture_output=True, text=True).stdout.strip()

    (repo / "x.txt").write_text("v1")
    git("add", ".")
    git("commit", "-m", "c1")
    sha1 = git("rev-parse", "HEAD")
    (repo / "x.txt").write_text("v2")
    git("commit", "-am", "c2")
    sha2 = git("rev-parse", "HEAD")
    return repo, sha1, sha2


def stdout_of(client, tid):
    return client.get(f"/v1/tasks/{tid}/log?stream=stdout",
                      headers=H_PROJ).json()["data"]


def test_repo_pinned_ref(client, tmp_path):
    repo, sha1, sha2 = make_git_repo(tmp_path)
    r = submit(client, {"project": "testproj", "command": "cat x.txt",
                        "repo": {"url": f"file://{repo}", "ref": sha1}})
    assert r.status_code == 201, r.text
    tid = r.json()["tasks"][0]["task_id"]
    t = wait_terminal(client, tid)
    assert t["status"] == "succeeded", t.get("error")
    assert "v1" in stdout_of(client, tid)
    assert "v2" not in stdout_of(client, tid)


def test_repo_refs_isolated(client, tmp_path):
    repo, sha1, sha2 = make_git_repo(tmp_path)
    out = {}
    for name, ref in (("old", sha1), ("new", sha2)):
        r = submit(client, {"project": "testproj", "command": "cat x.txt",
                            "repo": {"url": f"file://{repo}", "ref": ref},
                            "idempotency_key": name})
        tid = r.json()["tasks"][0]["task_id"]
        out[name] = wait_terminal(client, tid)
    assert all(t["status"] == "succeeded" for t in out.values())
    # source checkout itself is untouched (still has v2, on main)
    assert (repo / "x.txt").read_text() == "v2"


def test_repo_url_mode(client, tmp_path):
    repo, sha1, sha2 = make_git_repo(tmp_path)
    r = submit(client, {"project": "testproj", "command": "cat x.txt",
                        "repo": {"url": f"file://{repo}", "ref": "main"}})
    assert r.status_code == 201, r.text
    tid = r.json()["tasks"][0]["task_id"]
    t = wait_terminal(client, tid)
    assert t["status"] == "succeeded", t.get("error")
    assert "v2" in stdout_of(client, tid)


def test_repo_worktree_cleaned_up(client, tmp_path):
    repo, sha1, _ = make_git_repo(tmp_path)
    url = f"file://{repo}"
    r = submit(client, {"project": "testproj", "command": "true",
                        "repo": {"url": url, "ref": sha1}})
    tid = r.json()["tasks"][0]["task_id"]
    wait_terminal(client, tid)
    # url mode worktrees come off the cached clone in data_dir/repos/<key>
    key = hashlib.sha256(url.encode()).hexdigest()[:16]
    clone = tmp_path / "data" / "repos" / key
    wt_list = subprocess.run(
        ["git", "-C", str(clone), "worktree", "list", "--porcelain"],
        capture_output=True, text=True).stdout
    assert wt_list.count("worktree ") == 1  # only the clone itself remains


def test_repo_bad_ref_fails_fast(client, tmp_path):
    repo, _, _ = make_git_repo(tmp_path)
    r = submit(client, {"project": "testproj", "command": "true",
                        "repo": {"url": f"file://{repo}", "ref": "does-not-exist"}})
    tid = r.json()["tasks"][0]["task_id"]
    t = wait_terminal(client, tid)
    assert t["status"] == "failed"
    assert "admit failed" in (t["error"] or "")


# ------------------------------------------------- v1.2 gap-filling features


def test_secret_env_not_exposed(client):
    r = submit(client, {
        "project": "testproj",
        "command": "echo key=$MYKEY",
        "secret_env": {"MYKEY": "s3cret-val"}})
    tid = r.json()["tasks"][0]["task_id"]
    t = wait_terminal(client, tid)
    assert t["status"] == "succeeded", t.get("error")
    assert "key=s3cret-val" in stdout_of(client, tid)
    rec = client.get(f"/v1/tasks/{tid}", headers=H_PROJ)
    assert "s3cret-val" not in rec.text  # never rendered
    assert rec.json()["secret_env_keys"] == ["MYKEY"]


def test_timeout_rules_unit(tmp_path):
    from task_scheduler.api import validate_task
    from task_scheduler.config import Config
    cfg = Config()
    base = {"project": "p", "command": "x",
            "repo": {"url": "file:///nonexistent", "ref": "abc123"}}
    row, err = validate_task(base | {"timeout_seconds": 30}, cfg, False)
    assert not err and row["timeout_seconds"] == 30        # explicit not floored
    row, err = validate_task(base | {"est_seconds": 10}, cfg, False)
    assert row["timeout_seconds"] == 600                   # derived floored
    row, err = validate_task(base | {"est_seconds": 3600}, cfg, False)
    assert row["timeout_seconds"] == 10800                 # 3× est
    row, err = validate_task(base | {"timeout_seconds": 99999999}, cfg, False)
    assert row["timeout_seconds"] == 129600                # ceiling applies


def test_learned_est(client):
    for i in range(3):
        r = submit(client, {"project": "testproj", "command": "sleep 1; echo d",
                            "labels": ["batch:learn"],
                            "idempotency_key": f"learn-{i}"})
        assert r.status_code == 201
    res = client.get("/v1/tasks/wait?label=batch:learn&timeout=40",
                     headers=H_PROJ).json()
    assert res["all_terminal"]
    r = submit(client, {"project": "testproj", "command": "echo x",
                        "labels": ["batch:learn2"]})
    tid = r.json()["tasks"][0]["task_id"]
    t = client.get(f"/v1/tasks/{tid}", headers=H_PROJ).json()
    assert t["est_seconds"] < 60  # learned median ~seconds, not the 300s default


def test_task_path_extra(client):
    # A tool only reachable via task_path_extra resolves by name.
    r = submit(client, {"project": "testproj", "command": "faketool"})
    tid = r.json()["tasks"][0]["task_id"]
    t = wait_terminal(client, tid)
    assert t["status"] == "succeeded", t.get("error")
    assert "tool-ran" in stdout_of(client, tid)


# ------------------------------------------------------------------ datasets


def make_tar(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for name, content in files.items():
            data = content.encode()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def upload(client, name, version, files, headers=H_PROJ):
    return client.post(
        f"/v1/datasets/{name}?version={version}",
        content=make_tar(files),
        headers={**headers, "Content-Type": "application/x-tar"},
    )


def test_dataset_upload_and_env_injection(client):
    r = upload(client, "mydata", "v1", {"data.txt": "hello-data"})
    assert r.status_code == 201, r.text
    ds = client.get("/v1/datasets", headers=H_PROJ).json()["datasets"]
    assert ds[0]["name"] == "mydata" and ds[0]["latest"] == "v1"

    r = submit(client, {
        "project": "testproj",
        "command": "cat $DS_DIR/data.txt",
        "datasets": [{"name": "mydata", "env": "DS_DIR"}]})
    tid = r.json()["tasks"][0]["task_id"]
    t = wait_terminal(client, tid)
    assert t["status"] == "succeeded", t.get("error")
    assert "hello-data" in stdout_of(client, tid)
    # no version requested → latest resolved to v1 at admit time, recorded
    spec = t["datasets"][0]
    assert spec.get("version") is None and spec["resolved_version"] == "v1" \
        and spec["resolved_dir"].endswith("v1")


def test_dataset_version_pinning(client):
    upload(client, "ds", "v1", {"f": "one"})
    upload(client, "ds", "v2", {"f": "two"})
    r = submit(client, {
        "project": "testproj", "command": "cat $D/f",
        "datasets": [{"name": "ds", "version": "v1", "env": "D"}]})
    t = wait_terminal(client, r.json()["tasks"][0]["task_id"])
    assert "one" in stdout_of(client, t["id"])


def test_dataset_missing_fails_fast(client):
    r = submit(client, {"project": "testproj", "command": "true",
                        "datasets": [{"name": "nonexistent"}]})
    t = wait_terminal(client, r.json()["tasks"][0]["task_id"])
    assert t["status"] == "failed"
    assert "nonexistent" in (t["error"] or "")


def test_dataset_in_use_not_deleted(client):
    upload(client, "busy", "v1", {"f": "x"})
    r = submit(client, {"project": "testproj", "command": "sleep 30",
                        "datasets": [{"name": "busy", "env": "D"}]})
    tid = r.json()["tasks"][0]["task_id"]
    for _ in range(40):
        if client.get(f"/v1/tasks/{tid}", headers=H_PROJ).json()["status"] == "running":
            break
        time.sleep(0.5)
    r = client.delete("/v1/datasets/busy/v1", headers=H_ADMIN)
    assert r.status_code == 409
    client.post(f"/v1/tasks/{tid}/cancel", headers=H_PROJ)
    wait_terminal(client, tid, timeout=30)


def test_dataset_bad_tar_rejected(client):
    r = client.post("/v1/datasets/bad?version=v1", content=b"not a tar",
                    headers=H_PROJ)
    assert r.status_code == 400
    # path-escape member rejected by the data filter
    r = upload(client, "evil", "v1", {"../escape.txt": "x"})
    assert r.status_code == 400


def test_dataset_writable(client):
    upload(client, "rw", "v1", {"orig.txt": "seed"})
    r = submit(client, {
        "project": "testproj",
        "command": "echo task-wrote > $D/new.txt && cat $D/orig.txt",
        "datasets": [{"name": "rw", "env": "D", "writable": True}]})
    t = wait_terminal(client, r.json()["tasks"][0]["task_id"])
    assert t["status"] == "succeeded", t.get("error")
    # task's write went to a private copy — the shared version is untouched
    dsdir = client.app.state.sched.datasets_root() / "rw" / "v1"
    assert not (dsdir / "new.txt").exists()
    assert (dsdir / "orig.txt").read_text() == "seed"


def test_dataset_upload_sha256(client):
    tar = make_tar({"f": "bytes"})
    good = hashlib.sha256(tar).hexdigest()
    r = client.post(f"/v1/datasets/hashed?version=v1&sha256={good}",
                    content=tar, headers=H_PROJ)
    assert r.status_code == 201
    r = client.post("/v1/datasets/hashed?version=v2&sha256=" + "0" * 64,
                    content=tar, headers=H_PROJ)
    assert r.status_code == 422
    ds = client.get("/v1/datasets", headers=H_PROJ).json()["datasets"]
    assert ds[0]["versions"] == ["v1"]  # failed upload left no version


def test_task_env_defaults(client):
    r = submit(client, {
        "project": "testproj",
        "command": "echo \"$TASK_TMPDIR|$TMPDIR|$PYTHONUNBUFFERED\"; "
                   "touch $TASK_TMPDIR/scratch"})
    tid = r.json()["tasks"][0]["task_id"]
    t = wait_terminal(client, tid)
    assert t["status"] == "succeeded", t.get("error")
    out = stdout_of(client, tid)
    tmpdir, tmpdir2, pyunbuf = out.strip().splitlines()[-1].split("|")
    assert tmpdir == tmpdir2 and pyunbuf == "1"
    assert (Path(tmpdir) / "scratch").exists()


def test_stderr_tail_on_record(client):
    r = submit(client, {"project": "testproj",
                        "command": "echo oops-tail >&2; exit 3"})
    t = wait_terminal(client, r.json()["tasks"][0]["task_id"])
    assert t["status"] == "failed"
    assert "oops-tail" in (t["stderr_tail"] or "")


def test_stats_endpoint(client):
    r = submit(client, {"project": "testproj",
                        "command": "echo ok", "cores": 2, "mem_mb": 512})
    tid = r.json()["tasks"][0]["task_id"]
    wait_terminal(client, tid)
    s = client.get("/v1/stats", headers=H_PROJ).json()
    p = s["projects"]["testproj"]
    assert p["tasks"] == 1 and p["succeeded"] == 1
    assert p["cpu_hours"] >= 0 and p["avg_duration_s"] >= 0
    assert s["pool"]["slots"]["total"] == 4
    assert s["machine"]["cpus"] >= 1
    assert s["totals"]["tasks"] == 1


def test_cgroup_scope_when_available(client):
    sched = client.app.state.sched
    if not sched.cgroups:
        pytest.skip("systemd-run scopes unavailable")
    r = submit(client, {"project": "testproj", "command": "sleep 30"})
    tid = r.json()["tasks"][0]["task_id"]
    st = ""
    for _ in range(40):
        st = client.get(f"/v1/tasks/{tid}", headers=H_PROJ).json()["status"]
        if st == "running":
            break
        time.sleep(0.5)
    assert st == "running"
    units = subprocess.run(
        ["systemctl", "--user", "list-units", "--no-legend", "--type=scope"],
        capture_output=True, text=True).stdout
    assert f"sched-{tid}-command.scope" in units
    client.post(f"/v1/tasks/{tid}/cancel", headers=H_PROJ)
    t = wait_terminal(client, tid, timeout=30)
    assert t["status"] == "cancelled"
