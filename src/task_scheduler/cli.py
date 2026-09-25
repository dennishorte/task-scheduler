"""sched — command-line client for the task scheduler."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request


def _base() -> tuple[str, str]:
    url = os.environ.get("SCHED_URL", "http://127.0.0.1:8377").rstrip("/")
    token = os.environ.get("SCHED_TOKEN", "")
    if not token:
        sys.exit("SCHED_TOKEN is not set")
    return url, token


def req(method: str, path: str, body: dict | None = None) -> dict:
    url, token = _base()
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(
        url + path, data=data, method=method,
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(r, timeout=310) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            detail = json.loads(e.read()).get("error", {})
            sys.exit(f"error {e.code}: {detail.get('code')}: {detail.get('message')}")
        except json.JSONDecodeError:
            sys.exit(f"error {e.code}: {e.read()[:500]!r}")


def out(obj) -> None:
    print(json.dumps(obj, indent=2))


def cmd_submit(a) -> None:
    if a.file:
        payload = json.loads(open(a.file).read())
        out(req("POST", "/v1/tasks", payload))
        return
    if not a.repo_url or not a.repo_ref:
        sys.exit("submit requires --repo-url and --repo-ref "
                 "(file:// URLs work for local repos)")
    task = {
        "project": a.project,
        "command": a.command,
        "cores": a.cores,
        "repo": {"url": a.repo_url, "ref": a.repo_ref},
    }
    if a.subdir:
        task["workdir_subdir"] = a.subdir
    if a.mem_mb is not None:
        task["mem_mb"] = a.mem_mb
    if a.est is not None:
        task["est_seconds"] = a.est
    if a.timeout is not None:
        task["timeout_seconds"] = a.timeout
    if a.label:
        task["labels"] = a.label
    if a.key:
        task["idempotency_key"] = a.key
    if a.artifact:
        task["artifact_patterns"] = a.artifact
    if a.env:
        task["env"] = dict(e.split("=", 1) for e in a.env)
    if a.secret_env:
        task["secret_env"] = dict(e.split("=", 1) for e in a.secret_env)
    if a.setup:
        task["setup_command"] = a.setup
    out(req("POST", "/v1/tasks", task))


def cmd_status(a) -> None:
    out(req("GET", f"/v1/tasks/{a.id}"))


def cmd_list(a) -> None:
    q = f"/v1/tasks?limit={a.limit}"
    if a.project:
        q += f"&project={a.project}"
    if a.status:
        q += f"&status={a.status}"
    if a.label:
        q += f"&label={a.label}"
    res = req("GET", q)
    for t in res["tasks"]:
        print(f"{t['id']} {t['status']:10} {t['project']:14} "
              f"cores={t['cores']:<3} {t['command'][:70]}")


def cmd_logs(a) -> None:
    stream = "stderr" if a.stderr else "stdout"
    offset = 0
    while True:
        res = req("GET", f"/v1/tasks/{a.id}/log?stream={stream}&offset={offset}"
                         + (f"&tail={a.tail}" if a.tail and offset == 0 else ""))
        if res["data"]:
            print(res["data"], end="")
        offset = res["next_offset"]
        if not a.follow:
            break
        task = req("GET", f"/v1/tasks/{a.id}")
        if task["status"] in ("succeeded", "failed", "timeout", "cancelled", "lost"):
            break
        time.sleep(2)


def cmd_wait(a) -> None:
    if a.label:
        out(req("GET", f"/v1/tasks/wait?label={a.label}&timeout={a.timeout}"))
    else:
        out(req("GET", f"/v1/tasks/{a.id}/wait?timeout={a.timeout}"))


def cmd_cancel(a) -> None:
    if a.label:
        out(req("POST", "/v1/tasks/cancel", {"label": a.label}))
    else:
        out(req("POST", f"/v1/tasks/{a.id}/cancel", {}))


def cmd_queue(a) -> None:
    out(req("GET", "/v1/queue"))


def main() -> None:
    p = argparse.ArgumentParser(prog="sched")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("submit")
    s.add_argument("--project", required=not "--file" in sys.argv)
    s.add_argument("--command", "-c")
    s.add_argument("--repo-url", help="repo URL to clone/fetch (required)")
    s.add_argument("--repo-ref", help="branch, tag, or commit to check out")
    s.add_argument("--subdir", help="subdirectory inside the worktree")
    s.add_argument("--cores", type=int, default=1)
    s.add_argument("--mem-mb", type=int, default=None)
    s.add_argument("--est", type=float, default=None, help="est_seconds")
    s.add_argument("--timeout", type=float, default=None)
    s.add_argument("--label", action="append")
    s.add_argument("--key", help="idempotency key")
    s.add_argument("--artifact", action="append", help="artifact glob")
    s.add_argument("--env", action="append", help="K=V (visible in API/UI)")
    s.add_argument("--secret-env", action="append",
                   help="K=V (runtime only, never persisted/shown)")
    s.add_argument("--setup", help="setup command")
    s.add_argument("--file", "-f", help="JSON file (single task or {'tasks':[...]})")
    s.set_defaults(f=cmd_submit)

    s = sub.add_parser("status"); s.add_argument("id"); s.set_defaults(f=cmd_status)
    s = sub.add_parser("list")
    s.add_argument("--project"); s.add_argument("--status"); s.add_argument("--label")
    s.add_argument("--limit", type=int, default=50)
    s.set_defaults(f=cmd_list)
    s = sub.add_parser("logs"); s.add_argument("id")
    s.add_argument("--stderr", action="store_true")
    s.add_argument("--tail", type=int)
    s.add_argument("--follow", "-f", action="store_true")
    s.set_defaults(f=cmd_logs)
    s = sub.add_parser("wait")
    s.add_argument("id", nargs="?")
    s.add_argument("--label"); s.add_argument("--timeout", type=float, default=300)
    s.set_defaults(f=cmd_wait)
    s = sub.add_parser("cancel")
    s.add_argument("id", nargs="?"); s.add_argument("--label")
    s.set_defaults(f=cmd_cancel)
    s = sub.add_parser("queue"); s.set_defaults(f=cmd_queue)

    a = p.parse_args()
    if a.cmd == "submit" and not a.file and not a.command:
        p.error("submit needs --command or --file")
    if a.cmd in ("wait", "cancel") and not a.id and not a.label:
        p.error(f"{a.cmd} needs an id or --label")
    a.f(a)


if __name__ == "__main__":
    main()
