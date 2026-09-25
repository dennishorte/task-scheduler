---
name: task-scheduler
description: Submit and monitor CPU tasks on the shared scheduler (HTTP API)
---

# task-scheduler

A shared HTTP task scheduler that arbitrates CPU time across projects on this
machine. Every task runs in a private `git worktree` materialized from
`repo.url` + `repo.ref`. Use it instead of running CPU-heavy work locally.

## Endpoints

Base URL: `SCHED_URL` (default `http://<scheduler-host>:8377`). All `/v1/*`
requests need `Authorization: Bearer <scheduler-project-token>`.

**Get the host, token, and project name from your project's `.env` file**
(`SCHED_URL`, `SCHED_TOKEN`, `SCHED_PROJECT`) — don't hardcode them, and don't
commit them elsewhere. If `.env` lacks them, stop and ask rather than
guessing.

- `POST /v1/tasks` — submit one task object or `{"tasks": [...]}` (batch is
  atomic; `idempotency_key` **required** on batches)
- `GET /v1/tasks/{id}` — task record (status, exit_code, verdict, timing)
- `GET /v1/tasks?status=&label=&limit=` — list
- `GET /v1/tasks/{id}/wait?timeout=300` — block until terminal state
- `GET /v1/tasks/wait?label=<l>&timeout=` — block until ALL tasks with label
  are terminal → `{"all_terminal", "by_status"}`
- `GET /v1/tasks/{id}/log?stream=stdout|stderr&offset=&tail=` — logs
  (`offset` for incremental follow, `tail` = last N bytes)
- `GET /v1/tasks/{id}/files?root=artifacts|workdir&prefix=` — list files
- `GET /v1/tasks/{id}/files/{path}?root=artifacts` — download file
- `POST /v1/tasks/{id}/cancel`, `POST /v1/tasks/cancel {"label": "..."}`
- `GET /v1/queue` — pool utilization; `GET /v1/health`
- `POST /v1/datasets/{name}?version=V` — upload a `.tar`/`.tar.gz` bundle
  (raw body; `?replace=1` overwrites, `?sha256=<hex>` verifies the archive
  server-side → 422 on mismatch)
- `GET /v1/datasets` — list names/versions/sizes
- `GET /v1/datasets/{name}/{version}/manifest` — the bundle's manifest.json
- `DELETE /v1/datasets/{name}/{version}` — remove a version (409 if a running
  task is using it)

## Task object

```json
{
  "project": "<scheduler-project>",                 // SCHED_PROJECT from .env —
                                                    // must match your token
  "command": ".venv/bin/python scripts/x.py --flag",
  "repo": {"url": "git@github.com:org/repo.git", "ref": "abc123"},   // REQUIRED
  //   The service clones/fetches the URL and creates a private git worktree
  //   at <ref> per task. SSH form for private repos — HTTPS has no
  //   credentials. The ref must exist on the remote (push first — local-only
  //   commits are invisible). Branch names resolve at dispatch time — use a
  //   SHA to pin an exact commit.
  "workdir_subdir": "subdir",                  // optional, inside the worktree
  "env": {"K": "V"},                           // persisted + shown in UI
  "secret_env": {"API_KEY": "..."},            // runtime only — never stored
                                             // in DB, API, or UI
  "cores": 1,                                  // ENFORCED via cgroup CPU quota
  "mem_mb": 2048,                              // ENFORCED via cgroup MemoryMax —
                                             // exceeding it OOM-kills the task
  "est_seconds": 1081,                         // expected WALL time
  "timeout_seconds": null,                     // explicit: honored up to 36h;
                                             // absent: clamp(3×est, 600s, 36h)
  "labels": ["batch:myrun", "key:tw05"],
  "idempotency_key": "myrun-tw05-<commit>",
  "artifact_patterns": ["out/**", "*.png"],    // collected into artifacts/
  "verdict_pattern": "MATCH|NEW_DIFF|...",     // last match in stdout tail → verdict
  "datasets": [{"name": "earth-real-data",     // shared read-only bundles;
               "version": "latest",            //   omit for latest
               "env": "WORLDGEN_DATA_DIR",     //   env var ← resolved dir
               "writable": false}]             //   true → per-task writable copy
}
```

Status values: `queued running succeeded failed timeout cancelled lost`.
Check `verdict`, `exit_code`, and `stderr_tail` (last ~500 chars of stderr —
usually enough to triage a failure without fetching the log) on completion.

## Conventions (important)

- **`repo.url` + `repo.ref` is the only task mode.** The service gives your
  task a private `git worktree`, isolated from whatever other agents do to
  any shared checkout. **The ref resolves when the task *starts*, not when
  you submit** — `ref: "main"` queued for hours runs whatever `main` points
  to at dispatch. For an exact pin, submit the full **SHA**; a SHA never
  moves, but **it must be pushed** — the service fetches from the remote, so
  commits that exist only in your local clone are invisible. `file://` URLs
  only work for repos on the **scheduler host's own disk** — from another
  machine, use a URL the host can reach (`git@…`, `https://…`).
- **Honest resource declarations**: `cores`/`mem_mb` are enforced through
  cgroup v2 scopes when available (CPU quota + MemoryMax — exceeding mem_mb
  OOM-kills your task). If you run `pytest -n 7`, declare `cores: 7` or you
  will be throttled. If the service reports `no_cgroup_enforcement`, they're
  hints only.
- **`est_seconds` = the whole in-process task.** A determinism check that runs
  the sim twice internally is one task with the summed estimate. Estimates
  drive longest-first ordering and default timeouts — use your baseline data.
  If omitted, the service falls back to a learned median of your recent
  same-label completions (then a global default) — explicit is still better.
- **Secrets go in `secret_env`, not `env`.** `env` is persisted in the DB and
  rendered in the dashboard; `secret_env` is passed to your process and never
  stored anywhere readable through the API.
- **Task env**: the service prepends `~/.local/bin`, `~/bin`, `~/.cargo/bin`
  (config `task_path_extra`), so `uv`, `cargo`, etc. resolve by name.
  `PYTHONUNBUFFERED=1` and `stdbuf -oL -eL` wrapping keep output flowing so
  killed/timed-out tasks still leave their tail in the logs. `TMPDIR` and
  `TASK_TMPDIR` point at a per-task scratch dir cleaned with the task —
  **use them instead of `/tmp`**, which is shared across all tasks.
- **Private repos: use the SSH URL form** (`git@github.com:org/repo.git`) —
  the service's ssh keys are available, but HTTPS URLs to private repos have
  no credential helper and fail with "could not read Username".
- **Staged pipelines**: submit each stage as a labeled batch, then
  `GET /v1/tasks/wait?label=...` → check `verdict`/`exit_code` per task →
  submit the next stage only if the gate passed. Do not submit downstream
  work before gates clear.
- **Idempotency**: always set `idempotency_key` (e.g.
  `<batch>-<item>-<commit>`). Retrying a submit with the same key+payload is
  free; same key + different payload → `409`.
- **Polling**: prefer `wait` endpoints. If you poll, use ≥5 s intervals —
  never tight-loop `GET /tasks/{id}`.
- **Artifacts**: declare `artifact_patterns` for any files you need back
  (images, CSVs). Fetch via the files endpoint. Artifacts share the record
  retention window — collect what you need promptly.
- **Sharding**: for embarrassingly-parallel work, submit per-item tasks (not
  pre-sharded bundles) so the pool packs them by estimate. Shard granularity
  should match your atomic unit of work.

## Datasets (shared read-only data)

Large inputs that aren't in git (generated worlds, fixtures, corpora) live in
`datasets/<name>/<version>/` under the scheduler's data dir. Upload a tar:

```bash
tar -cf bundle.tar -C <dir> .            # or .tar.gz
curl -X POST "$SCHED_URL/v1/datasets/earth-real-data?version=7" \
  -H "Authorization: Bearer $SCHED_TOKEN" \
  -H "Content-Type: application/x-tar" --data-binary @bundle.tar
```

Every upload bumps the `latest` pointer. A task declaring
`"datasets": [{"name": "earth-real-data", "env": "WORLDGEN_DATA_DIR"}]` fails
fast at admit if the dataset is missing, and gets `WORLDGEN_DATA_DIR` set to
the resolved version dir. `SCHED_DATASETS_DIR` (the root) is always in the
task env. Pin `"version": "7"` when a run must be reproducible; omit for
latest. Version resolution happens at admit — like `repo.ref`, `latest`
means "at dispatch," not "at submit." Carry a `manifest.json` in the bundle
(version + sha256s) if your project verifies content; `?sha256=` on upload
verifies the *archive* integrity server-side.

**Datasets are read-only.** If your workload writes into its data root,
declare `"writable": true` — the task gets a private copy of the version
(CoW-reflink where the filesystem supports it) which is deleted when the
task ends. Never write directly to a shared dataset dir.

## Cookbook

```bash
# pytest lives in [project.optional-dependencies] dev:
uv run --extra dev python -m pytest tests/unit -m "not slow"

# datasets the task mutates → writable copy, no cp+chmod boilerplate:
"datasets": [{"name": "ds", "env": "DATA", "writable": true}]

# scratch space → $TASK_TMPDIR (per-task, cleaned), never shared /tmp:
mktemp -d "$TASK_TMPDIR/work.XXXX"
```
