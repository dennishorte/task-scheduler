# Task Scheduler — Design Document

**Status**: v0.2 — revised after independent design review
**Date**: 2026-09-23
**Author**: Devin (with dennis)

## 1. Overview

A single-machine task scheduling service that arbitrates CPU time across multiple
independent projects. Clients are LLM coding agents (occasionally the human owner)
running locally or on other machines. Tasks today are Python-based, dispatched as
shell commands executed in project environments (uv-managed venvs, repos at pinned
commits).

The scheduler owns a pool of **CPU slots** (≈ core count) and a **memory budget**
(≈ RAM headroom) and admits queued tasks when both fit, ordered by a scheduling
policy (longest-first by client-supplied cost estimate). It manages the full task
lifecycle: environment setup, execution, timeout enforcement, log capture,
artifact collection, and status reporting — over a simple HTTP API designed to be
used by agents without specialized client libraries.

## 2. Requirements

### 2.1 Functional

- **Remote dispatch**: agents on other machines submit tasks over HTTP.
- **Task = command + environment**: a shell command, a working directory (existing
  checkout or `repo@ref` materialized by the service), env vars, and resource
  requirements (`cores`, `mem_mb`).
- **Core-weighted scheduling**: tasks declare how many cores they need
  (e.g., a `pytest -n 7` suite declares `cores: 7`); a 64-slot pool admits any mix
  that fits.
- **Memory accounting**: tasks declare `mem_mb`; admission is constrained by a RAM
  budget so the dominant workload (dozens of concurrent Python jobs) cannot push
  the host into OOM-killer roulette. Admission accounting only — enforcement
  (cgroup limits) is deferred.
- **Estimate-driven ordering**: clients supply `est_seconds`; the dispatcher runs
  longest-first (LPT) so heavy shards start early instead of straggling.
- **Per-project fairness**: a dynamic weighted fair-share policy (§5.2) prevents
  one agent's batch from starving other projects — while guaranteeing a lone
  project the whole machine. No static caps to tune.
- **Timeouts**: hard per-task timeouts (default ~3× `est_seconds`, floored and
  clamped), enforced by killing the process group.
- **Artifact collection**: declared output files (PNG/GIF/HDF5/CSV) are retrievable
  over HTTP after completion.
- **Logs**: stdout/stderr captured to files, retrievable (full, tail, offset).
- **Idempotent submission**: `idempotency_key` makes agent retries safe;
  required on batch submits.
- **Batch operations**: atomic batch submit; batch wait by label; cancel by id
  or by label.
- **Status UI**: local web dashboard showing running/queued/completed tasks,
  slot/memory utilization, per-project usage, logs, and artifacts.

### 2.2 Non-functional

- Single user, multiple concurrent agents — auth exists for *attribution and
  quotas*, not adversarial isolation.
- Durability across service restarts (task records in SQLite; running tasks are
  reconciled on startup).
- All tasks execute on this machine; remote machines only submit.
- Target hardware: 64 cores / 78 GB RAM (slots default to `nproc - 8`, memory
  budget to total RAM − 8 GB).

### 2.3 Non-goals (v1)

- Distributed/remote execution (workers on other machines).
- Task dependency graphs — staged ordering (gates → tests → corpus) is client-side.
- Preemption / priority inversion.
- Resource *enforcement* (`cores`/`mem_mb` are scheduling hints, not cgroup
  limits — see §5.3).
- Multi-user security model, per-task sandboxing.
- Webhook notifications (agents poll / long-poll).

## 3. Architecture

```
        remote agents                    local agents / CLI / UI
              │                                │
              └──────────┬─────────────────────┘
                         ▼  HTTP (bearer token)
              ┌───────────────────────┐
              │   FastAPI service      │
              │  ┌─────────────────┐   │
              │  │ Dispatcher loop  │   │   admits tasks when
              │  │ (asyncio task)   │──▶│   slots + mem + quota allow
              │  └─────────────────┘   │
              │  ┌─────────────────┐   │
              │  │ Runner           │──▶│   subprocess + pgid,
              │  │                  │   │   logs, timeout, artifacts
              │  └─────────────────┘   │
              │  ┌─────────────────┐   │
              │  │ Janitor          │──▶│   retention, worktree prune,
              │  │                  │   │   free-space watermark
              │  └─────────────────┘   │
              │  SQLite (WAL)          │
              └───────────────────────┘
                         │
              data_dir/
                tasks/{id}/   worktree, logs, exit_code, artifacts/
                repos/        cached clones for repo@ref tasks
```

Single process (uvicorn). The dispatcher is an in-process asyncio loop over the
SQLite queue — no external broker. Chosen over Celery/Windmill/Prefect because the
requirements that matter (core-weighted slots, LPT ordering, artifact retrieval)
are absent or awkward in each, while what they provide (UI, retries, auth) is
cheap to replicate for a single-user system.

## 4. Task model

```
POST /v1/tasks          (single object, or {"tasks": [...]} for batch)
{
  "project":          "slabpull",                    // required; must match token
  "command":          ".venv/bin/python scripts/digest.py --shard 3/69 --check",
  "repo":  {"url": "git@github.com:org/r.git", "ref": "abc123"},  // REQUIRED —
                                                        // service clones+fetches
                                                        // and runs in a private
                                                        // git worktree at <ref>
  "workdir_subdir":   "subdir",                      // optional, within the worktree
  "setup_command":    "uv sync --frozen",            // optional, runs once before command
  "env":              {"SEED_DATA": "/data/seeds"},
  "cores":            1,                              // slots consumed; default 1
  "mem_mb":           2048,                           // RAM accounted; default per config
  "est_seconds":      1081,                           // scheduling order + timeout base
  "timeout_seconds":  3243,                           // see §4.2 for derivation
  "labels":           ["corpus", "key:tw05_nazca"],
  "idempotency_key":  "slabpull-corpus-tw05-abc123", // REQUIRED on batch submits
  "artifact_patterns":["out/**", "*.png"],
  "verdict_pattern":  "MATCH|DIFF_RESOLVED|SKIP|NEW_DIFF|D1_MISMATCH"  // optional
}
→ 201 {"task_id": "…", "status": "queued"}
  200 {"task_id": "…", ...}  // idempotency_key replay with identical payload
  409 {"error": {"code": "conflict", ...}}  // same key, different payload
```

### 4.1 Field semantics

- `repo:{url, ref}` is the **only** task mode — `workdir` and `repo.path`
  submissions are rejected. The service maintains a cached clone per URL in
  `repos_dir` and fetches before each task; each task gets
  `git worktree add` under `tasks/{id}/worktree`. `ref` resolves at admit
  time — a branch name runs whatever it points to at dispatch; submit a SHA
  for an exact pin. Per-source-repo mutexes serialize
  fetch/worktree-add/remove/prune so concurrent tasks can't collide on git
  ref locks.
- `cores` counts against the slot pool; `cores > slots` is rejected
  (`oversized`). `mem_mb` counts against the memory budget likewise; a task
  exceeding the whole budget is rejected. `mem_mb` defaults to config
  `default_mem_mb` (2048) with per-project overrides.
- `est_seconds` is the ordering key (DESC → longest first). Optional: absent
  estimates use `default_est_seconds` (config, 300). Convention for agents
  (documented in SKILL.md): estimate the *whole in-process task* — a determinism
  key that runs the sim twice in one process is one task with the summed
  estimate. v1.2: derive defaults from historical median duration per
  `(project, labels)`.
- `idempotency_key`: unique per project. Replay with an identical payload hash
  → `200` returning the existing task; replay with a *different* payload →
  `409 conflict` (a silent match would hide agent bugs). Required on batch
  submits — that is where retry storms live.
- `labels` are queryable, used for batch wait/cancel. Convention: `batch:<id>`
  groups a wave; `key:<name>` tags corpus shards.
- `verdict_pattern`: the runner scans the **last 256 KB** of stdout and stores
  the **last** regex match as `task.verdict` (a key that runs twice in-process
  prints two verdicts; last wins so `D1_MISMATCH` can't hide behind an earlier
  `MATCH`). Per-project default in config; per-task override allowed.
- `env` values are persisted and displayed in the UI — secrets belong in
  `secret_env` (merged into the child's environment; stored only as a 0600
  file in the task dir, never in the DB, API, or UI).

### 4.2 Timeout derivation

```
timeout_seconds = explicit value
    ? min(explicit, max_timeout_seconds)          // explicit honored
    : clamp(3 × est_seconds,
            min = timeout_floor_seconds (config, 600),
            max = max_timeout_seconds   (config, 129600 / 36h))
```

- Floor: a 10 s gate must not get a 30 s timeout — flaky `uv` resolves and cold
  caches would kill pre-flight stages.
- Ceiling: an agent typo must not park a slot for days.
- Applies to `command` only. `setup_command` has its own
  `setup_timeout_seconds` (config, 600).

### 4.3 Batch submit

- `{"tasks": [...]}` is **atomic**: all validated and inserted in one
  transaction, or none. Response: `201 {"tasks": [{"task_id": …}, …]}` in
  request order. Any validation error → `400` with the offending index(es);
  nothing is committed. The agent fixes and resubmits — idempotency keys make
  the retry safe even if it raced a partial acceptance elsewhere.
- `max_batch_size` config, default 500.

### 4.4 States

`queued → running → succeeded | failed | timeout | cancelled | lost`

- `timeout`: killed for exceeding `timeout_seconds`.
- `lost`: scheduler restarted and the process could not be recovered (see §6.4).
- Setup failures are `failed` with `phase = "setup"` (schema column).

## 5. Scheduling policy

### 5.1 Dispatcher

Runs on submit, on task completion, and on a 5 s tick:

1. Take `queued` tasks ordered by `est_seconds` DESC, `submitted_at` ASC
   (absent est was already defaulted at submit — no NULLs reach ordering).
2. Scan in order; admit each task iff `cores <= free_slots` **and**
   `mem_mb <= free_mem_mb` **and** within its project cap (§5.2).
3. **Backfill**: a task that doesn't fit does not block later tasks that do.
   With estimate-ordered submission the big jobs are already at the head;
   strict head-of-line blocking would waste slots. Residual risk: a large-core
   task could starve under a *continuous* stream of small tasks — bounded in
   practice by finite batches; if observed, add a reservation counter (head
   task unfilled for K ticks → reserve its `cores`, admit smalls only into the
   surplus). Deferred to v1.2.

- `slots`: config, default `nproc - 8` (headroom for service, OS, interactive
  use).
- `mem_slots`: config, default total RAM − 8 GB.
- `free_slots`/`free_mem_mb` are *derived* each tick from `running` rows —
  never a maintained counter (no leak path).

### 5.2 Fairness — dynamic weighted fair share

No static per-project caps. Fairness is computed per dispatch pass:

- The **competing set** is projects with at least one `queued` task.
- Each competing project's share of the pool, measured in slots (sum of `cores`
  of its `running` tasks):

```
share_i = slots × weight_i / Σ(weight over competing projects)
```

  floored, minimum 1. `weight` defaults to 1 per project (equal split); optional
  `[projects.X] weight` in config for deliberate asymmetry.
- **Pass 1**: scan the LPT-ordered queue; admit a task iff its project is under
  its share *and* it fits `free_slots`/`free_mem_mb`.
- **Pass 2 (work-conserving backfill)**: if slots remain, scan again and admit
  any task that fits regardless of share. Over-share projects only ever take
  slots that under-share projects' queued tasks can't currently use — a lone
  project therefore always fills the machine, and surplus is borrowed rather
  than idled.

Memory is a global budget only (not fair-shared): a task must fit `mem_slots`
to be admitted in either pass.

Example: slabpull (weight 1) and worldgen (weight 1) both have queued work →
28 slots each on a 56-slot pool; slabpull alone → 56. If worldgen's queue
drains, slabpull's share floats back to 56 automatically.

### 5.3 `cores`/`mem_mb` — enforced via cgroup v2 scopes

**Implemented (ahead of the original v1.2 schedule).** When
`systemd-run --user --scope` is available (probed at startup), every task runs
in its own scope `sched-<id>-<phase>` with `CPUQuota = cores × 100%`,
`MemoryMax = mem_mb`, `MemorySwapMax = 0`, `TimeoutStopUSec = 15s`, and
`KillMode = control-group`. This gives real CPU/memory limits *and* teardown
of all descendants including setsid'd daemons. Termination prefers
`systemctl --user stop <scope>`; the pgid path remains as fallback. If the
probe fails, the service degrades to hint-mode and reports
`degraded: no_cgroup_enforcement` in `/v1/queue` and `/v1/health`.

### 5.4 No preemption

A running task keeps its resources to completion/timeout.

## 6. Runner

### 6.1 Execution

- Per-task directory `data_dir/tasks/{id}/` created at admit time.
- Workdir resolution: `git worktree add tasks/{id}/worktree <ref>` from the
  cached clone (plus `workdir_subdir` if given); then `setup_command` if
  present.
- Spawn via a small shell stub:

```sh
setsid sh -c '
  echo $$ > pgid
  cd "$WORKDIR" && sh -c "$COMMAND" >> stdout.log 2>> stderr.log
  echo $? > exit_code        # no exec — this line must run to record completion
'
```

  `setsid` → own process group; `pgid`/`exit_code` files make the task
  recoverable after a service restart (§6.4). (Under cgroup enforcement the
  stub runs inside a `systemd-run --user --scope` instead of `setsid` — the
  scope is the isolation boundary.)

- Task env = service env + `env` + `secret_env` (0600 file), with
  `task_path_extra` dirs (default `~/.local/bin`, `~/bin`, `~/.cargo/bin`)
  prepended to PATH so user-level tools resolve by name.

### 6.1a Datasets

Named, versioned read-only data bundles for inputs too big for git
(generated worlds, corpora). `POST /v1/datasets/{name}?version=` uploads a
tar bundle to `data_dir/datasets/{name}/{version}/` with an auto-bumped
`latest` symlink. Tasks declare `datasets: [{name, version?, env?}]`; at
admit the scheduler resolves each spec (fail-fast if missing), records
`resolved_version`/`resolved_dir`, and injects `env` → resolved dir into the
task's environment (`SCHED_DATASETS_DIR` is always set to the root). The
bundle layout and any `manifest.json` (version + sha256s) is the project's
contract — the scheduler just hosts and pins the bytes. Delete is refused
while a running task references the version.

- stdout/stderr → `stdout.log`/`stderr.log`. On reaching `log_max_bytes`
  (config, 64 MB): append a truncation marker line and **stop writing** —
  offsets held by `/log` clients stay valid; `/log` past the marker returns
  the marker then EOF.

### 6.2 Termination (timeout, cancel, orphan cleanup)

- Persist intent first: write the `task_events` transition
  (`running → timeout`/`cancelled`) **before** signaling, so a crash between
  signal and DB write can't launder the cause on restart.
- `SIGTERM` pgid → 10 s grace → `SIGKILL` pgid.
- On normal completion (exit_code file appears): check the pgid is empty —
  a command that exits but leaked children into its process group gets a
  `SIGKILL` sweep *before* slots are released. (Setsid'd grandchildren escape
  this — fixed properly by the cgroup path in §5.3.)

### 6.3 Completion

On exit: record `exit_code`, `ended_at`; scan stdout tail for `verdict_pattern`
(last match, 256 KB window); copy `artifact_patterns` matches into
`tasks/{id}/artifacts/` (patterns are rejected at submit if they resolve
outside the workdir; per-artifact and total size caps apply — oversized files
are skipped with a warning recorded on the task); update state.

### 6.4 Crash recovery (service restart)

For each task left in `running`, in this order:

1. **`exit_code` file exists?** → task completed while we were down; mark
   `succeeded`/`failed` from the real code. (A task finishing during a restart
   must never be reported `lost` — that would trigger pointless re-runs of
   multi-hour jobs.)
2. **Past deadline?** `started_at + timeout_seconds` already elapsed → kill
   pgid, mark `timeout`. Downtime doesn't launder timeouts.
3. **pgid alive *and* identity verified?** → resume waiting on the exit-code
   file. Identity check: record leader pid + `/proc/{pid}` start-time at spawn;
   on restart require `/proc/{leader}` start-time to match before trusting
   `killpg` — protects against pgid reuse after a reboot (we must never signal
   a stranger's process group).
4. **Otherwise** → `lost` (with the persisted intent from §6.2, pre-signaled
   kills already carry their true terminal state).

### 6.5 Worktree lifecycle

Terminal state → `git worktree remove tasks/{id}/worktree` (falls back to
rm + `git worktree prune` by the janitor — plain `rm -rf` leaves stale admin
entries in the cached clone's `.git/worktrees/`). Note: per-task worktrees ×
per-task `uv sync` multiplies `.venv` disk usage for large batches; uv's
hardlinked cache keeps wheels cheap, but a shared per-repo-per-ref venv
(`UV_PROJECT_ENVIRONMENT`) is the documented option if it becomes a problem.

## 7. HTTP API

All under `/v1`, `Authorization: Bearer <project-token>` (admin token for
cross-project ops + UI). Timestamps are ISO-8601 UTC with milliseconds
(`2026-09-23T10:15:30.123Z`).

| Endpoint | Purpose |
|---|---|
| `POST /v1/tasks` | submit single or `{"tasks":[…]}` batch (atomic, §4.3) |
| `GET /v1/tasks` | list; filters `status`, `project`, `label`, `limit`, `cursor` (opaque token over `(submitted_at, id)` — batch submits share timestamps) |
| `GET /v1/tasks/{id}` | full record: state, phase, timing, exit_code, verdict, resources, `queue_position` + `free_slots` when queued |
| `POST /v1/tasks/{id}/cancel` | queued → cancelled; running → TERM→KILL |
| `POST /v1/tasks/cancel` | batch cancel by `{"label"?, "ids"?}`; project token scopes to its own tasks. (Literal route registered before `/{id}`; task ids are `t_`-prefixed ULIDs, reserved words impossible.) |
| `GET /v1/tasks/{id}/wait?timeout=` | long-poll until terminal; returns immediately if already terminal; server caps `timeout` at 300 s |
| `GET /v1/tasks/wait?label=&timeout=` | **batch wait** — returns when all matching tasks are terminal, else `{"terminal": n, "remaining": m}` at timeout. The primary sync point for staged pipelines ("submit 69 → wait → check verdicts → next wave") |
| `GET /v1/tasks/{id}/log?stream=stdout|stderr&offset=&tail=` | log fetch; `offset` for incremental follow (stable across truncation, §6.1) |
| `GET /v1/tasks/{id}/files?prefix=` | list artifacts + workdir files; `prefix` required for workdir listings, capped at 1000 entries |
| `GET /v1/tasks/{id}/files/{path}` | download; path must `resolve()` under task dir **or** workdir — symlink-safe prefix check against both roots, not a `..` string check |
| `POST /v1/datasets/{name}?version=` | upload a tar/`.tar.gz` bundle → `datasets/{name}/{version}/`, bumps `latest`; `?replace=1` overwrites. Body streamed to disk, extracted with `filter="data"` (no escapes/specials) |
| `GET /v1/datasets` | list names, versions, latest, sizes |
| `GET /v1/datasets/{name}/{version}/manifest` | the bundle's `manifest.json` (project-side verify/publish contract) |
| `DELETE /v1/datasets/{name}/{version}` | GC a version; 409 while a running task references it |
| `GET /v1/queue` | slots used/free, mem used/free, per-project running/queued, uptime, `degraded` flags (e.g. low disk) |
| `GET /v1/health` | liveness + degradation |

Errors: JSON `{"error": {"code": "…", "message": "…"}}`; codes `unauthorized`,
`not_found`, `validation`, `oversized`, `conflict` (idempotency-key payload
mismatch; cancel of a terminal task).

## 8. Local UI

Served by the same FastAPI app — no separate service. Goal: at-a-glance status
for the owner, and easy inspection of what agents have submitted.

**Stack**: server-rendered shell + vanilla JS polling the `/v1` API every ~2 s
(SSE upgrade path later). Single dependency-free page; htmx acceptable if it
stays a vendored single file. Admin token entered once, kept in `localStorage`.

**Views**:

- **`/` dashboard**
  - Header: slot gauge (used/total), memory gauge, uptime, per-project
    running/queued counts, degradation banner (e.g. disk watermark).
  - *Running*: project, command (truncated), cores/mem, elapsed vs
    `est_seconds` progress bar (past-estimate tasks highlighted), verdict chip
    when available, cancel button.
  - *Queued*: in dispatch order — literally "what runs next," so LPT ordering
    is visible; shows est_seconds, cores, mem.
  - *Completed* (recent 50): status badge (`succeeded`/`failed`/`timeout`/
    `cancelled`/`lost`), phase, duration, exit code, verdict chip, detail link.
  - Filters: project dropdown, status tabs, label search.
- **`/ui/tasks/{id}` detail**
  - Metadata (full command, env, repo@ref, timings incl. `task_events`
    timeline, idempotency key).
  - Live log tail (auto-scroll, stdout/stderr toggle, incremental via `offset`).
  - Artifact list with download links + inline preview for images (PNG/GIF) —
    this matters for the render-probe verification loop.
  - Cancel button while non-terminal.

**Read-mostly**: UI can only cancel, never edit or reorder. Auth: admin token
required whenever the service is bound beyond loopback.

## 9. Auth & multi-agent policy

- Config holds per-project tokens: `[projects.slabpull] token="…"`,
  `weight=1`, `default_mem_mb=…` (see §5.2 for `weight`).
- Request `project` must equal the token's project → attribution enforced,
  quotas enforceable, label-scoped cancel can't cross projects.
- One `admin` token: owner + UI; may act on any project.
- Bind default: **`127.0.0.1`** — remote access requires explicit
  `bind = "0.0.0.0"` (or a specific LAN/Tailscale interface). Tokens are
  required on any non-loopback bind.
- Transport: plaintext HTTP acceptable on LAN/Tailscale for v1 (tokens are for
  attribution, not secrecy); TLS via Caddy if ever exposed further. SKILL.md
  must never instruct agents to reuse real credentials as project tokens.

## 10. Storage

SQLite, WAL mode. Single writer (dispatcher); API reads are snapshots.

```sql
tasks(id TEXT PK, project, status, phase, command, workdir, repo_ref, env_json,
      cores, mem_mb, est_seconds, timeout_seconds, labels_json,
      idempotency_key, payload_hash, artifact_patterns_json, verdict_pattern,
      verdict, exit_code, pgid, leader_pid, leader_start,
      submitted_at, started_at, ended_at, error,
      UNIQUE(project, idempotency_key))
task_events(task_id, ts, from_status, to_status, detail)
  -- index on task_id (detail-view timeline); written BEFORE side effects
```

Indices on `tasks(status)`, `tasks(project, status)`, `tasks(submitted_at, id)`.

**Janitor** (periodic):
- Deletes terminal tasks + workdirs/artifacts past `retention_days` (default 30).
- `git worktree prune` on cached repos (cleanup after interrupted removals).
- Free-space watermark: `statvfs(data_dir)` each pass; below `min_free_gb`
  (default 10) → admissions pause, `/v1/queue` and `/v1/health` report
  `degraded`. First symptom of a full disk must be a clear flag, not a
  cascade of SQLite `EIO` failures misreported as task errors.

## 11. Client ergonomics

- `SKILL.md` in this repo — the agent interface contract: API reference +
  conventions (shard-longest-first submission; `est_seconds` = whole
  in-process task, from baseline JSONL; declare `cores`/`mem_mb` honestly —
  enforced via cgroup scopes; label scheme `batch:<id>`/`key:<name>`;
  staged-gate pattern: submit batch → `wait?label=` → check verdicts → next
  wave; use `wait` endpoints or poll ≥5 s, never tight-loop `GET /tasks/{id}`).

## 12. Failure modes & edge cases

| Case | Handling |
|---|---|
| Task completes during service restart | exit-code file checked *first* → real terminal state (§6.4) |
| Service stop/restart | systemd `KillMode=process` — children survive; §6.4 reconciles |
| pgid reused by another process after reboot | leader pid + `/proc` start-time identity check before any signal |
| Timeout elapses during downtime | killed immediately on re-attach |
| Command exits, children linger in pgid | SIGKILL sweep before slots release |
| setsid'd grandchildren | escape pgid sweep — fixed by cgroup scopes (v1.2) |
| Memory pressure | `mem_mb` admission accounting vs `mem_slots` budget |
| Agent retry storms | `idempotency_key` required on batches; payload-mismatch → 409 |
| Partial-invalid batch | atomic reject with per-index errors |
| Task never exits | `timeout_seconds` → TERM→KILL (floored + clamped, §4.2) |
| Workdir deleted underneath | validated at admit; failure → `failed` |
| Log blowup | `log_max_bytes` → marker + stop writing; offsets stay valid |
| Artifact glob escapes workdir | rejected at submit (`resolve()` prefix check) |
| Artifact glob matches GBs | per-file + total size caps; skipped with warning |
| Disk fills (logs/artifacts/worktrees/DB) | janitor watermark pauses admissions, `degraded` flag |
| DB contention | single writer + WAL |

## 13. Deployment

- uv-managed package in this repo; `scheduler.toml` config; systemd **user**
  unit (`~/.config/systemd/user/task-scheduler.service`) with
  `KillMode=process` so service restarts don't kill running tasks.
- `data_dir` default `~/.local/share/task-scheduler`.

## 14. Milestones

- **v1**: API + dispatcher (slots + mem + fair share) + runner + logs +
  cancel + wait (single & batch) + SQLite persistence + crash recovery +
  artifacts + verdict scan + CLI + SKILL.md. (Artifacts/verdict are v1 because
  the workload brief makes them first-class, not extras.)
- **v1.1** (implemented): local UI dashboard + detail view; `repo@ref`
  worktrees — tasks run in a private `git worktree` at `repo.ref` (SHA pins an
  exact commit; branch names resolve at dispatch).
- **v1.2** (partially landed early): cgroup v2 enforcement ✅, learned
  historical-median `est_seconds` ✅, `secret_env` ✅, artifact-retention
  split ✅. Remaining: SSE log streaming, slot reservation for large-core
  tasks, shared-venv option, webhooks, remote workers.

## 15. Resolved & remaining open questions

Resolved by review:
- Batch semantics: atomic + per-item response; `idempotency_key` required on batches.
- Timeout: floor 600 s, ceiling 36 h, covers command only; setup has own timeout.
- Recovery order: exit-file → deadline → verified pgid → `lost`.
- Verdict: last-match wins in trailing 256 KB.
- `priority_hint` cut — `est_seconds` ordering + client-side staging suffices;
  it would only invite priority spam.
- Timestamps: ISO-8601 UTC ms.

Resolved by owner (2026-09-23):
- Remote access is over **LAN IP**: deploy with `bind` set to the LAN
  interface; plaintext HTTP + bearer tokens accepted for v1 (attribution, not
  secrecy). TLS via Caddy only if exposure ever grows.
- **No static caps** — replaced by dynamic weighted fair share (§5.2); a lone
  project always gets the full machine.
- **Artifacts share the record retention** (`retention_days`, default 30);
  split policies later only if disk pressure demands it.

Remaining:
1. Per-project `weight` and `default_mem_mb` values once projects are
   enumerated (defaults are sane without them).
