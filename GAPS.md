# Known gaps & limitations

Audit of sharp edges in the current implementation, with status.
Last updated: 2026-09-23 (post v1.1).

Legend: ✅ fixed · 🔧 config knob · 📌 accepted / by design · 🗓 planned

## Resource accounting

| Gap | Status | Notes |
|---|---|---|
| `cores`/`mem_mb` unenforced — a `cores:1` task can eat 64 cores | ✅ | cgroup v2 scopes via `systemd-run --user --scope` (CPUQuota + MemoryMax). Falls back to pgid-hint mode if scopes unavailable; check `degraded`/`GET /v1/queue`. |
| Under-declared `mem_mb` → OOM risk | ✅ | MemoryMax makes the declaration real; kernel OOMs the task's cgroup, not the host. |
| setsid'd/daemonized grandchildren escape the kill sweep | ✅ | cgroup kill covers all descendants. |

## Lifecycle

| Gap | Status | Notes |
|---|---|---|
| No auto-retry of `failed`/`lost`/`timeout` tasks | 📌 | Agents own retries; `idempotency_key` makes resubmission safe. |
| Machine reboot kills running tasks → `lost` on next start | 📌 | Tasks don't auto-requeue. |
| Explicit `timeout_seconds` clamped to floor (600 s) | ✅ | Floor now applies only to derived timeouts; explicit values clamp to `max_timeout_seconds` only. |
| 30-day retention deletes artifacts with records | 🔧 | `artifact_retention_days` can now split policies. |
| `wait`/`wait?label=` capped at 300 s per call | 📌 | Agents loop; documented in SKILL.md. |
| `lost` tasks need client-side resubmission | 📌 | By design — the scheduler doesn't guess intent. |

## Repo mode

| Gap | Status | Notes |
|---|---|---|
| `url` mode fetch/clone blocks the dispatcher | ✅ | Dispatch pass runs in a worker thread; admissions no longer stall. |
| Private repo URLs need service-side git credentials | 📌 | `repo.url` is the only mode — use SSH form (`git@…`); the service user's ssh config/keys apply. HTTPS to private repos fails (no credential helper). `workdir`/`repo.path` were removed to eliminate mutable-checkout and cross-user `safe.directory` footguns. |
| Worktree + per-task `uv sync` multiplies disk | 📌 | uv hardlinks mitigate; shared `UV_PROJECT_ENVIRONMENT` is the documented option if it bites. |

## Security / data

| Gap | Status | Notes |
|---|---|---|
| `env` values persisted + shown in UI | ✅ | `secret_env` field: merged into the child's environment, stored only as `0600` file in the task dir (never in DB, API, or UI). |
| Trivial tokens on `0.0.0.0` | 📌 | Attribution only, by explicit owner choice. Don't expose beyond the LAN. |
| Tasks run as the `dennis` user, no sandbox | 📌 | Single-user trust model. |
| Artifact `files` download is per-file HTTP | 📌 | Large artifacts could use range/streaming later. |

## Scheduling semantics

| Gap | Status | Notes |
|---|---|---|
| Unestimated tasks sort near end (default 300 s) | ✅ | Service now learns a per-(project,label) median duration from history when `est_seconds` is absent. |
| `est_seconds` doubles as queue-jump lever | 📌 | Trusted agents; could inflate to jump ahead. |
| `queue_position` ignores fair-share caps | 📌 | It's a hint, not a schedule. |
| Big-core task starvation under continuous small-task stream | 🗓 | Bounded by finite batches in practice; slot-reservation counter is the designed fix if observed. |

## UX / ops

| Gap | Status | Notes |
|---|---|---|
| Truncated task IDs unusable | ✅ | API accepts unambiguous id prefixes. |
| Log cap cuts the tail (no rotation) | 📌 | Chatty tasks should redirect output to a file + declare it an artifact. |
| Dashboard polls every 2.5 s | 📌 | SSE upgrade path deferred. |
| Batch errors embed JSON in `error.message` | 📌 | Machine-readable enough; could be structured later. |

## Deferred wholesale (v1.2+)

cgroup enforcement landed ahead of schedule ✅. Still deferred:
dependency graphs, preemption, webhooks/notifications, SSE log streaming,
remote workers, per-task CPU pinning (`AllowedCPUs`), multi-user hardening.
