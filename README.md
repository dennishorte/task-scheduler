# task-scheduler

Single-machine HTTP task scheduler that arbitrates CPU time across
agent-driven projects. See [DESIGN.md](DESIGN.md) for the full design and
[SKILL.md](SKILL.md) for the agent-facing API contract.

## Quick start

```bash
uv sync
cp scheduler.example.toml ~/.config/task-scheduler/scheduler.toml  # edit tokens
uv run task-scheduler --config ~/.config/task-scheduler/scheduler.toml
```

Dashboard: http://127.0.0.1:8377/ (admin token)

## Clients

Plain HTTP is the only client interface — agents need nothing installed but
curl. See [SKILL.md](SKILL.md) for the endpoint reference and submit a task
like:

```bash
curl -X POST http://<host>:8377/v1/tasks \
  -H "Authorization: Bearer <project-token>" -H "Content-Type: application/json" \
  -d '{"project": "P", "command": "pytest tests/unit",
       "repo": {"url": "git@github.com:org/repo.git", "ref": "<sha>"},
       "cores": 7}'
```

Every task runs in a private `git worktree` cloned from `repo.url` at `ref`
(SSH form for private repos; `file://` works for local). A SHA pins the exact
commit; a branch name resolves when the task dispatches.

## Deploy

`deploy/task-scheduler.service` is a systemd **user** unit
(`KillMode=process` so tasks survive service restarts).

## Test

```bash
uv run pytest tests/
```
