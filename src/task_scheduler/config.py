"""Configuration loading for the task scheduler service."""
from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


def _total_mem_mb() -> int:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) // 1024
    except OSError:
        pass
    return 8192


@dataclass
class ProjectConfig:
    token: str
    weight: float = 1.0
    default_mem_mb: int | None = None
    verdict_pattern: str | None = None


@dataclass
class Config:
    bind: str = "127.0.0.1"
    port: int = 8377
    data_dir: Path = field(
        default_factory=lambda: Path("~/.local/share/task-scheduler").expanduser()
    )
    slots: int = max(1, (os.cpu_count() or 8) - 8)
    mem_slots_mb: int = max(1024, _total_mem_mb() - 8192)
    default_est_seconds: float = 300
    default_mem_mb: int = 2048
    timeout_floor_seconds: float = 600
    max_timeout_seconds: float = 129600  # 36h
    setup_timeout_seconds: float = 600
    log_max_bytes: int = 64 * 1024 * 1024
    retention_days: int = 30
    min_free_gb: float = 10
    max_batch_size: int = 500
    artifact_file_max_bytes: int = 100 * 1024 * 1024
    artifact_total_max_bytes: int = 500 * 1024 * 1024
    # Prepended to every task's PATH so user-level tools (uv, cargo, mise…)
    # resolve by name — the service env itself has a minimal PATH.
    task_path_extra: list[str] = field(
        default_factory=lambda: ["~/.local/bin", "~/bin", "~/.cargo/bin"]
    )
    admin_token: str | None = None
    ui_enabled: bool = True
    enforce_cgroups: bool = True
    artifact_retention_days: int | None = None
    projects: dict[str, ProjectConfig] = field(default_factory=dict)

    def project(self, name: str) -> ProjectConfig | None:
        return self.projects.get(name)


def load_config(path: str | Path) -> Config:
    cfg = Config()
    raw = tomllib.loads(Path(path).read_text())

    scalar_keys = {
        "bind": str,
        "port": int,
        "slots": int,
        "mem_slots_mb": int,
        "default_est_seconds": (int, float),
        "default_mem_mb": int,
        "timeout_floor_seconds": (int, float),
        "max_timeout_seconds": (int, float),
        "setup_timeout_seconds": (int, float),
        "log_max_bytes": int,
        "retention_days": int,
        "min_free_gb": (int, float),
        "max_batch_size": int,
        "artifact_file_max_bytes": int,
        "artifact_total_max_bytes": int,
        "admin_token": str,
        "ui_enabled": bool,
        "enforce_cgroups": bool,
        "artifact_retention_days": int,
    }
    for key, typ in scalar_keys.items():
        if key in raw:
            val = raw.pop(key)
            if not isinstance(val, typ):
                raise SystemExit(f"config: {key} must be {typ}")
            setattr(cfg, key, val)

    if "data_dir" in raw:
        cfg.data_dir = Path(raw.pop("data_dir")).expanduser()
    if "task_path_extra" in raw:
        cfg.task_path_extra = [str(p) for p in raw.pop("task_path_extra")]

    projects = raw.pop("projects", {})
    for name, pcfg in projects.items():
        token = pcfg.get("token")
        if not token:
            raise SystemExit(f"config: projects.{name} missing token")
        cfg.projects[name] = ProjectConfig(
            token=token,
            weight=float(pcfg.get("weight", 1.0)),
            default_mem_mb=pcfg.get("default_mem_mb"),
            verdict_pattern=pcfg.get("verdict_pattern"),
        )

    if raw:
        raise SystemExit(f"config: unknown keys {sorted(raw)}")
    if cfg.admin_token is None and not cfg.projects:
        raise SystemExit("config: set admin_token and/or at least one [projects.X]")
    return cfg
