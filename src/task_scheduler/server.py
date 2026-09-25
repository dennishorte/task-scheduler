"""Entry point: task-scheduler --config /path/to/scheduler.toml"""
from __future__ import annotations

import argparse

import uvicorn

from .api import create_app
from .config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(prog="task-scheduler")
    parser.add_argument("--config", required=True, help="path to scheduler.toml")
    args = parser.parse_args()
    cfg = load_config(args.config)
    app = create_app(args.config)
    uvicorn.run(app, host=cfg.bind, port=cfg.port, log_level="info")


if __name__ == "__main__":
    main()
