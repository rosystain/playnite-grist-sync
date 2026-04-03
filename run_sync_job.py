#!/usr/bin/env python3
"""Unified sync runner for Windows Task Scheduler.

Features:
- Runs P2G then G2P in one job.
- Rotating log file.
- Single-instance lock file to prevent overlapping runs.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Optional


class LockError(Exception):
    pass


class FileLock:
    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self.fd: Optional[int] = None

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise LockError(f"Lock already exists: {self.lock_path}") from exc

        payload = (
            f"pid={os.getpid()}\n"
            f"startedAt={datetime.now(timezone.utc).isoformat()}\n"
        )
        os.write(self.fd, payload.encode("utf-8"))

    def release(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        try:
            self.lock_path.unlink(missing_ok=True)
        except OSError:
            pass


def setup_logger(log_file: Path, max_mb: int, backups: int) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("sync-job")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    file_handler = RotatingFileHandler(
        filename=str(log_file),
        maxBytes=max_mb * 1024 * 1024,
        backupCount=backups,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def run_step(
    name: str,
    python_exe: Path,
    script_path: Path,
    logger: logging.Logger,
    script_args: Optional[List[str]] = None,
) -> int:
    cmd = [str(python_exe), str(script_path)]
    if script_args:
        cmd.extend(script_args)
    logger.info("Step start: %s", name)
    logger.info("Command: %s", " ".join(cmd))

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(script_path.parent),
    )

    assert process.stdout is not None
    for line in process.stdout:
        logger.info("[%s] %s", name, line.rstrip())

    code = process.wait()
    if code == 0:
        logger.info("Step success: %s", name)
    else:
        logger.error("Step failed: %s (exit=%s)", name, code)
    return code


def resolve_path(base_dir: Path, value: str) -> Path:
    p = Path(value)
    return p if p.is_absolute() else (base_dir / p)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run P2G + G2P with lock and rotating logs")
    parser.add_argument("--python", default=sys.executable, help="Python executable path")
    parser.add_argument("--p2g", default="sync_playnite_to_grist.py", help="P2G script path")
    parser.add_argument("--g2p", default="sync_grist_to_playnite.py", help="G2P script path")
    parser.add_argument("--skip-p2g", action="store_true", help="Skip Playnite->Grist step")
    parser.add_argument("--skip-g2p", action="store_true", help="Skip Grist->Playnite step")
    g2p_mode = parser.add_mutually_exclusive_group()
    g2p_mode.add_argument("--g2p-apply", action="store_true", help="Force G2P apply mode")
    g2p_mode.add_argument("--g2p-dry-run", action="store_true", help="Force G2P dry-run mode")
    parser.add_argument("--continue-on-error", action="store_true", help="Run next step even if previous fails")
    parser.add_argument("--log-file", default="logs/sync-job.log", help="Log file path")
    parser.add_argument("--log-max-mb", type=int, default=10, help="Rotate when log exceeds this size (MB)")
    parser.add_argument("--log-backups", type=int, default=14, help="Number of rotated files to keep")
    parser.add_argument("--lock-file", default=".sync-job.lock", help="Lock file path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent

    python_exe = resolve_path(base_dir, args.python)
    p2g_path = resolve_path(base_dir, args.p2g)
    g2p_path = resolve_path(base_dir, args.g2p)
    log_file = resolve_path(base_dir, args.log_file)
    lock_file = resolve_path(base_dir, args.lock_file)

    logger = setup_logger(log_file, args.log_max_mb, args.log_backups)
    lock = FileLock(lock_file)

    try:
        lock.acquire()
    except LockError as exc:
        logger.error("Another sync job is already running: %s", exc)
        return 2

    logger.info("Sync job started")
    logger.info("Log file: %s", log_file)
    logger.info("Lock file: %s", lock_file)

    try:
        if not python_exe.exists():
            logger.error("Python executable not found: %s", python_exe)
            return 3

        if not args.skip_p2g:
            if not p2g_path.exists():
                logger.error("P2G script not found: %s", p2g_path)
                return 3
            p2g_code = run_step("P2G", python_exe, p2g_path, logger)
            if p2g_code != 0 and not args.continue_on_error:
                return p2g_code

        if not args.skip_g2p:
            if not g2p_path.exists():
                logger.error("G2P script not found: %s", g2p_path)
                return 3
            g2p_args: List[str] = []
            if args.g2p_apply:
                g2p_args.append("--apply")
            elif args.g2p_dry_run:
                g2p_args.append("--dry-run")

            g2p_code = run_step("G2P", python_exe, g2p_path, logger, g2p_args)
            if g2p_code != 0 and not args.continue_on_error:
                return g2p_code

        logger.info("Sync job completed")
        return 0
    except KeyboardInterrupt:
        logger.error("Sync job interrupted")
        return 130
    finally:
        lock.release()


if __name__ == "__main__":
    sys.exit(main())
