#!/usr/bin/env python3
"""Unified sync runner for Windows Task Scheduler.

Features:
- Runs P2G then G2P in one job.
- Rotating log file.
- Single-instance lock file to prevent overlapping runs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class LockError(Exception):
    pass


class NotificationError(Exception):
    pass


ERROR_NOTIFY_COOLDOWN_SECONDS = 1800


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


def is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def runtime_base_dir() -> Path:
    if is_frozen():
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def default_step_path(step: str) -> str:
    if is_frozen():
        return f"{step}.exe"
    return f"{step}.py"


def parse_bool_like(value: str) -> Optional[bool]:
    text = value.strip().lower()
    if text in {"true", "yes", "1"}:
        return True
    if text in {"false", "no", "0"}:
        return False
    return None


def parse_scalar_text(raw: str) -> str:
    text = raw.strip()
    if "#" in text:
        text = text.split("#", 1)[0].rstrip()
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        return text[1:-1]
    return text


def read_simple_config(config_path: Path) -> dict:
    out = {}
    if not config_path.exists():
        return out
    try:
        lines = config_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return out

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or ":" not in line:
            continue
        key, raw = line.split(":", 1)
        out[key.strip()] = parse_scalar_text(raw)
    return out


def bool_from_config(config: dict, key: str, default: bool) -> bool:
    raw = str(config.get(key, "")).strip()
    if not raw:
        return default
    parsed = parse_bool_like(raw)
    return default if parsed is None else parsed


def read_g2p_enabled(config_path: Path) -> bool:
    cfg = read_simple_config(config_path)
    return bool_from_config(cfg, "g2p_enabled", False)


def send_playnite_notification(
    base_url: str,
    token: str,
    text: str,
    level: str,
    timeout_seconds: int = 8,
) -> None:
    if not base_url or not token:
        raise NotificationError("base_url/token missing")

    notify_type = "error" if level.lower() == "error" else "info"
    payload = {"text": text, "type": notify_type}
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = Request(
        url=f"{base_url.rstrip('/')}/api/notifications",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    try:
        with urlopen(req, timeout=timeout_seconds):
            return
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise NotificationError(f"HTTP {exc.code}: {details}") from exc
    except URLError as exc:
        raise NotificationError(str(exc)) from exc


def parse_iso_utc(value: str) -> Optional[datetime]:
    text = (value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def read_notify_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return raw if isinstance(raw, dict) else {}


def write_notify_state(path: Path, payload: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        # Notification state persistence failure should not break sync.
        return


def should_send_error_notification(state_path: Path, text: str, now: datetime) -> bool:
    state = read_notify_state(state_path)
    last_text_hash = str(state.get("last_error_hash", "")).strip()
    last_sent_at = parse_iso_utc(str(state.get("last_sent_at", "")))
    current_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()

    if (
        last_text_hash
        and last_text_hash == current_hash
        and last_sent_at is not None
        and (now - last_sent_at).total_seconds() < ERROR_NOTIFY_COOLDOWN_SECONDS
    ):
        return False

    write_notify_state(
        state_path,
        {
            "last_error_hash": current_hash,
            "last_sent_at": now.isoformat(),
            "cooldownSeconds": ERROR_NOTIFY_COOLDOWN_SECONDS,
        },
    )
    return True


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

    logger.addHandler(file_handler)
    return logger


def windows_popen_kwargs() -> dict:
    # Keep task runs fully silent on Windows: no child console window.
    if os.name != "nt":
        return {}
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    return {
        "startupinfo": startupinfo,
        "creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0),
    }


def run_step(
    name: str,
    python_exe: Path,
    script_path: Path,
    logger: logging.Logger,
    script_args: Optional[List[str]] = None,
) -> int:
    if script_path.suffix.lower() == ".exe":
        cmd = [str(script_path)]
    else:
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
        **windows_popen_kwargs(),
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


def run_embedded_step(name: str, module_name: str, logger: logging.Logger, script_args: Optional[List[str]] = None) -> int:
    logger.info("Step start (embedded): %s", name)
    logger.info("Module: %s Args: %s", module_name, " ".join(script_args or []))
    try:
        if module_name == "sync_playnite_to_grist":
            import sync_playnite_to_grist as mod
        elif module_name == "sync_grist_to_playnite":
            import sync_grist_to_playnite as mod
        else:
            raise ValueError(f"Unknown embedded module: {module_name}")
        old_argv = list(sys.argv)
        sys.argv = [module_name + ".py", *(script_args or [])]
        try:
            code = int(mod.main())
        finally:
            sys.argv = old_argv
    except Exception as exc:
        logger.exception("Step crashed: %s", name)
        logger.error("Exception: %s", exc)
        return 1

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
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument("--p2g", default=default_step_path("sync_playnite_to_grist"), help="P2G script or exe path")
    parser.add_argument("--g2p", default=default_step_path("sync_grist_to_playnite"), help="G2P script or exe path")
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
    base_dir = runtime_base_dir()

    python_exe = resolve_path(base_dir, args.python)
    p2g_path = resolve_path(base_dir, args.p2g)
    g2p_path = resolve_path(base_dir, args.g2p)
    config_path = resolve_path(base_dir, args.config)
    log_file = resolve_path(base_dir, args.log_file)
    lock_file = resolve_path(base_dir, args.lock_file)
    notify_state_file = resolve_path(base_dir, "logs/notify-error-state.json")

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

    cfg = read_simple_config(config_path)
    base_url = str(cfg.get("base_url", "")).strip().rstrip("/")
    token = str(cfg.get("token", "")).strip()
    sent_error_notification = False

    def notify_if_needed(level: str, text: str) -> None:
        nonlocal sent_error_notification
        if level != "error":
            return
        if sent_error_notification:
            logger.info("Playnite notification skipped: already sent in this run")
            return
        now = datetime.now(timezone.utc)
        if not should_send_error_notification(notify_state_file, text, now):
            logger.info("Playnite notification skipped: same error in cooldown window")
            return
        try:
            send_playnite_notification(base_url, token, text, level)
            sent_error_notification = True
            logger.info("Playnite notification sent: %s", level)
        except NotificationError as exc:
            logger.warning("Playnite notification failed: %s", exc)

    try:
        if not python_exe.exists() and (
            Path(args.p2g).suffix.lower() != ".exe" or Path(args.g2p).suffix.lower() != ".exe"
        ):
            logger.error("Python executable not found: %s", python_exe)
            notify_if_needed("error", "Sync failed: Python executable not found")
            return 3

        if not args.skip_p2g:
            if p2g_path.exists():
                p2g_code = run_step("P2G", python_exe, p2g_path, logger)
            elif is_frozen():
                p2g_code = run_embedded_step("P2G", "sync_playnite_to_grist", logger)
            else:
                logger.error("P2G target not found: %s", p2g_path)
                notify_if_needed("error", "Sync failed: P2G target not found")
                return 3
            if p2g_code != 0 and not args.continue_on_error:
                notify_if_needed("error", f"Sync failed: P2G exit={p2g_code}")
                return p2g_code

        g2p_enabled = read_g2p_enabled(config_path)
        should_run_g2p = (not args.skip_g2p) and (args.g2p_apply or args.g2p_dry_run or g2p_enabled)
        if not should_run_g2p:
            logger.info("G2P step skipped (default behavior). Enable g2p_enabled in config or use --g2p-apply/--g2p-dry-run.")
        elif not args.skip_g2p:
            g2p_args: List[str] = []
            if args.g2p_apply:
                g2p_args.append("--apply")
            elif args.g2p_dry_run:
                g2p_args.append("--dry-run")

            if g2p_path.exists():
                g2p_code = run_step("G2P", python_exe, g2p_path, logger, g2p_args)
            elif is_frozen():
                g2p_code = run_embedded_step("G2P", "sync_grist_to_playnite", logger, g2p_args)
            else:
                logger.error("G2P target not found: %s", g2p_path)
                notify_if_needed("error", "Sync failed: G2P target not found")
                return 3
            if g2p_code != 0 and not args.continue_on_error:
                notify_if_needed("error", f"Sync failed: G2P exit={g2p_code}")
                return g2p_code

        logger.info("Sync job completed")
        return 0
    except KeyboardInterrupt:
        logger.error("Sync job interrupted")
        notify_if_needed("error", "Sync interrupted")
        return 130
    finally:
        lock.release()


if __name__ == "__main__":
    sys.exit(main())
