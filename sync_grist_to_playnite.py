#!/usr/bin/env python3
"""Sync selected metadata from Grist back to Playnite.

Conflict strategy (Policy B):
1) If row fingerprint unchanged since last g2p run -> skip.
2) If changed, editedAt must be strictly newer than syncedAt.
3) If editedAt gate passes, compare editedAt vs Playnite modified.
4) If both timestamps exist, Grist wins only when editedAt is newer.
5) If Playnite modified is missing, apply is controlled by
    g2p_allow_when_playnite_modified_missing (default: true).
6) If editedAt is missing, equal, or older -> skip (Playnite priority).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


CONFIG_PATH = Path("config.yaml")

DEFAULT_G2P_FIELDS = [
    "description",
    "notes",
    "releaseDate",
    "favorite",
    "hidden",
    "userScore",
    "categories",
    "tags",
    "features",
    "genres",
    "developers",
    "publishers",
    "series",
]
LIST_FIELDS = {
    "categories",
    "tags",
    "features",
    "genres",
    "developers",
    "publishers",
    "series",
    "platforms",
    "ageRatings",
    "regions",
}
IGNORED_G2P_FIELDS = {
    "completionStatus",
    "hasIcon",
    "hasCover",
    "hasBackground",
    "version",
    "installDirectory",
    "installSize",
}


class ConfigError(Exception):
    pass


@dataclass
class Config:
    base_url: str
    token: str
    grist_base_url: str
    grist_doc_id: str
    grist_api_key: str
    grist_table_name: str
    grist_batch_size: int
    g2p_state_path: str
    g2p_fields: List[str]
    g2p_max_pages: int
    g2p_allow_when_playnite_modified_missing: bool
    g2p_incremental_cutoff: bool


def parse_bool(raw: str) -> bool:
    value = raw.strip().lower()
    if value in {"true", "yes", "1"}:
        return True
    if value in {"false", "no", "0"}:
        return False
    raise ConfigError(f"Invalid boolean value: {raw}")


def parse_scalar(raw: str) -> Any:
    raw = raw.strip()
    if not raw:
        return ""
    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]
    if raw.startswith("'") and raw.endswith("'"):
        return raw[1:-1]
    if raw.lower() in {"true", "false", "yes", "no"}:
        return parse_bool(raw)
    if raw.lstrip("-").isdigit():
        return int(raw)
    return raw


def load_simple_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

    data: Dict[str, Any] = {}
    for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if ":" not in line:
            raise ConfigError(f"Line {idx}: expected key: value")
        key, value = line.split(":", 1)
        key = key.strip()
        if not key:
            raise ConfigError(f"Line {idx}: empty key")
        data[key] = parse_scalar(value)
    return data


def parse_csv_fields(raw: Any) -> List[str]:
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    if raw is None:
        return list(DEFAULT_G2P_FIELDS)
    text = str(raw).strip()
    if not text:
        return list(DEFAULT_G2P_FIELDS)
    return [x.strip() for x in text.split(",") if x.strip()]


def load_config(path: Path) -> Config:
    raw = load_simple_yaml(path)
    required = [
        "base_url",
        "token",
        "grist_base_url",
        "grist_doc_id",
        "grist_api_key",
        "grist_table_name",
        "grist_batch_size",
    ]
    missing = [k for k in required if k not in raw]
    if missing:
        raise ConfigError(f"Missing config keys: {', '.join(missing)}")

    cfg = Config(
        base_url=str(raw["base_url"]).rstrip("/"),
        token=str(raw["token"]),
        grist_base_url=str(raw["grist_base_url"]).rstrip("/"),
        grist_doc_id=str(raw["grist_doc_id"]),
        grist_api_key=str(raw["grist_api_key"]),
        grist_table_name=str(raw["grist_table_name"]),
        grist_batch_size=int(raw["grist_batch_size"]),
        g2p_state_path=str(raw.get("g2p_state_path", "sync_state_g2p.json")),
        g2p_fields=parse_csv_fields(raw.get("g2p_fields")),
        g2p_max_pages=int(raw.get("g2p_max_pages", 2000)),
        g2p_allow_when_playnite_modified_missing=bool(
            raw.get("g2p_allow_when_playnite_modified_missing", True)
        ),
        g2p_incremental_cutoff=bool(raw.get("g2p_incremental_cutoff", True)),
    )

    if not cfg.token:
        raise ConfigError("config token is empty")
    if not cfg.grist_api_key:
        raise ConfigError("config grist_api_key is empty")
    if cfg.grist_batch_size <= 0:
        raise ConfigError("grist_batch_size must be > 0")
    if cfg.g2p_max_pages <= 0:
        raise ConfigError("g2p_max_pages must be > 0")

    return cfg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync selected fields from Grist back to Playnite")
    parser.add_argument("--config", default=str(CONFIG_PATH), help="Path to config file")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Force apply mode (write to Playnite)")
    mode.add_argument("--dry-run", action="store_true", help="Force dry-run mode")
    return parser.parse_args()


def http_json(method: str, url: str, headers: Dict[str, str], body: Optional[Any] = None) -> Any:
    payload = None
    request_headers = dict(headers)
    if body is not None:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        request_headers["Content-Type"] = "application/json"

    req = Request(url, headers=request_headers, data=payload, method=method)

    try:
        with urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            if not raw:
                return {}
            return json.loads(raw)
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {method} {url}\n{details}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error {method} {url}: {exc}") from exc


def parse_iso_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    # Grist trigger formulas can store epoch seconds as numeric values.
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    v = str(value).strip()
    if not v:
        return None

    # Numeric-like strings are also treated as epoch seconds.
    try:
        if v.replace(".", "", 1).isdigit():
            return datetime.fromtimestamp(float(v), tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        pass

    if v.endswith("Z"):
        v = v[:-1] + "+00:00"

    if "." in v and ("+" in v[10:] or "-" in v[10:]):
        left, right = v.split(".", 1)
        sign_pos = max(right.rfind("+"), right.rfind("-"))
        if sign_pos > 0:
            frac = right[:sign_pos]
            tz = right[sign_pos:]
            if len(frac) > 6:
                frac = frac[:6]
            v = left + "." + frac + tz

    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def grist_headers(cfg: Config) -> Dict[str, str]:
    token = cfg.grist_api_key.strip()
    auth_value = token if token.lower().startswith("bearer ") else f"Bearer {token}"
    return {
        "Authorization": auth_value,
        "X-Api-Key": token,
        "Accept": "application/json",
    }


def grist_url(cfg: Config, suffix: str) -> str:
    return f"{cfg.grist_base_url}/docs/{cfg.grist_doc_id}{suffix}"


def grist_sql_query(cfg: Config, sql: str) -> Dict[str, Any]:
    headers = grist_headers(cfg)
    url = grist_url(cfg, f"/sql?{urlencode({'q': sql})}")
    payload = http_json("GET", url, headers)
    return payload if isinstance(payload, dict) else {}


def _record_edited_dt(rec: Dict[str, Any]) -> Optional[datetime]:
    fields = rec.get("fields", {}) if isinstance(rec.get("fields"), dict) else {}
    return parse_iso_datetime(fields.get("editedAt"))


def fetch_grist_records(cfg: Config, cutoff_edited: Optional[datetime]) -> Tuple[List[Dict[str, Any]], bool]:
    # Preferred path: SQL pagination (LIMIT/OFFSET) is reliable on this deployment.
    # Order by editedAt DESC first so recent local edits are processed earlier.
    out: List[Dict[str, Any]] = []
    table_name_sql = '"' + cfg.grist_table_name.replace('"', '""') + '"'
    offset = 0
    page = 0
    order_by_clause = "editedAt desc, id desc"
    edited_order_supported = True
    cutoff_hit = False

    while page < cfg.g2p_max_pages:
        page += 1
        print(f"Fetching Grist SQL page {page} (offset={offset})...")
        sql = (
            f"select * from {table_name_sql} "
            f"order by {order_by_clause} limit {cfg.grist_batch_size} offset {offset}"
        )
        try:
            payload = grist_sql_query(cfg, sql)
        except RuntimeError as exc:
            # Fallback once if editedAt does not exist in old tables.
            if edited_order_supported and "editedat" in str(exc).lower():
                edited_order_supported = False
                order_by_clause = "modified desc, id desc"
                page -= 1
                print("Grist table has no editedAt column; fallback to order by modified.")
                continue
            raise
        records = payload.get("records", [])
        if not records:
            break

        if (
            cutoff_edited is not None
            and edited_order_supported
            and order_by_clause.startswith("editedAt")
        ):
            keep_count = len(records)
            for idx, rec in enumerate(records):
                redited = _record_edited_dt(rec)
                # Use strict less-than so equal-timestamp boundary rows are re-checked,
                # avoiding missed updates when multiple rows share the same editedAt value.
                if redited is not None and redited < cutoff_edited:
                    keep_count = idx
                    cutoff_hit = True
                    break
            if keep_count > 0:
                out.extend(records[:keep_count])
            if cutoff_hit:
                break
            if len(records) < cfg.grist_batch_size:
                break
            offset += len(records)
            continue

        out.extend(records)
        if len(records) < cfg.grist_batch_size:
            break
        offset += len(records)

    if out:
        return out, cutoff_hit

    # Fallback: one-shot records endpoint.
    headers = grist_headers(cfg)
    print("SQL pagination returned no records, fallback to one-shot endpoint...")
    payload = http_json("GET", grist_url(cfg, f"/tables/{cfg.grist_table_name}/records"), headers)
    return payload.get("records", []), False


def fetch_playnite_detail(cfg: Config, game_id: str) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {cfg.token}",
        "Accept": "application/json",
    }
    return http_json("GET", f"{cfg.base_url}/api/games/{game_id}", headers)


def fetch_playnite_modified_map(cfg: Config) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {cfg.token}",
        "Accept": "application/json",
    }
    out: Dict[str, Any] = {}
    offset = 0
    limit = 500
    total = None

    while True:
        params = {"limit": limit, "offset": offset, "hidden": "true"}
        url = f"{cfg.base_url}/api/games?{urlencode(params)}"
        print(f"Fetching Playnite modified map (offset={offset})...")
        payload = http_json("GET", url, headers)
        games = payload.get("games", [])
        if total is None:
            total = int(payload.get("total", 0))
        if not games:
            break

        for g in games:
            gid = g.get("id")
            if isinstance(gid, str) and gid:
                out[gid] = g.get("modified")

        offset += len(games)
        if total is not None and offset >= total:
            break

    return out


def update_playnite_game(cfg: Config, game_id: str, payload: Dict[str, Any]) -> None:
    headers = {
        "Authorization": f"Bearer {cfg.token}",
        "Accept": "application/json",
    }
    http_json("PUT", f"{cfg.base_url}/api/games/{game_id}", headers, payload)


def to_plain_list(value: Any) -> Optional[List[Any]]:
    if value is None:
        return None
    if not isinstance(value, list):
        return None
    if value and value[0] == "L":
        return value[1:]
    return value


def _normalize_links_list(items: List[Any]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            name = str(item.get("name", "")).strip()
            url = str(item.get("url", "")).strip()
            if url:
                out.append({"name": name or url, "url": url})
            continue

        if isinstance(item, str):
            text = item.strip()
            if not text:
                continue
            # Accept plain URL items as links.
            if text.startswith("http://") or text.startswith("https://"):
                out.append({"name": text, "url": text})

    return out


def parse_links_from_grist(value: Any) -> Optional[List[Dict[str, str]]]:
    if isinstance(value, list):
        normalized = _normalize_links_list(value)
        return normalized if normalized else None

    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    # First, support old JSON-string format.
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            normalized = _normalize_links_list(parsed)
            if normalized:
                return normalized
    except ValueError:
        pass

    # Then parse Markdown links, e.g. [DLsite](https://...)
    pattern = re.compile(r"\[([^\]]+)\]\((https?://[^)\s]+)\)")
    matches = pattern.findall(text)
    if matches:
        return [{"name": name.strip() or url.strip(), "url": url.strip()} for name, url in matches]

    return None


def normalize_release_date(value: Any) -> Optional[str]:
    if value is None:
        return None

    # Numeric input from Grist may be epoch seconds/milliseconds.
    if isinstance(value, (int, float)):
        ts = float(value)
        # Heuristic: milliseconds are much larger than seconds.
        if abs(ts) >= 1e11:
            ts = ts / 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
        except (OverflowError, OSError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None

    # Accept compact date format, e.g. 20260403.
    if re.fullmatch(r"\d{8}", text):
        try:
            return datetime.strptime(text, "%Y%m%d").date().isoformat()
        except ValueError:
            return None

    # Accept slashed/dotted date formats.
    for fmt in ("%Y/%m/%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass

    # Accept plain date string and ISO datetime-like strings.
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        text = text[:10]
        try:
            return datetime.strptime(text, "%Y-%m-%d").date().isoformat()
        except ValueError:
            pass

    # Fallback for numeric-like strings representing epoch seconds/milliseconds.
    if re.fullmatch(r"\d+(\.\d+)?", text):
        try:
            ts = float(text)
            if abs(ts) >= 1e11:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
        except (OverflowError, OSError, ValueError):
            return None

    return None


def normalize_g2p_payload(
    fields: Dict[str, Any],
    allowed: List[str],
    stats: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for key in allowed:
        if key in IGNORED_G2P_FIELDS:
            continue
        if key not in fields:
            continue
        val = fields.get(key)

        if key in LIST_FIELDS:
            plain = to_plain_list(val)
            if plain is None:
                continue
            payload[key] = [str(x) for x in plain if x is not None and str(x) != ""]
            continue

        if key == "links":
            parsed_links = parse_links_from_grist(val)
            if parsed_links is not None:
                payload[key] = parsed_links
            continue

        if key == "releaseDate":
            normalized = normalize_release_date(val)
            if normalized is not None:
                payload[key] = normalized
            elif val is not None and str(val).strip() and stats is not None:
                stats["invalid_release_date"] = stats.get("invalid_release_date", 0) + 1
            continue

        payload[key] = val

    return payload


def fingerprint_payload(game_id: str, payload: Dict[str, Any]) -> str:
    stable = {"playniteId": game_id, "fields": payload}
    encoded = json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def load_g2p_state(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return {}
    rows = raw.get("rows", {}) if isinstance(raw, dict) else {}
    if not isinstance(rows, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in rows.items():
        if isinstance(k, str) and isinstance(v, str):
            out[k] = v
    return out


def load_g2p_edited_watermark(path: Path) -> Optional[datetime]:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return None
    if not isinstance(raw, dict):
        return None
    wm = raw.get("lastMaxGristEditedAt")
    if wm in {None, ""}:
        # Backward compatibility for previous state key.
        wm = raw.get("lastMaxGristModified")
    return parse_iso_datetime(wm)


def save_g2p_state(path: Path, rows: Dict[str, str], last_max_grist_edited_at: Optional[str]) -> None:
    payload = {
        "version": 1,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "rows": rows,
        "lastMaxGristEditedAt": last_max_grist_edited_at or "",
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def should_apply(
    edited_at: Any,
    synced_at: Any,
    playnite_modified: Any,
    allow_when_playnite_missing: bool,
) -> bool:
    edited = parse_iso_datetime(edited_at)
    synced = parse_iso_datetime(synced_at)
    # Treat missing/invalid editedAt as infinitely old.
    if edited is None:
        return False
    # Synced timestamp must exist.
    if synced is None:
        return False
    if edited <= synced:
        return False
    p = parse_iso_datetime(playnite_modified)
    if p is None:
        return allow_when_playnite_missing
    if edited <= p:
        return False
    return True


def main() -> int:
    try:
        args = parse_args()
        cfg = load_config(Path(args.config))
        apply_mode = True
        if args.apply:
            apply_mode = True
        elif args.dry_run:
            apply_mode = False
        state_path = Path(cfg.g2p_state_path)
        prev_state = load_g2p_state(state_path)
        previous_cutoff = load_g2p_edited_watermark(state_path) if cfg.g2p_incremental_cutoff else None
        next_state: Dict[str, str] = {}

        records, cutoff_hit = fetch_grist_records(cfg, previous_cutoff)
        playnite_modified_map = fetch_playnite_modified_map(cfg)
        scanned = 0
        changed = 0
        applied = 0
        skipped_by_fingerprint = 0
        skipped_by_policy = 0
        skipped_no_payload = 0
        normalize_stats: Dict[str, int] = {"invalid_release_date": 0}
        max_grist_edited_seen: Optional[datetime] = None

        for rec in records:
            fields = rec.get("fields", {}) if isinstance(rec.get("fields"), dict) else {}
            game_id = fields.get("playniteId")
            if not isinstance(game_id, str) or not game_id:
                continue
            raw_name = fields.get("name")
            game_name = str(raw_name).strip() if raw_name is not None else ""
            display_name = game_name if game_name else "<unknown>"

            scanned += 1
            row_edited_dt = parse_iso_datetime(fields.get("editedAt"))
            if row_edited_dt is not None and (
                max_grist_edited_seen is None or row_edited_dt > max_grist_edited_seen
            ):
                max_grist_edited_seen = row_edited_dt

            payload = normalize_g2p_payload(fields, cfg.g2p_fields, normalize_stats)
            if not payload:
                skipped_no_payload += 1
                print(f"{display_name}({game_id}) SKIP_NO_PAYLOAD")
                continue

            fp = fingerprint_payload(game_id, payload)

            if prev_state.get(game_id) == fp:
                next_state[game_id] = fp
                skipped_by_fingerprint += 1
                continue

            changed += 1

            if not should_apply(
                fields.get("editedAt"),
                fields.get("syncedAt"),
                playnite_modified_map.get(game_id),
                cfg.g2p_allow_when_playnite_modified_missing,
            ):
                # Consume baseline on policy-skip rows to prevent repeated changed-noise.
                # Future user edits will still produce a new fingerprint and be re-evaluated.
                next_state[game_id] = fp
                skipped_by_policy += 1
                print(f"{display_name}({game_id}) SKIP_POLICY")
                continue

            if apply_mode:
                update_playnite_game(cfg, game_id, payload)
                # Advance baseline only on successful apply.
                next_state[game_id] = fp
                applied += 1
                print(f"{display_name}({game_id}) APPLY fields={','.join(sorted(payload.keys()))}")
            else:
                # Dry mode: do not consume pending changes.
                if game_id in prev_state:
                    next_state[game_id] = prev_state[game_id]
                print(f"{display_name}({game_id}) DRY_PENDING fields={','.join(sorted(payload.keys()))}")

            if scanned % 100 == 0:
                print(f"Progress: scanned={scanned}, changed={changed}, applied={applied}")

        # Preserve baseline for ids not touched in this run.
        for k, v in prev_state.items():
            if k not in next_state:
                next_state[k] = v

        last_max_grist_edited_at = (
            max_grist_edited_seen.isoformat() if max_grist_edited_seen is not None else None
        )
        save_g2p_state(state_path, next_state, last_max_grist_edited_at)

    except (ConfigError, RuntimeError) as exc:
        print(f"Error: {exc}")
        return 1

    print("G2P sync completed.")
    print(f"Apply mode: {'ON' if apply_mode else 'OFF (dry)'}")
    print(f"Scanned records: {scanned}")
    print(f"Changed fingerprints: {changed}")
    print(f"Applied to Playnite: {applied}")
    print(f"Skipped (fingerprint unchanged): {skipped_by_fingerprint}")
    print(f"Skipped (policy Playnite priority): {skipped_by_policy}")
    print(f"Skipped (no writable payload): {skipped_no_payload}")
    print(f"Skipped (invalid releaseDate format): {normalize_stats.get('invalid_release_date', 0)}")
    if cfg.g2p_incremental_cutoff:
        print(
            "Incremental cutoff: "
            + ("hit (early stop)" if cutoff_hit else "not hit")
        )
    print(f"State file: {cfg.g2p_state_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
