#!/usr/bin/env python3
"""Full sync Playnite games into Grist with auto table/column provisioning.

Behavior:
- Pull all games from Playnite Bridge API with pagination.
- Convert list fields to Grist list-cell format: ["L", ...].
- Ensure target Grist table exists.
- Ensure required columns exist.
- Upsert by business key `id` (Playnite id), not by Grist record id.
"""

from __future__ import annotations

import json
import hashlib
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


CONFIG_PATH = Path("config.yaml")
BUSINESS_KEY = "playniteId"
EDITED_AT_COLUMN = "editedAt"
UNWANTED_FIELDS = {
    "hasIcon",
    "hasCover",
    "hasBackground",
    "version",
    "installDirectory",
    "installSize",
}


def grist_column_type(column_id: str) -> str:
    """Return Grist column type for known fields, fallback to Any."""
    type_map = {
        BUSINESS_KEY: "Text",
        "name": "Text",
        "source": "Text",
        "links": "Text",
        "genres": "ChoiceList",
        "categories": "ChoiceList",
        "tags": "ChoiceList",
        "features": "ChoiceList",
        "platforms": "ChoiceList",
        "developers": "ChoiceList",
        "publishers": "ChoiceList",
        "series": "ChoiceList",
        "ageRatings": "ChoiceList",
        "regions": "ChoiceList",
        "completionStatus": "Text",
        "isInstalled": "Bool",
        "favorite": "Bool",
        "hidden": "Bool",
        "playtime": "Numeric",
        "playCount": "Numeric",
        "lastActivity": "DateTime:Asia/Shanghai",
        "modified": "DateTime:Asia/Shanghai",
        EDITED_AT_COLUMN: "DateTime:Asia/Shanghai",
        "added": "DateTime:Asia/Shanghai",
        "releaseDate": "Date",
        "userScore": "Numeric",
        "communityScore": "Numeric",
        "criticScore": "Numeric",
        "syncedAt": "DateTime:Asia/Shanghai",
    }
    return type_map.get(column_id, "Any")


def grist_column_widget_options(column_id: str) -> Optional[str]:
    if column_id == "links":
        return json.dumps({"widget": "Markdown"}, ensure_ascii=False)
    if not grist_column_type(column_id).startswith("DateTime"):
        return None
    return json.dumps(
        {
            "widget": "TextBox",
            "dateFormat": "YYYY-MM-DD",
            "timeFormat": "HH:mm",
            "isCustomDateFormat": False,
            "isCustomTimeFormat": False,
            "alignment": "left",
        },
        ensure_ascii=False,
    )


def grist_column_fields(column_id: str) -> Dict[str, Any]:
    fields: Dict[str, Any] = {"type": grist_column_type(column_id)}
    widget_options = grist_column_widget_options(column_id)
    if widget_options is not None:
        fields["widgetOptions"] = widget_options
    return fields


class ConfigError(Exception):
    pass


@dataclass
class Config:
    base_url: str
    token: str
    limit: int
    max_pages: int
    include_hidden: bool
    grist_base_url: str
    grist_doc_id: str
    grist_api_key: str
    grist_table_name: str
    grist_batch_size: int
    grist_register_choices: bool
    grist_delete_missing: bool
    detail_sync_enabled: bool
    detail_full_backfill: bool
    sync_state_path: str
    g2p_fields: List[str]


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


def parse_csv_fields(raw: Any) -> List[str]:
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    if raw is None:
        return []
    text = str(raw).strip()
    if not text:
        return []
    return [x.strip() for x in text.split(",") if x.strip()]


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


def load_config(path: Path) -> Config:
    raw = load_simple_yaml(path)
    required = [
        "base_url",
        "token",
        "limit",
        "max_pages",
        "include_hidden",
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
        limit=int(raw["limit"]),
        max_pages=int(raw["max_pages"]),
        include_hidden=bool(raw["include_hidden"]),
        grist_base_url=str(raw["grist_base_url"]).rstrip("/"),
        grist_doc_id=str(raw["grist_doc_id"]),
        grist_api_key=str(raw["grist_api_key"]),
        grist_table_name=str(raw["grist_table_name"]),
        grist_batch_size=int(raw["grist_batch_size"]),
        grist_register_choices=bool(raw.get("grist_register_choices", True)),
        grist_delete_missing=bool(raw.get("grist_delete_missing", True)),
        detail_sync_enabled=bool(raw.get("detail_sync_enabled", True)),
        detail_full_backfill=bool(raw.get("detail_full_backfill", False)),
        sync_state_path=str(raw.get("sync_state_path", "sync_state.json")),
        g2p_fields=parse_csv_fields(raw.get("g2p_fields")),
    )

    if not cfg.token:
        raise ConfigError("config token is empty")
    if not cfg.grist_doc_id:
        raise ConfigError("config grist_doc_id is empty")
    if not cfg.grist_api_key or "PASTE" in cfg.grist_api_key.upper():
        raise ConfigError("Please set a real grist_api_key in config.yaml")
    if not cfg.grist_table_name:
        raise ConfigError("config grist_table_name is empty")
    if cfg.limit <= 0 or cfg.max_pages <= 0 or cfg.grist_batch_size <= 0:
        raise ConfigError("limit, max_pages, grist_batch_size must be > 0")

    return cfg


def http_json(method: str, url: str, headers: Dict[str, str], body: Optional[Dict[str, Any]] = None) -> Any:
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


def fetch_playnite_games(config: Config) -> List[Dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {config.token}",
        "Accept": "application/json",
    }
    games: List[Dict[str, Any]] = []

    offset = 0
    pages = 0
    total = None

    while pages < config.max_pages:
        params = {
            "limit": config.limit,
            "offset": offset,
            "hidden": str(config.include_hidden).lower(),
        }
        url = f"{config.base_url}/api/games?{urlencode(params)}"
        payload = http_json("GET", url, headers)

        page_games = payload.get("games", [])
        if total is None:
            total = int(payload.get("total", 0))

        if not page_games:
            break

        games.extend(page_games)
        offset += len(page_games)
        pages += 1

        if total is not None and offset >= total:
            break

    return games


def fetch_playnite_game_detail(config: Config, game_id: str) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {config.token}",
        "Accept": "application/json",
    }
    url = f"{config.base_url}/api/games/{game_id}"
    payload = http_json("GET", url, headers)
    return payload if isinstance(payload, dict) else {}


def to_grist_list(value: Any) -> List[Any]:
    if not isinstance(value, list):
        return ["L"]
    if value and value[0] == "L":
        return value
    return ["L", *value]


def links_to_markdown(value: Any) -> str:
    links = value
    if isinstance(links, str):
        text = links.strip()
        if not text:
            return ""
        try:
            parsed = json.loads(text)
        except ValueError:
            return text
        if isinstance(parsed, list):
            links = parsed
        else:
            return text

    if not isinstance(links, list):
        return ""

    parts: List[str] = []
    for item in links:
        if isinstance(item, dict):
            name = str(item.get("name", "")).strip()
            url = str(item.get("url", "")).strip()
            if url:
                label = name or url
                parts.append(f"[{label}]({url})")
            elif name:
                parts.append(name)
            continue

        if isinstance(item, str) and item.strip():
            parts.append(item.strip())

    return ", ".join(parts)


def normalize_game(game: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(game)

    for field in UNWANTED_FIELDS:
        row.pop(field, None)

    # Grist reserves "id" for internal record id, so keep source id in a custom key.
    row[BUSINESS_KEY] = str(row.get("id", ""))
    row.pop("id", None)

    for field in [
        "genres",
        "categories",
        "tags",
        "features",
        "platforms",
        "developers",
        "publishers",
        "series",
        "ageRatings",
        "regions",
    ]:
        row[field] = to_grist_list(row.get(field, []))

    # Convert links to markdown text, example: [DLsite](https://...)
    row["links"] = links_to_markdown(row.get("links"))

    # Safety net for unexpected list-valued fields from detail payloads.
    for k, v in list(row.items()):
        if isinstance(v, list):
            if k == "links":
                continue
            if all(isinstance(item, (str, int, float, bool)) or item is None for item in v):
                row[k] = to_grist_list(v)
            else:
                row[k] = json.dumps(v, ensure_ascii=False)

    # Keep a synchronization marker for observability.
    row["syncedAt"] = datetime.now(timezone.utc).isoformat()

    return row


def state_file_path(config: Config) -> Path:
    return Path(config.sync_state_path)


def load_sync_state_payload(config: Config) -> Dict[str, Any]:
    path = state_file_path(config)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return {}

    return payload if isinstance(payload, dict) else {}


def load_sync_state_rows(config: Config) -> Dict[str, str]:
    payload = load_sync_state_payload(config)

    rows = payload.get("rows", {}) if isinstance(payload, dict) else {}
    if not isinstance(rows, dict):
        return {}

    result: Dict[str, str] = {}
    for k, v in rows.items():
        if isinstance(k, str) and isinstance(v, str):
            result[k] = v
    return result


def load_sync_state_modified(config: Config) -> Dict[str, str]:
    payload = load_sync_state_payload(config)
    marks = payload.get("modified", {}) if isinstance(payload, dict) else {}
    if not isinstance(marks, dict):
        return {}

    result: Dict[str, str] = {}
    for k, v in marks.items():
        if isinstance(k, str) and isinstance(v, str):
            result[k] = v
    return result


def save_sync_state(config: Config, fingerprints: Dict[str, str], modified_marks: Dict[str, str]) -> None:
    path = state_file_path(config)
    payload = {
        "version": 2,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "rows": fingerprints,
        "modified": modified_marks,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def fingerprint_row(row: Dict[str, Any]) -> str:
    # syncedAt is run-specific metadata and must not affect change detection.
    stable = {k: v for k, v in row.items() if k != "syncedAt"}
    encoded = json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def select_changed_rows(
    rows: List[Dict[str, Any]],
    state: Dict[str, str],
    existing_in_grist: Optional[set[str]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, str], List[str]]:
    changed: List[Dict[str, Any]] = []
    next_state: Dict[str, str] = {}

    for row in rows:
        business_id = row.get(BUSINESS_KEY)
        if not isinstance(business_id, str) or not business_id:
            continue

        fp = fingerprint_row(row)
        next_state[business_id] = fp
        should_upsert = state.get(business_id) != fp
        # If a row is missing in Grist (manual delete), force reinsert.
        if existing_in_grist is not None and business_id not in existing_in_grist:
            should_upsert = True

        if should_upsert:
            changed.append(row)

    removed_ids = [k for k in state.keys() if k not in next_state]
    return changed, next_state, removed_ids


def collect_columns(rows: List[Dict[str, Any]]) -> List[str]:
    ordered: List[str] = []
    seen = set()

    # Keep business key first for upsert key semantics.
    preferred = [BUSINESS_KEY]
    for key in preferred:
        if key not in seen:
            ordered.append(key)
            seen.add(key)

    for row in rows:
        for key in row.keys():
            if key not in seen:
                ordered.append(key)
                seen.add(key)

    # Keep local edit timestamp column present even if source rows do not include it.
    if EDITED_AT_COLUMN not in seen:
        ordered.append(EDITED_AT_COLUMN)
        seen.add(EDITED_AT_COLUMN)

    return ordered


def grist_headers(config: Config) -> Dict[str, str]:
    token = config.grist_api_key.strip()
    auth_value = token if token.lower().startswith("bearer ") else f"Bearer {token}"
    return {
        "Authorization": auth_value,
        "X-Api-Key": token,
        "Accept": "application/json",
    }


def grist_url(config: Config, suffix: str) -> str:
    return f"{config.grist_base_url}/docs/{config.grist_doc_id}{suffix}"


def grist_sql_query(config: Config, sql: str) -> Dict[str, Any]:
    headers = grist_headers(config)
    url = grist_url(config, f"/sql?{urlencode({'q': sql})}")
    payload = http_json("GET", url, headers)
    return payload if isinstance(payload, dict) else {}


def list_grist_tables(config: Config) -> List[str]:
    headers = grist_headers(config)
    payload = http_json("GET", grist_url(config, "/tables"), headers)
    tables = payload.get("tables", []) if isinstance(payload, dict) else []
    names: List[str] = []
    for t in tables:
        table_id = t.get("id")
        if isinstance(table_id, str) and table_id:
            names.append(table_id)
    return names


def ensure_table(config: Config, columns: List[str]) -> str:
    headers = grist_headers(config)
    existing_tables = list_grist_tables(config)

    # Prefer exact match; fallback to case-insensitive match.
    if config.grist_table_name in existing_tables:
        print(f"Table exists: {config.grist_table_name}")
        return config.grist_table_name

    lower_map = {name.lower(): name for name in existing_tables}
    matched = lower_map.get(config.grist_table_name.lower())
    if matched:
        print(f"Table exists (case-insensitive match): {matched}")
        return matched

    table_columns = [
        {"id": BUSINESS_KEY, "fields": grist_column_fields(BUSINESS_KEY)}
    ]
    for col in columns:
        if col == BUSINESS_KEY:
            continue
        table_columns.append({"id": col, "fields": grist_column_fields(col)})

    create_payload = {
        "tables": [
            {
                "id": config.grist_table_name,
                "columns": table_columns,
            }
        ]
    }

    try:
        result = http_json("POST", grist_url(config, "/tables"), headers, create_payload)
        created_name = config.grist_table_name
        if isinstance(result, dict):
            tables = result.get("tables", [])
            if tables and isinstance(tables[0], dict):
                created_name = str(tables[0].get("id", created_name))
        if created_name != config.grist_table_name:
            print(
                f"Created table with renamed id: {created_name} (requested: {config.grist_table_name})"
            )
        else:
            print(f"Created table: {created_name}")
        return created_name
    except RuntimeError as exc:
        # Table already exists in many Grist deployments returns HTTP 400/409.
        msg = str(exc)
        if "HTTP 400" in msg or "HTTP 409" in msg or "already exists" in msg.lower():
            # Re-check list to avoid creating duplicates on platforms that auto-rename.
            refreshed = list_grist_tables(config)
            if config.grist_table_name in refreshed:
                print(f"Table exists: {config.grist_table_name}")
                return config.grist_table_name
            lower_map = {name.lower(): name for name in refreshed}
            matched = lower_map.get(config.grist_table_name.lower())
            if matched:
                print(f"Table exists (case-insensitive match): {matched}")
                return matched
            raise RuntimeError(
                f"Table create conflict, but target table not found: {config.grist_table_name}"
            )
        raise


def fetch_existing_columns(config: Config) -> List[str]:
    headers = grist_headers(config)
    payload = http_json(
        "GET",
        grist_url(config, f"/tables/{config.grist_table_name}/columns"),
        headers,
    )
    records = payload.get("columns", payload if isinstance(payload, list) else [])
    cols: List[str] = []
    for c in records:
        col_id = c.get("id")
        if col_id:
            cols.append(col_id)
    return cols


def fetch_existing_column_types(config: Config) -> Dict[str, str]:
    headers = grist_headers(config)
    payload = http_json(
        "GET",
        grist_url(config, f"/tables/{config.grist_table_name}/columns"),
        headers,
    )
    records = payload.get("columns", payload if isinstance(payload, list) else [])
    result: Dict[str, str] = {}
    for c in records:
        col_id = c.get("id")
        if not isinstance(col_id, str) or not col_id:
            continue
        fields = c.get("fields", {}) if isinstance(c.get("fields"), dict) else {}
        col_type = fields.get("type")
        if isinstance(col_type, str) and col_type:
            result[col_id] = col_type
    return result


def fetch_existing_columns_meta(config: Config) -> Dict[str, Dict[str, Any]]:
    headers = grist_headers(config)
    payload = http_json(
        "GET",
        grist_url(config, f"/tables/{config.grist_table_name}/columns"),
        headers,
    )
    records = payload.get("columns", payload if isinstance(payload, list) else [])
    result: Dict[str, Dict[str, Any]] = {}
    for c in records:
        col_id = c.get("id")
        if isinstance(col_id, str) and col_id:
            result[col_id] = c
    return result


def _extract_column_ref(meta: Dict[str, Any]) -> Optional[int]:
    fields = meta.get("fields", {}) if isinstance(meta.get("fields"), dict) else {}
    for key in ["colRef", "id"]:
        value = fields.get(key)
        if isinstance(value, int):
            return value
    top_id = meta.get("id")
    if isinstance(top_id, int):
        return top_id
    return None


def _watch_fields_for_edited_at(config: Config, desired_columns: List[str]) -> List[str]:
    desired_set = set(desired_columns)
    watched: List[str] = []
    seen = set()
    for field in config.g2p_fields:
        if field in {BUSINESS_KEY, "modified", EDITED_AT_COLUMN, "syncedAt"}:
            continue
        if field in desired_set and field not in seen:
            watched.append(field)
            seen.add(field)
    return watched


def _build_edited_at_recalc_deps(config: Config, desired_columns: List[str], existing_meta: Dict[str, Dict[str, Any]]) -> List[Any]:
    watched_fields = _watch_fields_for_edited_at(config, desired_columns)
    refs: List[int] = []
    for field in watched_fields:
        col_meta = existing_meta.get(field)
        if not col_meta:
            continue
        ref = _extract_column_ref(col_meta)
        if isinstance(ref, int):
            refs.append(ref)

    # Grist list-cell format required by recalcDeps.
    return ["L", *refs]


def ensure_columns(config: Config, desired_columns: List[str]) -> None:
    existing = set(fetch_existing_columns(config))
    missing = [c for c in desired_columns if c not in existing]
    if not missing:
        return

    headers = grist_headers(config)
    body = {
        "columns": [
            {"id": c, "fields": {"type": grist_column_type(c)}}
            for c in missing
        ]
    }
    http_json(
        "POST",
        grist_url(config, f"/tables/{config.grist_table_name}/columns"),
        headers,
        body,
    )
    print(f"Added columns: {', '.join(missing)}")


def reconcile_column_types(config: Config, desired_columns: List[str]) -> None:
    existing_meta = fetch_existing_columns_meta(config)
    edited_at_recalc_deps = _build_edited_at_recalc_deps(config, desired_columns, existing_meta)
    updates = []
    for c in desired_columns:
        expected = grist_column_type(c)
        expected_widget_options = grist_column_widget_options(c)
        expected_formula: Optional[str] = None
        expected_is_formula: Optional[bool] = None
        expected_recalc_when: Optional[int] = None
        expected_recalc_deps: Optional[List[Any]] = None
        if c == EDITED_AT_COLUMN:
            expected_is_formula = False
            expected_formula = "NOW()"
            expected_recalc_when = 0
            expected_recalc_deps = edited_at_recalc_deps
        elif c == "modified":
            expected_is_formula = False
            expected_formula = ""
            expected_recalc_when = 0

        col_meta = existing_meta.get(c, {})
        fields = col_meta.get("fields", {}) if isinstance(col_meta.get("fields"), dict) else {}
        actual = fields.get("type") if isinstance(fields.get("type"), str) else None
        actual_widget_options = fields.get("widgetOptions")
        actual_formula = fields.get("formula") if isinstance(fields.get("formula"), str) else None
        actual_is_formula = fields.get("isFormula") if isinstance(fields.get("isFormula"), bool) else None
        actual_recalc_when = fields.get("recalcWhen")
        actual_recalc_deps = fields.get("recalcDeps")

        patch_fields: Dict[str, Any] = {}
        if actual and actual != expected:
            patch_fields["type"] = expected
        if expected_formula is not None and actual_formula != expected_formula:
            patch_fields["formula"] = expected_formula
        if expected_is_formula is not None and actual_is_formula != expected_is_formula:
            patch_fields["isFormula"] = expected_is_formula
        if expected_recalc_when is not None and actual_recalc_when != expected_recalc_when:
            patch_fields["recalcWhen"] = expected_recalc_when
        if expected_recalc_deps is not None and actual_recalc_deps != expected_recalc_deps:
            patch_fields["recalcDeps"] = expected_recalc_deps
        if expected_widget_options is not None and actual_widget_options != expected_widget_options:
            patch_fields["widgetOptions"] = expected_widget_options

        if patch_fields:
            updates.append({"id": c, "fields": patch_fields})

    if not updates:
        return

    headers = grist_headers(config)
    # Some Grist deployments require same field keys for every column in one PATCH.
    # Apply per-column updates to stay compatible across deployments.
    for update in updates:
        http_json(
            "PATCH",
            grist_url(config, f"/tables/{config.grist_table_name}/columns"),
            headers,
            {"columns": [update]},
        )
    changed = ", ".join([u["id"] for u in updates])
    print(f"Updated column types: {changed}")


def _choice_labels_from_widget_options(widget_options: str) -> List[str]:
    if not widget_options:
        return []
    try:
        parsed = json.loads(widget_options)
    except (TypeError, ValueError):
        return []

    choices = parsed.get("choices", []) if isinstance(parsed, dict) else []
    labels: List[str] = []
    for ch in choices:
        if isinstance(ch, str) and ch:
            labels.append(ch)
        elif isinstance(ch, dict):
            label = ch.get("label")
            if isinstance(label, str) and label:
                labels.append(label)
    return labels


def _choice_labels_from_rows(rows: List[Dict[str, Any]], column_id: str) -> List[str]:
    labels: List[str] = []
    seen = set()
    for row in rows:
        value = row.get(column_id)
        if not isinstance(value, list):
            continue
        items = value[1:] if value and value[0] == "L" else value
        for item in items:
            if isinstance(item, str) and item and item != "L" and item not in seen:
                labels.append(item)
                seen.add(item)
    return labels


def append_missing_choices(config: Config, rows: List[Dict[str, Any]], desired_columns: List[str]) -> None:
    if not config.grist_register_choices:
        return

    col_types = fetch_existing_column_types(config)
    meta = fetch_existing_columns_meta(config)
    updates = []

    for col in desired_columns:
        if col_types.get(col) != "ChoiceList":
            continue

        col_meta = meta.get(col, {})
        fields = col_meta.get("fields", {}) if isinstance(col_meta.get("fields"), dict) else {}
        widget_options = fields.get("widgetOptions")
        existing_labels = _choice_labels_from_widget_options(widget_options if isinstance(widget_options, str) else "")
        cleaned_existing_labels: List[str] = []
        seen_existing = set()
        for label in existing_labels:
            if not isinstance(label, str) or not label or label == "L" or label in seen_existing:
                continue
            cleaned_existing_labels.append(label)
            seen_existing.add(label)

        options_changed = cleaned_existing_labels != existing_labels
        existing_labels = cleaned_existing_labels
        existing_set = set(existing_labels)

        row_labels = _choice_labels_from_rows(rows, col)
        missing = [x for x in row_labels if x not in existing_set]
        if not missing and not options_changed:
            continue

        merged = existing_labels + missing
        new_widget_options = json.dumps({"choices": merged}, ensure_ascii=False)
        updates.append({"id": col, "fields": {"widgetOptions": new_widget_options}})

    if not updates:
        return

    headers = grist_headers(config)
    http_json(
        "PATCH",
        grist_url(config, f"/tables/{config.grist_table_name}/columns"),
        headers,
        {"columns": updates},
    )
    print("Updated choices for columns: " + ", ".join([u["id"] for u in updates]))


def fetch_existing_record_map(config: Config) -> Dict[str, int]:
    headers = grist_headers(config)
    record_map: Dict[str, int] = {}
    parsed_any = False

    # Prefer SQL pagination because /records offset can be unreliable on some deployments.
    table_name_sql = '"' + config.grist_table_name.replace('"', '""') + '"'
    offset = 0
    page_size = 2000
    sql_ok = False

    try:
        while True:
            sql = (
                f"select id, {BUSINESS_KEY} from {table_name_sql} "
                f"order by id limit {page_size} offset {offset}"
            )
            payload = grist_sql_query(config, sql)
            records = payload.get("records", [])
            if not records:
                break

            sql_ok = True
            for rec in records:
                fields = rec.get("fields", {}) if isinstance(rec.get("fields"), dict) else {}
                # SQL endpoint may return selected id inside fields.id, not top-level id.
                rec_id_raw = rec.get("id", fields.get("id"))
                business_raw = fields.get(BUSINESS_KEY)
                business_id = str(business_raw).strip() if business_raw is not None else ""
                if rec_id_raw is None or not business_id:
                    continue
                try:
                    record_map[business_id] = int(rec_id_raw)
                    parsed_any = True
                except (TypeError, ValueError):
                    continue

            if len(records) < page_size:
                break
            offset += len(records)
    except RuntimeError as exc:
        print(f"SQL record map fetch failed, fallback to one-shot endpoint: {exc}")

    if sql_ok:
        return record_map

    # Fallback path: one-shot records endpoint (no offset pagination).
    payload = http_json("GET", grist_url(config, f"/tables/{config.grist_table_name}/records"), headers)
    records = payload.get("records", [])
    for rec in records:
        fields = rec.get("fields", {}) if isinstance(rec.get("fields"), dict) else {}
        rec_id_raw = rec.get("id", fields.get("id"))
        business_raw = fields.get(BUSINESS_KEY)
        business_id = str(business_raw).strip() if business_raw is not None else ""
        if rec_id_raw is None or not business_id:
            continue
        try:
            record_map[business_id] = int(rec_id_raw)
            parsed_any = True
        except (TypeError, ValueError):
            continue

    # Safety guard: if Grist returns rows but no business key mapping can be parsed,
    # fail fast to avoid silently treating all source rows as new inserts.
    if records and not parsed_any:
        raise RuntimeError(
            "Failed to parse existing Grist record ids/business keys from /records response. "
            "Aborting to prevent duplicate inserts."
        )

    return record_map


def chunked(items: List[Any], size: int) -> List[List[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def upsert_records(config: Config, rows: List[Dict[str, Any]]) -> Tuple[int, int]:
    headers = grist_headers(config)
    existing = fetch_existing_record_map(config)

    to_add: List[Dict[str, Any]] = []
    to_update: List[Dict[str, Any]] = []
    inserted_record_ids: List[int] = []

    for row in rows:
        business_id = row.get(BUSINESS_KEY)
        if not isinstance(business_id, str) or not business_id:
            continue

        if business_id in existing:
            to_update.append({"id": existing[business_id], "fields": row})
        else:
            to_add.append({"fields": row})

    for batch in chunked(to_add, config.grist_batch_size):
        body = {"records": batch}
        result = http_json(
            "POST",
            grist_url(config, f"/tables/{config.grist_table_name}/records"),
            headers,
            body,
        )
        records = result.get("records", []) if isinstance(result, dict) else []
        for rec in records:
            rec_id = rec.get("id")
            if isinstance(rec_id, int):
                inserted_record_ids.append(rec_id)

    # New rows may initialize editedAt via trigger formula; force baseline to null.
    if inserted_record_ids:
        for batch in chunked(inserted_record_ids, config.grist_batch_size):
            body = {
                "records": [
                    {"id": rec_id, "fields": {EDITED_AT_COLUMN: None}}
                    for rec_id in batch
                ]
            }
            http_json(
                "PATCH",
                grist_url(config, f"/tables/{config.grist_table_name}/records"),
                headers,
                body,
            )

    for batch in chunked(to_update, config.grist_batch_size):
        body = {"records": batch}
        http_json(
            "PATCH",
            grist_url(config, f"/tables/{config.grist_table_name}/records"),
            headers,
            body,
        )

    return len(to_add), len(to_update)


def delete_missing_records(config: Config, missing_business_ids: List[str]) -> int:
    if not missing_business_ids:
        return 0

    headers = grist_headers(config)
    existing = fetch_existing_record_map(config)
    rec_ids = [existing[b] for b in missing_business_ids if b in existing]
    if not rec_ids:
        return 0

    deleted = 0
    for batch in chunked(rec_ids, config.grist_batch_size):
        # Primary path for this Grist deployment.
        try:
            # This deployment expects raw JSON array body: [54, 55, ...]
            payload = json.dumps(batch, ensure_ascii=False).encode("utf-8")
            req_headers = dict(headers)
            req_headers["Content-Type"] = "application/json"
            req = Request(
                grist_url(config, f"/tables/{config.grist_table_name}/records/delete"),
                headers=req_headers,
                data=payload,
                method="POST",
            )
            with urlopen(req, timeout=30) as resp:
                _ = resp.read()
        except RuntimeError:
            # Compatibility fallback for deployments that support DELETE /records.
            http_json(
                "DELETE",
                grist_url(config, f"/tables/{config.grist_table_name}/records"),
                headers,
                {"records": batch},
            )
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            # Secondary fallback: some deployments accept object payload instead.
            try:
                http_json(
                    "POST",
                    grist_url(config, f"/tables/{config.grist_table_name}/records/delete"),
                    headers,
                    {"records": batch},
                )
            except RuntimeError:
                raise RuntimeError(
                    f"HTTP {exc.code} POST {grist_url(config, f'/tables/{config.grist_table_name}/records/delete')}\n{details}"
                ) from exc
        except URLError as exc:
            raise RuntimeError(
                f"Network error POST {grist_url(config, f'/tables/{config.grist_table_name}/records/delete')}: {exc}"
            ) from exc
        deleted += len(batch)

    return deleted


def main() -> int:
    try:
        config = load_config(CONFIG_PATH)
        games = fetch_playnite_games(config)

        previous_state = load_sync_state_rows(config)
        previous_modified = load_sync_state_modified(config)

        detail_fetch_count = 0
        games_merged: List[Dict[str, Any]] = []
        next_modified: Dict[str, str] = {}

        for g in games:
            game_id = str(g.get("id", ""))
            if not game_id:
                continue

            modified_raw = g.get("modified")
            modified_str = str(modified_raw) if modified_raw is not None else ""
            if modified_str:
                next_modified[game_id] = modified_str

            need_detail = False
            if config.detail_sync_enabled:
                if config.detail_full_backfill:
                    need_detail = True
                elif game_id not in previous_state:
                    need_detail = True
                elif modified_str and previous_modified.get(game_id) != modified_str:
                    need_detail = True

            if need_detail:
                detail = fetch_playnite_game_detail(config, game_id)
                if detail:
                    merged = dict(g)
                    merged.update(detail)
                    games_merged.append(merged)
                    detail_fetch_count += 1
                    # Prefer modified from detail if present.
                    dmod = detail.get("modified")
                    dmod_str = str(dmod) if dmod is not None else ""
                    if dmod_str:
                        next_modified[game_id] = dmod_str
                    continue

            games_merged.append(g)

        rows = [normalize_game(g) for g in games_merged]

        if not rows:
            print("No games found from Playnite API.")
            return 0

        columns = collect_columns(rows)
        actual_table = ensure_table(config, columns)
        if actual_table != config.grist_table_name:
            print(f"Using resolved table name: {actual_table}")
            config.grist_table_name = actual_table
        ensure_columns(config, columns)
        reconcile_column_types(config, columns)

        existing_record_map = fetch_existing_record_map(config)
        existing_in_grist = set(existing_record_map.keys())

        changed_rows, next_state, removed_ids = select_changed_rows(
            rows,
            previous_state,
            existing_in_grist,
        )

        deleted = 0
        if config.grist_delete_missing and removed_ids:
            deleted = delete_missing_records(config, removed_ids)
            if deleted > 0:
                print(f"Deleted missing rows in Grist: {deleted}")
        elif removed_ids:
            print(f"Note: {len(removed_ids)} ids missing in Playnite (deletion disabled).")

        # Keep ChoiceList options healthy even when no row payload changes.
        append_missing_choices(config, rows, columns)

        if not changed_rows:
            print("No changed rows detected by local fingerprint state.")
            save_sync_state(config, next_state, next_modified)
            print(f"Detail fetch count: {detail_fetch_count}")
            print(f"State file: {config.sync_state_path}")
            return 0

        for row in changed_rows:
            business_id = row.get(BUSINESS_KEY)
            if not isinstance(business_id, str) or not business_id:
                continue
            action = "UPDATE" if business_id in existing_record_map else "INSERT"
            name = row.get("name")
            game_name = str(name).strip() if name is not None else ""
            display_name = game_name if game_name else "<unknown>"
            print(f"{display_name}({business_id}) {action}")

        inserted, updated = upsert_records(config, changed_rows)
        save_sync_state(config, next_state, next_modified)

    except (ConfigError, RuntimeError) as exc:
        print(f"Error: {exc}")
        return 1

    print("Sync completed.")
    print(f"Source games: {len(rows)}")
    print(f"Detail fetch count: {detail_fetch_count}")
    if config.detail_full_backfill:
        print("Detail full backfill mode: enabled")
    print(f"Changed rows: {len(changed_rows)}")
    print(f"Missing in source since last run: {len(removed_ids)}")
    print(f"Deleted in Grist: {deleted}")
    print(f"Inserted: {inserted}")
    print(f"Updated: {updated}")
    print(f"Target table: {config.grist_table_name}")
    print(f"State file: {config.sync_state_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
