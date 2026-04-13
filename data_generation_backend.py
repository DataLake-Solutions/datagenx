# Copyright (c) 2026 DataLake Solutions. All rights reserved.
#
# This source code and all related materials are proprietary to DataLake Solutions.
#
# You may not, without prior written permission from DataLake Solutions:
# - copy, modify, distribute, sublicense, sell, publish, or otherwise disclose this code;
# - share this code with third parties or post it to public repositories, forums, or websites;
# - use this code to create derivative works for external distribution or commercial exploitation.
#
# Unauthorized use, disclosure, or distribution is strictly prohibited.
import json
import csv
import os
import re
import subprocess
import sys
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent
SCHEMAS_ROOT = ROOT_DIR / "schemas"
SCHEMA_DB_PATH = ROOT_DIR / "schemas.json"
LEGACY_SCHEMA_DB_PATH = ROOT_DIR / "data" / "schemas.json"

STATUS_KEYS = [
    "schema_gen_status",
    "dg_code_gen_status",
    "dg_bulkdata_gen_status",
    "dg_sf_upload_status",
]


def _now_utc() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _safe_name(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", value).strip("_") or "schema"


def ensure_storage() -> None:
    SCHEMAS_ROOT.mkdir(parents=True, exist_ok=True)
    # one-time migration from old layout: data/schemas.json -> schemas.json
    if not SCHEMA_DB_PATH.exists() and LEGACY_SCHEMA_DB_PATH.exists():
        SCHEMA_DB_PATH.write_text(LEGACY_SCHEMA_DB_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    if not SCHEMA_DB_PATH.exists():
        seed = {
            "schemas": [
                {
                    "p_id": "1",
                    "org_id": "1001",
                    "org_name": "Test",
                    "schema_prompt": "",
                    "schema_list": [
                        {
                            "table_id": str(uuid.uuid4()),
                            "table_name": "PARTY",
                            "num_entries": 1000,
                            "ddl": "CREATE TABLE PARTY (\n  PARTY_ID STRING PRIMARY KEY,\n  FULL_NAME STRING,\n  EMAIL STRING,\n  CREATED_AT TIMESTAMP\n);",
                            "instructions": "Generate realistic customer names and emails.",
                            "columns_list": [],
                        }
                    ],
                    "schema_gen_status": "NEW",
                    "schema_gen_last_update": _now_utc(),
                    "schema_gen_log": "",
                    "dg_code_gen_status": "NEW",
                    "dg_code_gen_at": _now_utc(),
                    "dg_code_gen_log": "",
                    "dg_bulkdata_gen_status": "NEW",
                    "dg_bulkdata_gen_at": _now_utc(),
                    "dg_bulkdata_gen_log": "",
                    "dg_sf_upload_status": "NEW",
                    "dg_sf_upload_at": _now_utc(),
                    "dg_sf_upload_log": "",
                }
            ]
        }
        SCHEMA_DB_PATH.write_text(json.dumps(seed, indent=2), encoding="utf-8")


def _read_db() -> Dict[str, Any]:
    ensure_storage()
    return json.loads(SCHEMA_DB_PATH.read_text(encoding="utf-8-sig"))


def _write_db(db: Dict[str, Any]) -> None:
    SCHEMA_DB_PATH.write_text(json.dumps(db, indent=2), encoding="utf-8")


def _schema_dir(schema_name: str) -> Path:
    return SCHEMAS_ROOT / _safe_name(schema_name)


def _tables_generated_dir(schema_name: str) -> Path:
    return _schema_dir(schema_name) / "Tables_generated"


def _instructions_dir(schema_name: str) -> Path:
    return _schema_dir(schema_name) / "instructions"


def _code_generated_dir(schema_name: str) -> Path:
    return _schema_dir(schema_name) / "code_generated"


def _validation_dir(schema_name: str) -> Path:
    return _schema_dir(schema_name) / "validations"


def _datavalidtion_script_dir(schema_name: str) -> Path:
    # Keep folder name aligned to requested path: Schema/Datavalidtion/
    return _schema_dir(schema_name) / "Datavalidtion"


def _logs_dir(schema_name: str) -> Path:
    return _schema_dir(schema_name) / "logs"


def _make_run_logger(schema_name: str, org_id: str):
    logs_dir = _logs_dir(schema_name)
    logs_dir.mkdir(parents=True, exist_ok=True)
    latest_log = logs_dir / "latest.log"
    header = (
        f"[{_now_utc()}] [INFO] [org_id={org_id}] "
        "Starting fresh run log.\n"
    )
    latest_log.write_text(header, encoding="utf-8")

    def _log(level: str, message: str) -> None:
        line = f"[{_now_utc()}] [{level}] [org_id={org_id}] {message}\n"
        with latest_log.open("a", encoding="utf-8") as f:
            f.write(line)

    _log("INFO", f"Run started for schema '{schema_name}'")
    return latest_log, _log


def _write_instruction_files(schema: Dict[str, Any]) -> None:
    schema_name = schema.get("org_name", schema.get("org_id", "schema"))
    inst_dir = _instructions_dir(schema_name)
    inst_dir.mkdir(parents=True, exist_ok=True)

    table_list = schema.get("schema_list", [])
    ddls = {
        str(t.get("table_name", "")): {
            "table_id": t.get("table_id", ""),
            "ddl": t.get("ddl", ""),
        }
        for t in table_list
    }
    instructions = {
        str(t.get("table_name", "")): {
            "table_id": t.get("table_id", ""),
            "instructions": t.get("instructions", ""),
            "num_entries": t.get("num_entries", 0),
        }
        for t in table_list
    }
    schema_prompt_payload = {
        "schema_name": schema.get("org_name", ""),
        "org_id": schema.get("org_id", ""),
        "schema_prompt": schema.get("schema_prompt", ""),
    }

    (inst_dir / "ddl.json").write_text(json.dumps(ddls, indent=2), encoding="utf-8")
    (inst_dir / "instructions.json").write_text(json.dumps(instructions, indent=2), encoding="utf-8")
    (inst_dir / "schema_prompt.json").write_text(json.dumps(schema_prompt_payload, indent=2), encoding="utf-8")


def list_schemas() -> List[Dict[str, str]]:
    db = _read_db()
    return [
        {"value": item["org_id"], "label": item.get("org_name", item["org_id"])}
        for item in db.get("schemas", [])
    ]


def _find_schema(db: Dict[str, Any], org_id: str) -> Dict[str, Any]:
    for item in db.get("schemas", []):
        if str(item.get("org_id")) == str(org_id):
            return item
    raise KeyError(f"Schema with org_id '{org_id}' not found.")


def _find_schema_by_name(db: Dict[str, Any], schema_name: str) -> Dict[str, Any] | None:
    safe_name = _safe_name(schema_name)
    for item in db.get("schemas", []):
        if str(item.get("org_name", "")).lower() == safe_name.lower():
            return item
    return None


def _new_schema_record(schema_name: str, org_id: str | None = None) -> Dict[str, Any]:
    schema_org_id = org_id or str(int(datetime.utcnow().timestamp()))
    return {
        "p_id": "1",
        "org_id": schema_org_id,
        "org_name": _safe_name(schema_name),
        "schema_prompt": "",
        "schema_list": [],
        "schema_gen_status": "NEW",
        "schema_gen_last_update": _now_utc(),
        "schema_gen_log": "",
        "dg_code_gen_status": "NEW",
        "dg_code_gen_at": _now_utc(),
        "dg_code_gen_log": "",
        "dg_bulkdata_gen_status": "NEW",
        "dg_bulkdata_gen_at": _now_utc(),
        "dg_bulkdata_gen_log": "",
        "dg_sf_upload_status": "NEW",
        "dg_sf_upload_at": _now_utc(),
        "dg_sf_upload_log": "",
    }


def _normalize_table_data(table_data: Dict[str, Any]) -> Dict[str, Any]:
    table_id = str(table_data.get("table_id", "")).strip()
    normalized = {
        "table_id": table_id or str(uuid.uuid4()),
        "table_name": str(table_data.get("table_name", "")).strip(),
        "num_entries": int(table_data.get("num_entries", 0)),
        "ddl": str(table_data.get("ddl", "")).strip(),
        "instructions": str(table_data.get("instructions", "") or ""),
        "columns_list": table_data.get("columns_list", []),
    }
    if not normalized["table_name"] or not normalized["ddl"] or normalized["num_entries"] <= 0:
        raise ValueError("table_name, ddl and positive num_entries are required.")
    return normalized


def get_schema(org_id: str) -> Dict[str, Any]:
    db = _read_db()
    return _find_schema(db, org_id)


def create_schema(schema_name: str) -> Dict[str, Any]:
    db = _read_db()
    safe = _safe_name(schema_name)
    names = {str(s.get("org_name", "")).lower() for s in db.get("schemas", [])}
    if safe.lower() in names:
        raise ValueError("Schema name already exists.")

    new_schema = _new_schema_record(safe)
    org_id = str(new_schema["org_id"])
    db.setdefault("schemas", []).append(new_schema)
    _write_db(db)
    _tables_generated_dir(safe).mkdir(parents=True, exist_ok=True)
    _code_generated_dir(safe).mkdir(parents=True, exist_ok=True)
    _validation_dir(safe).mkdir(parents=True, exist_ok=True)
    _datavalidtion_script_dir(safe).mkdir(parents=True, exist_ok=True)
    _logs_dir(safe).mkdir(parents=True, exist_ok=True)
    _write_instruction_files(new_schema)
    return {"value": org_id, "label": safe}


def update_schema_prompt(org_id: str, schema_prompt: str) -> None:
    db = _read_db()
    schema = _find_schema(db, org_id)
    schema["schema_prompt"] = schema_prompt
    _write_db(db)
    _write_instruction_files(schema)


def upsert_schema_table(org_id: str, table_data: Dict[str, Any]) -> None:
    db = _read_db()
    schema = _find_schema(db, org_id)
    table_list = schema.setdefault("schema_list", [])

    normalized = _normalize_table_data(table_data)

    replaced = False
    for idx, existing in enumerate(table_list):
        if str(existing.get("table_id")) == normalized["table_id"]:
            table_list[idx] = normalized
            replaced = True
            break
    if not replaced:
        table_list.append(normalized)

    _write_db(db)
    _tables_generated_dir(schema.get("org_name", org_id)).mkdir(parents=True, exist_ok=True)
    _code_generated_dir(schema.get("org_name", org_id)).mkdir(parents=True, exist_ok=True)
    _validation_dir(schema.get("org_name", org_id)).mkdir(parents=True, exist_ok=True)
    _datavalidtion_script_dir(schema.get("org_name", org_id)).mkdir(parents=True, exist_ok=True)
    _logs_dir(schema.get("org_name", org_id)).mkdir(parents=True, exist_ok=True)
    _write_instruction_files(schema)


def remove_table(org_id: str, table_id: str) -> None:
    db = _read_db()
    schema = _find_schema(db, org_id)
    table_list = schema.setdefault("schema_list", [])
    schema["schema_list"] = [t for t in table_list if str(t.get("table_id")) != str(table_id)]
    _write_db(db)
    _write_instruction_files(schema)


def sync_schema_from_payload(payload: Dict[str, Any], replace_existing: bool = True) -> Dict[str, Any]:
    schema_name = str(payload.get("schema_name", "")).strip()
    if not schema_name:
        raise ValueError("schema_name is required.")

    raw_tables = payload.get("tables", [])
    if not isinstance(raw_tables, list) or not raw_tables:
        raise ValueError("tables must be a non-empty list.")

    schema_prompt = str(payload.get("schema_prompt", "") or "")
    db = _read_db()
    schema = _find_schema_by_name(db, schema_name)
    safe_schema_name = _safe_name(schema_name)

    if schema is None:
        schema = _new_schema_record(safe_schema_name)
        db.setdefault("schemas", []).append(schema)
    elif not replace_existing:
        raise ValueError(f"Schema '{safe_schema_name}' already exists.")

    schema["org_name"] = safe_schema_name
    schema["schema_prompt"] = schema_prompt
    schema["schema_list"] = [_normalize_table_data(table) for table in raw_tables]
    schema["schema_gen_status"] = "NEW"
    schema["schema_gen_log"] = ""
    schema["dg_code_gen_status"] = "NEW"
    schema["dg_code_gen_log"] = ""
    schema["dg_bulkdata_gen_status"] = "NEW"
    schema["dg_bulkdata_gen_log"] = ""
    schema["dg_sf_upload_status"] = "NEW"
    schema["dg_sf_upload_log"] = ""
    schema["schema_gen_last_update"] = _now_utc()
    schema.pop("last_error_trace", None)

    _write_db(db)
    _tables_generated_dir(safe_schema_name).mkdir(parents=True, exist_ok=True)
    _code_generated_dir(safe_schema_name).mkdir(parents=True, exist_ok=True)
    _validation_dir(safe_schema_name).mkdir(parents=True, exist_ok=True)
    _datavalidtion_script_dir(safe_schema_name).mkdir(parents=True, exist_ok=True)
    _logs_dir(safe_schema_name).mkdir(parents=True, exist_ok=True)
    _write_instruction_files(schema)
    return dict(schema)


def _extract_code_between_backticks(content: str) -> str:
    pattern = r"```(?:\w+)?\n(.*?)```"
    matches = re.findall(pattern, content, re.DOTALL)
    if not matches:
        raise ValueError("No code block found between triple backticks.")
    return matches[0].strip()


def _extract_missing_faker_method(stderr_text: str) -> str:
    m = re.search(r"has no attribute '([A-Za-z_][A-Za-z0-9_]*)'", stderr_text or "")
    return m.group(1) if m else ""


def _fallback_faker_method(missing_method: str) -> str:
    name = (missing_method or "").lower()
    fallback_map = {
        "county": "state",
        "province": "state",
        "region": "state",
        "postalcode": "zipcode",
        "postcode": "zipcode",
        "zip_code": "zipcode",
        "app_version": "pystr",
        "semver": "pystr",
        "version": "pystr",
    }
    if name in fallback_map:
        return fallback_map[name]
    if "date" in name:
        return "date"
    if "time" in name:
        return "date_time"
    if "email" in name:
        return "email"
    if "phone" in name or "mobile" in name:
        return "phone_number"
    if "uuid" in name or name.endswith("_id") or name == "id":
        return "uuid4"
    if "name" in name:
        return "name"
    if "country" in name:
        return "country"
    if "city" in name:
        return "city"
    if "state" in name or "county" in name or "region" in name or "province" in name:
        return "state"
    if "zip" in name or "postal" in name:
        return "zipcode"
    if "address" in name:
        return "street_address"
    if "ip" in name:
        return "ipv4"
    if "company" in name or "business" in name or "employer" in name:
        return "company"
    if "job" in name or "occupation" in name:
        return "job"
    return "pystr"


def _patch_generated_faker_method(script_path: Path, missing_method: str, replacement_method: str) -> bool:
    src = script_path.read_text(encoding="utf-8")
    pattern = rf"\bfake\.{re.escape(missing_method)}\s*\("
    if not re.search(pattern, src):
        return False
    patched = re.sub(pattern, f"fake.{replacement_method}(", src)
    if patched == src:
        return False
    script_path.write_text(patched, encoding="utf-8")
    return True


def _needs_faker_date_parse_patch(stderr_text: str) -> bool:
    s = (stderr_text or "").lower()
    return "can't parse date string" in s and "faker.providers.date_time.parseerror" in s


def _patch_generated_faker_date_literals(script_path: Path) -> bool:
    src = script_path.read_text(encoding="utf-8")

    pattern = re.compile(
        r"(\b(?:start_date|end_date)\s*=\s*)['\"](\d{4})-(\d{2})-(\d{2})['\"]"
    )

    def _repl(match: re.Match[str]) -> str:
        prefix = match.group(1)
        y = int(match.group(2))
        m = int(match.group(3))
        d = int(match.group(4))
        return f"{prefix}datetime.strptime('{y:04d}-{m:02d}-{d:02d}', '%Y-%m-%d').date()"

    patched = pattern.sub(_repl, src)
    if patched == src:
        return False

    has_datetime_symbol = bool(re.search(r"\bdatetime\b", patched))
    has_datetime_import = bool(re.search(r"^\s*from\s+datetime\s+import\s+.*\bdatetime\b", patched, flags=re.MULTILINE))
    has_datetime_module_import = bool(re.search(r"^\s*import\s+datetime(\s+as\s+\w+)?\s*$", patched, flags=re.MULTILINE))
    if has_datetime_symbol and not (has_datetime_import or has_datetime_module_import):
        m = re.search(r"^\s*from\s+datetime\s+import\s+([^\n]+)\s*$", patched, flags=re.MULTILINE)
        if m:
            imports = [x.strip() for x in m.group(1).split(",") if x.strip()]
            if "datetime" not in imports:
                imports.append("datetime")
                new_line = "from datetime import " + ", ".join(imports)
                patched = patched[:m.start()] + new_line + patched[m.end():]
        else:
            patched = "from datetime import datetime\n" + patched

    script_path.write_text(patched, encoding="utf-8")
    return True


def _patch_generated_temporal_safety(script_path: Path) -> bool:
    src = script_path.read_text(encoding="utf-8")
    patched = src

    patched = re.sub(r"\bfake\.date_between\(", "_dls_safe_date_between(fake, ", patched)
    patched = re.sub(r"\bfake\.date_time_between\(", "_dls_safe_date_time_between(fake, ", patched)
    patched = re.sub(r"\bfake\.date_between_dates\(", "_dls_safe_date_between_dates(fake, ", patched)

    marker = "# __DLS_TEMPORAL_SAFE_PATCH__"
    if marker not in patched and patched != src:
        helper_block = (
            "\n\n# __DLS_TEMPORAL_SAFE_PATCH__\n"
            "from datetime import date, datetime, time\n"
            "\n"
            "def _dls_to_date(v):\n"
            "    if isinstance(v, date) and not isinstance(v, datetime):\n"
            "        return v\n"
            "    if isinstance(v, datetime):\n"
            "        return v.date()\n"
            "    if isinstance(v, str):\n"
            "        s = v.strip()\n"
            "        if not s:\n"
            "            return date.today()\n"
            "        s = s.replace('Z', '+00:00')\n"
            "        try:\n"
            "            return date.fromisoformat(s[:10])\n"
            "        except Exception:\n"
            "            try:\n"
            "                return datetime.fromisoformat(s).date()\n"
            "            except Exception:\n"
            "                return date.today()\n"
            "    return date.today()\n"
            "\n"
            "def _dls_to_datetime(v):\n"
            "    if isinstance(v, datetime):\n"
            "        return v\n"
            "    if isinstance(v, date):\n"
            "        return datetime.combine(v, time.min)\n"
            "    if isinstance(v, str):\n"
            "        s = v.strip()\n"
            "        if not s:\n"
            "            return datetime.now()\n"
            "        s = s.replace('Z', '+00:00')\n"
            "        try:\n"
            "            return datetime.fromisoformat(s)\n"
            "        except Exception:\n"
            "            try:\n"
            "                return datetime.combine(date.fromisoformat(s[:10]), time.min)\n"
            "            except Exception:\n"
            "                return datetime.now()\n"
            "    return datetime.now()\n"
            "\n"
            "def _dls_safe_date_between(fake_obj, *args, **kwargs):\n"
            "    start = _dls_to_date(kwargs.get('start_date', date(1970, 1, 1)))\n"
            "    end = _dls_to_date(kwargs.get('end_date', date.today()))\n"
            "    if start > end:\n"
            "        start, end = end, start\n"
            "    kwargs['start_date'] = start\n"
            "    kwargs['end_date'] = end\n"
            "    return getattr(fake_obj, 'date_between')(*args, **kwargs)\n"
            "\n"
            "def _dls_safe_date_time_between(fake_obj, *args, **kwargs):\n"
            "    start = _dls_to_datetime(kwargs.get('start_date', datetime(1970, 1, 1)))\n"
            "    end = _dls_to_datetime(kwargs.get('end_date', datetime.now()))\n"
            "    if start > end:\n"
            "        start, end = end, start\n"
            "    kwargs['start_date'] = start\n"
            "    kwargs['end_date'] = end\n"
            "    return getattr(fake_obj, 'date_time_between')(*args, **kwargs)\n"
            "\n"
            "def _dls_safe_date_between_dates(fake_obj, *args, **kwargs):\n"
            "    date_start = kwargs.get('date_start', kwargs.get('start_date', date(1970, 1, 1)))\n"
            "    date_end = kwargs.get('date_end', kwargs.get('end_date', date.today()))\n"
            "    start = _dls_to_date(date_start)\n"
            "    end = _dls_to_date(date_end)\n"
            "    if start > end:\n"
            "        start, end = end, start\n"
            "    kwargs['date_start'] = start\n"
            "    kwargs['date_end'] = end\n"
            "    kwargs.pop('start_date', None)\n"
            "    kwargs.pop('end_date', None)\n"
            "    return getattr(fake_obj, 'date_between_dates')(*args, **kwargs)\n"
        )

        fake_anchor = re.search(r"^\s*fake\s*=\s*Faker\(\)\s*$", patched, flags=re.MULTILINE)
        if fake_anchor:
            insert_at = fake_anchor.end()
            patched = patched[:insert_at] + helper_block + patched[insert_at:]
        else:
            patched = helper_block + patched

    if patched == src:
        return False
    script_path.write_text(patched, encoding="utf-8")
    return True


def _needs_negative_randrange_patch(stderr_text: str) -> bool:
    s = (stderr_text or "").lower()
    return "empty range for randrange" in s or "valueerror: empty range" in s


def _patch_generated_random_date_windows(script_path: Path) -> bool:
    src = script_path.read_text(encoding="utf-8")
    patched = src

    patched = re.sub(
        r"random\.randint\(\s*0\s*,\s*int\(\(end\s*-\s*start\)\.total_seconds\(\)\)\s*\)",
        "random.randint(0, max(0, int((end - start).total_seconds())))",
        patched,
    )
    patched = re.sub(
        r"random\.randint\(\s*0\s*,\s*\(end\s*-\s*start\)\.days\s*\)",
        "random.randint(0, max(0, (end - start).days))",
        patched,
    )

    if patched == src:
        return False
    script_path.write_text(patched, encoding="utf-8")
    return True


def _needs_faker_random_element_weights_patch(stderr_text: str) -> bool:
    s = (stderr_text or "").lower()
    return "random_element() got an unexpected keyword argument 'weights'" in s


def _patch_faker_random_element_weights(script_path: Path) -> bool:
    src = script_path.read_text(encoding="utf-8")
    patched = src

    pattern = re.compile(
        r"fake\.random_element\(\s*([^\n,][^\n]*?)\s*,\s*weights\s*=\s*([^\n\)]*?)\s*\)"
    )
    patched = pattern.sub(r"random.choices(\1, weights=\2)[0]", patched)

    if patched == src:
        return False
    script_path.write_text(patched, encoding="utf-8")
    return True


def _needs_mixed_date_datetime_patch(stderr_text: str) -> bool:
    s = (stderr_text or "").lower()
    return (
        "can't compare datetime.datetime to datetime.date" in s
        or "not supported between instances of 'datetime.date' and 'str'" in s
        or "not supported between instances of 'str' and 'datetime.date'" in s
        or "not supported between instances of 'datetime.datetime' and 'str'" in s
        or "not supported between instances of 'str' and 'datetime.datetime'" in s
    )


def _patch_mixed_date_datetime_expressions(script_path: Path) -> bool:
    src = script_path.read_text(encoding="utf-8")
    patched = src

    patched = re.sub(
        r"min\(\s*([^,\n]+?)\s*,\s*datetime\.now\(\)\s*\)",
        r"min(_dls_to_datetime(\1), datetime.now())",
        patched,
    )
    patched = re.sub(
        r"max\(\s*([^,\n]+?)\s*,\s*datetime\.now\(\)\s*\)",
        r"max(_dls_to_datetime(\1), datetime.now())",
        patched,
    )
    patched = re.sub(
        r"min\(\s*datetime\.now\(\)\s*,\s*([^,\n]+?)\s*\)",
        r"min(datetime.now(), _dls_to_datetime(\1))",
        patched,
    )
    patched = re.sub(
        r"max\(\s*datetime\.now\(\)\s*,\s*([^,\n]+?)\s*\)",
        r"max(datetime.now(), _dls_to_datetime(\1))",
        patched,
    )
    patched = re.sub(
        r"min\(\s*([^,\n]+?)\s*,\s*date\.today\(\)\s*\)",
        r"min(_dls_to_date(\1), date.today())",
        patched,
    )
    patched = re.sub(
        r"max\(\s*([^,\n]+?)\s*,\s*date\.today\(\)\s*\)",
        r"max(_dls_to_date(\1), date.today())",
        patched,
    )
    patched = re.sub(
        r"min\(\s*date\.today\(\)\s*,\s*([^,\n]+?)\s*\)",
        r"min(date.today(), _dls_to_date(\1))",
        patched,
    )
    patched = re.sub(
        r"max\(\s*date\.today\(\)\s*,\s*([^,\n]+?)\s*\)",
        r"max(date.today(), _dls_to_date(\1))",
        patched,
    )

    helper_marker = "def _dls_to_datetime(v):"
    if helper_marker not in patched and "_dls_to_datetime(" in patched:
        helper_block = (
            "\n\ndef _dls_to_datetime(v):\n"
            "    if isinstance(v, datetime):\n"
            "        return v\n"
            "    if isinstance(v, date):\n"
            "        return datetime.combine(v, datetime.min.time())\n"
            "    if isinstance(v, str):\n"
            "        s = v.strip()\n"
            "        if not s:\n"
            "            return datetime.min\n"
            "        s = s.replace('Z', '+00:00')\n"
            "        try:\n"
            "            return datetime.fromisoformat(s)\n"
            "        except Exception:\n"
            "            try:\n"
            "                return datetime.combine(date.fromisoformat(s[:10]), datetime.min.time())\n"
            "            except Exception:\n"
            "                return datetime.min\n"
            "    return v\n"
            "\n"
            "def _dls_to_date(v):\n"
            "    if isinstance(v, date) and not isinstance(v, datetime):\n"
            "        return v\n"
            "    if isinstance(v, datetime):\n"
            "        return v.date()\n"
            "    if isinstance(v, str):\n"
            "        s = v.strip()\n"
            "        if not s:\n"
            "            return date.min\n"
            "        s = s.replace('Z', '+00:00')\n"
            "        try:\n"
            "            return date.fromisoformat(s[:10])\n"
            "        except Exception:\n"
            "            try:\n"
            "                return datetime.fromisoformat(s).date()\n"
            "            except Exception:\n"
            "                return date.min\n"
            "    return v\n"
        )
        import_anchor = re.search(r"^\s*from\s+datetime\s+import\s+[^\n]+$", patched, flags=re.MULTILINE)
        if import_anchor:
            insert_at = import_anchor.end()
            patched = patched[:insert_at] + helper_block + patched[insert_at:]
        else:
            patched = helper_block + patched

    if patched == src:
        return False
    script_path.write_text(patched, encoding="utf-8")
    return True


def _apply_generated_safety_patches(script_path: Path) -> List[str]:
    applied: List[str] = []
    if _patch_generated_temporal_safety(script_path):
        applied.append("temporal_safety")
    if _patch_generated_faker_date_literals(script_path):
        applied.append("faker_date_literals")
    if _patch_generated_random_date_windows(script_path):
        applied.append("random_date_windows")
    if _patch_faker_random_element_weights(script_path):
        applied.append("faker_random_element_weights")
    if _patch_mixed_date_datetime_expressions(script_path):
        applied.append("mixed_date_datetime")
    return applied


def _needs_validation_json_patch(stderr_text: str) -> bool:
    s = (stderr_text or "").lower()
    return "not json serializable" in s


def _patch_validation_json_serialization(script_path: Path) -> bool:
    src = script_path.read_text(encoding="utf-8")
    marker = "__DLS_JSON_SAFE_PATCH__"
    if marker in src:
        return False

    patch_block = (
        "\n# __DLS_JSON_SAFE_PATCH__\n"
        "_orig_json_dump = json.dump\n"
        "def _dls_json_default(obj):\n"
        "    if hasattr(obj, 'item'):\n"
        "        try:\n"
        "            return obj.item()\n"
        "        except Exception:\n"
        "            pass\n"
        "    if isinstance(obj, (set, tuple)):\n"
        "        return list(obj)\n"
        "    return str(obj)\n"
        "def _dls_safe_json_dump(obj, fp, *args, **kwargs):\n"
        "    kwargs.setdefault('default', _dls_json_default)\n"
        "    return _orig_json_dump(obj, fp, *args, **kwargs)\n"
        "json.dump = _dls_safe_json_dump\n"
    )

    if "import json" in src:
        src = src.replace("import json", "import json" + patch_block, 1)
    else:
        src = patch_block + src

    script_path.write_text(src, encoding="utf-8")
    return True


def _normalize_table_name(value: str) -> str:
    v = str(value or "").strip()
    if v.lower().endswith(".csv"):
        v = v[:-4]
    return v.upper()


def _write_fallback_validation_report(
    schema_list: List[Dict[str, Any]],
    csv_dir: Path,
    validation_report_path: Path,
    reason: str,
) -> Dict[str, Any]:
    tables = []
    for table in schema_list:
        tname = str(table.get("table_name", ""))
        csv_path = csv_dir / f"{tname}.csv"
        checks: List[Dict[str, Any]] = []

        exists = csv_path.exists()
        checks.append({
            "name": "CSV file exists",
            "passed": exists,
            "details": str(csv_path),
        })

        row_count = 0
        header_ok = False
        if exists:
            try:
                with csv_path.open("r", encoding="utf-8", newline="") as f:
                    reader = csv.reader(f)
                    header = next(reader, [])
                    header_ok = isinstance(header, list) and len(header) > 0
                    for _ in reader:
                        row_count += 1
            except Exception:
                header_ok = False

        checks.append({
            "name": "CSV has header",
            "passed": header_ok,
            "details": f"header_ok={header_ok}",
        })
        checks.append({
            "name": "CSV has rows",
            "passed": row_count > 0,
            "details": f"rows={row_count}",
        })

        tables.append({
            "table_name": tname,
            "checks": checks,
        })

    report = {
        "summary": f"Fallback validation used: {reason}",
        "tables": tables,
    }
    validation_report_path.parent.mkdir(parents=True, exist_ok=True)
    validation_report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def _normalize_validation_report_payload(report: Any) -> Dict[str, Any]:
    """
    Accept common LLM-produced shapes and normalize to:
    {"summary": str, "tables": List[Dict[str, Any]]}
    """
    if isinstance(report, dict):
        tables = report.get("tables", [])
        if not isinstance(tables, list):
            tables = []
        summary = str(report.get("summary", "")).strip()
        return {"summary": summary, "tables": tables}

    if isinstance(report, list):
        if not report:
            return {"summary": "", "tables": []}

        # Case A: one-item wrapper list: [ {summary, tables:[...]} ]
        if len(report) == 1 and isinstance(report[0], dict) and "tables" in report[0]:
            return _normalize_validation_report_payload(report[0])

        # Case B: list of table objects directly: [ {table_name, checks}, ... ]
        if all(isinstance(item, dict) and "table_name" in item for item in report):
            return {"summary": "", "tables": report}

    raise ValueError(
        "validation_report.json must be either an object "
        "with {summary,tables} or a list containing that object/table entries."
    )


def _update_status(schema: Dict[str, Any], **updates: Any) -> None:
    schema.update(updates)
    schema["schema_gen_last_update"] = _now_utc()


def _build_prompt(schema_name: str, schema_prompt: str, schema_list: List[Dict[str, Any]]) -> str:
    prompt = (
        "You are a Python code generation assistant. Generate one runnable Python file.\n"
        "Goal: create fake data rows and save CSVs for each table.\n"
        "Use Faker. Keep foreign key consistency where needed.\n"
        "Use safe filesystem writing and write files to output dir from env var OUTPUT_DIR.\n"
        f"Schema Name: {schema_name}\n"
    )
    if schema_prompt.strip():
        prompt += f"Schema-level instructions:\n{schema_prompt}\n"

    prompt += "Table definitions:\n"
    for table in schema_list:
        prompt += f"{table['ddl']}\n"
        prompt += f"Rows required: exactly {table['num_entries']}\n"
        if table.get("instructions"):
            prompt += f"Instructions: {table['instructions']}\n"
        prompt += "\n"

    prompt += (
        "Rules:\n"
        "- Create one generator function per table.\n"
        "- Strictly honor each column datatype from DDL when generating values.\n"
        "- Always write file as <table_name>.csv into OUTPUT_DIR.\n"
        "- Use csv.DictWriter with utf-8.\n"
        "- Faker compatibility is strict: use only common Faker methods (name, first_name, last_name, email, phone_number, "
        "address, city, state, country, zipcode, uuid4, date, date_time, date_of_birth, random_int, random_number, "
        "random_element, bothify, numerify, lexify, pystr, ipv4, company, job).\n"
        "- Do NOT call fake.random_element(..., weights=...). If weighted choice is needed, use random.choices(options, weights=weights)[0].\n"
        "- Do NOT use unsupported/custom Faker calls like fake.version(), fake.semver(), fake.app_version(), or provider-specific methods.\n"
        "- For application version strings, generate with Python/random logic (e.g., f\"{random.randint(1,9)}.{random.randint(0,9)}.{random.randint(0,99)}\") instead of Faker.\n"
        "- For fake.date_between/date_time_between/date_between_dates, do NOT pass hard-coded YYYY-MM-DD strings; pass Python date/datetime objects or Faker relative tokens.\n"
        "- For date arithmetic, convert Faker date strings to date/datetime objects before timedelta math.\n"
        "- Any random date/datetime window must guard for reversed ranges (if end < start, clamp duration to 0 before randint).\n"
        "- Do not compare `date` to `datetime` directly (for min/max or range bounds). Convert date to datetime first.\n"
        "- Add defensive code: avoid zero-division and ensure all referenced variables are defined.\n"
        "- Script must run end-to-end with no placeholders and no TODO comments.\n"
        "- Include all imports in generated file.\n"
        "- Return code only in triple backticks.\n"
    )
    return prompt


def _openai_generate_code(schema_name: str, schema_prompt: str, schema_list: List[Dict[str, Any]]) -> str:
    load_dotenv(ROOT_DIR / ".env", override=False)
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing in .env")

    prompt = _build_prompt(schema_name, schema_prompt, schema_list)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a Python code generation assistant."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.3,
    }

    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=120,
    )
    response.raise_for_status()
    raw = response.json()["choices"][0]["message"]["content"].strip()
    return _extract_code_between_backticks(raw)


def _build_validation_prompt(schema_name: str, schema_prompt: str, schema_list: List[Dict[str, Any]]) -> str:
    prompt = (
        "You are a Python data validation assistant.\n"
        "Generate one runnable Python file that validates generated CSV data using pandas.\n"
        "The script must read CSV files from env var CSV_DIR and write JSON report to env var VALIDATION_REPORT_PATH.\n"
        "The script must never modify CSV files.\n"
        f"Schema Name: {schema_name}\n"
    )
    if schema_prompt.strip():
        prompt += f"Schema-level instructions:\n{schema_prompt}\n"

    prompt += "Tables:\n"
    for table in schema_list:
        prompt += f"- Table: {table['table_name']}\nDDL:\n{table['ddl']}\n"
        prompt += f"Instructions:\n{table.get('instructions', '')}\n\n"

    prompt += (
        "Validation requirements:\n"
        "- At least 2 meaningful checks per table.\n"
        "- Include referential integrity checks where FK-like fields exist.\n"
        "- Include distribution/constraint checks (ranges, uniqueness, null %, domain values).\n"
        "- Handle missing files or columns gracefully and mark checks failed instead of crashing.\n\n"
        "Output report JSON schema:\n"
        "{\n"
        "  \"summary\": \"...\",\n"
        "  \"tables\": [\n"
        "    {\n"
        "      \"table_name\": \"...\",\n"
        "      \"checks\": [\n"
        "        {\"name\": \"...\", \"passed\": true, \"details\": \"...\"}\n"
        "      ]\n"
        "    }\n"
        "  ]\n"
        "}\n"
        "- Always write this report to VALIDATION_REPORT_PATH as JSON.\n"
        "- Return code only inside triple backticks.\n"
    )
    return prompt


def _openai_generate_validation_code(schema_name: str, schema_prompt: str, schema_list: List[Dict[str, Any]]) -> str:
    load_dotenv(ROOT_DIR / ".env", override=False)
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing in .env")

    prompt = _build_validation_prompt(schema_name, schema_prompt, schema_list)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a Python data validation assistant."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }

    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=120,
    )
    response.raise_for_status()
    raw = response.json()["choices"][0]["message"]["content"].strip()
    return _extract_code_between_backticks(raw)


def _generate_simple_validation_code(schema_name: str, schema_list: List[Dict[str, Any]]) -> str:
    tables_meta = [
        {
            "table_name": str(t.get("table_name", "")),
            "expected_rows": int(t.get("num_entries", 0) or 0),
            "instructions": str(t.get("instructions", "") or ""),
        }
        for t in schema_list
    ]

    return f"""import csv
import json
import os
import re

CSV_DIR = os.getenv("CSV_DIR", ".")
VALIDATION_REPORT_PATH = os.getenv("VALIDATION_REPORT_PATH", "validation_report.json")
TABLES = {json.dumps(tables_meta, indent=2)}


def _to_float(value):
    try:
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _ratio(numerator: int, denominator: int) -> float:
    return (float(numerator) / float(denominator)) if denominator else 0.0


def _parse_distribution_rules(instructions: str):
    # Supported syntax inside instructions (single line):
    # distribution: gender=M:60,F:40; account_status=Active:80,Inactive:20
    rules = []
    if not instructions:
        return rules

    m = re.search(r"distribution\s*:\s*(.+)", instructions, flags=re.IGNORECASE)
    if not m:
        return rules

    chunk = m.group(1).strip()
    segments = [s.strip() for s in chunk.split(';') if s.strip()]
    for seg in segments:
        if '=' not in seg:
            continue
        col, rhs = seg.split('=', 1)
        col = col.strip()
        target = {{}}
        parts = [p.strip() for p in rhs.split(',') if p.strip()]
        for part in parts:
            if ':' not in part:
                continue
            val, pct = part.rsplit(':', 1)
            val = val.strip()
            pct_clean = pct.strip().replace('%', '')
            try:
                target[val] = float(pct_clean)
            except Exception:
                continue
        if target:
            rules.append({{"column": col, "target": target}})
    return rules


def _phone_expectation_from_instructions(instructions: str) -> str:
    s = (instructions or "").lower()
    if "e.164" in s or "e164" in s:
        return "e164"
    return "default"


def _check_table(table: dict) -> dict:
    table_name = str(table.get("table_name", ""))
    expected_rows = int(table.get("expected_rows", 0) or 0)
    instructions = str(table.get("instructions", "") or "")
    csv_path = os.path.join(CSV_DIR, f"{{table_name}}.csv")

    checks = []
    exists = os.path.exists(csv_path)
    checks.append({{
        "name": "CSV file exists",
        "passed": bool(exists),
        "details": csv_path,
    }})

    header = []
    rows = []
    if exists:
        try:
            with open(csv_path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                header = list(reader.fieldnames or [])
                for row in reader:
                    rows.append(row)
        except Exception as exc:
            checks.append({{
                "name": "CSV readable",
                "passed": False,
                "details": str(exc),
            }})
            return {{"table_name": table_name, "checks": checks}}

    row_count = len(rows)
    checks.append({{
        "name": "CSV has header",
        "passed": bool(header),
        "details": f"header_columns={{len(header)}}",
    }})
    checks.append({{
        "name": "CSV has rows",
        "passed": bool(row_count > 0),
        "details": f"rows={{row_count}}",
    }})

    if expected_rows > 0:
        checks.append({{
            "name": "Row count matches expected num_entries",
            "passed": bool(row_count == expected_rows),
            "details": f"expected={{expected_rows}}, actual={{row_count}}",
        }})

    # Phone format checks (instruction-aware)
    if "phone_number" in header and row_count > 0:
        mode = _phone_expectation_from_instructions(instructions)
        valid = 0
        for r in rows:
            raw = str(r.get("phone_number", "")).strip()
            if mode == "e164":
                ok = bool(re.match(r"^\+[1-9]\d{7,14}$", raw))
            else:
                digits = re.sub(r"\D", "", raw)
                ok = len(digits) >= 10
            if ok:
                valid += 1
        rate = _ratio(valid, row_count)
        checks.append({{
            "name": "Phone number format",
            "passed": bool(rate >= 0.95),
            "details": f"valid={{valid}}/{{row_count}} ({{rate:.1%}}), mode={{mode}}",
        }})

    # Email format checks
    if "email_address" in header and row_count > 0:
        pattern = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        valid = 0
        non_empty = 0
        for r in rows:
            v = str(r.get("email_address", "")).strip()
            if not v:
                continue
            non_empty += 1
            if pattern.match(v):
                valid += 1
        passed = (non_empty == 0) or (valid == non_empty)
        checks.append({{
            "name": "Email format",
            "passed": bool(passed),
            "details": f"valid_non_empty={{valid}}/{{non_empty}}",
        }})

    # Uniqueness checks for key-like columns (if present)
    for key_col in ["customer_id", "government_id", "phone_number"]:
        if key_col in header and row_count > 0:
            values = [str(r.get(key_col, "")).strip() for r in rows]
            non_empty = [v for v in values if v]
            unique_count = len(set(non_empty))
            passed = (len(non_empty) == row_count) and (unique_count == row_count)
            checks.append({{
                "name": f"Unique/non-null {{key_col}}",
                "passed": bool(passed),
                "details": f"non_empty={{len(non_empty)}}, unique={{unique_count}}, rows={{row_count}}",
            }})

    # Numeric consistency/range checks (if present)
    if "credit_score" in header and row_count > 0:
        vals = [_to_float(r.get("credit_score")) for r in rows]
        finite = [v for v in vals if v is not None]
        in_range = [v for v in finite if 300 <= v <= 900]
        passed = bool(finite) and (len(in_range) == len(finite))
        checks.append({{
            "name": "Credit score range (300-900)",
            "passed": bool(passed),
            "details": f"in_range={{len(in_range)}}/{{len(finite)}}",
        }})

    if "annual_income" in header and "monthly_income" in header and row_count > 0:
        comparable = 0
        matched = 0
        for r in rows:
            a = _to_float(r.get("annual_income"))
            m = _to_float(r.get("monthly_income"))
            if a is None or m is None:
                continue
            comparable += 1
            if abs(a - (m * 12.0)) <= 0.02:
                matched += 1
        passed = (comparable == 0) or (matched == comparable)
        checks.append({{
            "name": "Annual income = monthly_income * 12",
            "passed": bool(passed),
            "details": f"matched={{matched}}/{{comparable}}",
        }})

    # Baseline distribution sanity
    for dist_col in ["gender", "lifecycle_stage", "account_status"]:
        if dist_col in header and row_count > 1:
            vals = [str(r.get(dist_col, "")).strip() for r in rows if str(r.get(dist_col, "")).strip()]
            distinct = len(set(vals))
            checks.append({{
                "name": f"Distribution sanity for {{dist_col}}",
                "passed": bool(distinct >= 2),
                "details": f"distinct_values={{distinct}}",
            }})

    # Instruction-driven distribution checks
    rules = _parse_distribution_rules(instructions)
    for rule in rules:
        col = str(rule.get("column", ""))
        target = rule.get("target", {{}})
        if not col or col not in header or not target or row_count == 0:
            continue

        values = [str(r.get(col, "")).strip() for r in rows if str(r.get(col, "")).strip()]
        total = len(values)
        if total == 0:
            checks.append({{
                "name": f"Distribution rule for {{col}}",
                "passed": False,
                "details": "no non-empty values",
            }})
            continue

        tolerance = 10.0
        parts = []
        ok_all = True
        for expected_val, expected_pct in target.items():
            actual_count = sum(1 for v in values if v == expected_val)
            actual_pct = (100.0 * actual_count) / total
            delta = abs(actual_pct - float(expected_pct))
            if delta > tolerance:
                ok_all = False
            parts.append(f"{{expected_val}} actual={{actual_pct:.1f}}% target={{float(expected_pct):.1f}}%")

        checks.append({{
            "name": f"Distribution rule for {{col}}",
            "passed": bool(ok_all),
            "details": "; ".join(parts) + f"; tolerance=+/-{{tolerance:.1f}}%",
        }})

    return {{"table_name": table_name, "checks": checks}}


def main() -> None:
    report = {{"summary": "", "tables": []}}
    total_checks = 0
    passed_checks = 0

    for table in TABLES:
        table_report = _check_table(table)
        checks = table_report.get("checks", [])
        total_checks += len(checks)
        passed_checks += sum(1 for c in checks if bool(c.get("passed", False)))
        report["tables"].append(table_report)

    report["summary"] = (
        f"Validation for schema '{schema_name}': {{passed_checks}}/{{total_checks}} checks passed."
    )

    out_dir = os.path.dirname(VALIDATION_REPORT_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(VALIDATION_REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)


if __name__ == "__main__":
    main()
"""



def generate_schema_data(org_id: str) -> Tuple[List[Path], Path]:
    db = _read_db()
    schema = _find_schema(db, org_id)
    schema_name = _safe_name(schema.get("org_name", org_id))
    schema_list = schema.get("schema_list", [])
    run_log_path, run_log = _make_run_logger(schema_name, str(org_id))
    schema["last_run_log_path"] = str(run_log_path)

    if not schema_list:
        run_log("ERROR", "No tables found in selected schema.")
        raise ValueError("No tables found in selected schema.")

    for key in STATUS_KEYS:
        if key.endswith("_status"):
            schema[key] = "NEW"

    _update_status(
        schema,
        schema_gen_status="NEW",
        schema_gen_log="",
        dg_code_gen_status="INPROGRESS",
        dg_code_gen_log="Generating Python data script.",
        dg_code_gen_at=_now_utc(),
        dg_bulkdata_gen_status="NEW",
        dg_bulkdata_gen_log="",
        dg_sf_upload_status="NEW",
        dg_sf_upload_log="",
    )
    _write_db(db)
    run_log("INFO", f"Initialized statuses. Table count={len(schema_list)}")

    schema_output_dir = _tables_generated_dir(schema_name)
    schema_output_dir.mkdir(parents=True, exist_ok=True)
    schema_code_dir = _code_generated_dir(schema_name)
    schema_code_dir.mkdir(parents=True, exist_ok=True)
    schema_validation_dir = _validation_dir(schema_name)
    schema_validation_dir.mkdir(parents=True, exist_ok=True)
    schema_datavalidtion_script_dir = _datavalidtion_script_dir(schema_name)
    schema_datavalidtion_script_dir.mkdir(parents=True, exist_ok=True)
    run_log("INFO", f"Output dir: {schema_output_dir}")
    run_log("INFO", f"Code dir: {schema_code_dir}")
    run_log("INFO", f"Validation dir: {schema_validation_dir}")

    try:
        run_log("INFO", "Starting Step 1: LLM code generation.")
        code = _openai_generate_code(schema_name, schema.get("schema_prompt", ""), schema_list)
        script_path = schema_code_dir / "generated_code.py"
        script_path.write_text(code, encoding="utf-8")
        run_log("INFO", f"Generated code written: {script_path} (chars={len(code)})")
        safety_patches = _apply_generated_safety_patches(script_path)
        if safety_patches:
            run_log("WARN", f"Applied pre-run safety patches: {', '.join(safety_patches)}")

        _update_status(
            schema,
            dg_code_gen_status="DONE",
            dg_code_gen_log="Code generated successfully.",
            dg_code_gen_at=_now_utc(),
            dg_bulkdata_gen_status="INPROGRESS",
            dg_bulkdata_gen_log="Running generated script.",
            dg_bulkdata_gen_at=_now_utc(),
        )
        _write_db(db)
        run_log("INFO", "Step 1 done. Starting Step 2: run generated script.")

        env = os.environ.copy()
        env["OUTPUT_DIR"] = str(schema_output_dir)
        run_log("INFO", f"Executing script: {sys.executable} {script_path}")
        run = None
        for attempt in range(1, 4):
            try:
                run = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    check=True,
                    env=env,
                    cwd=str(schema_output_dir),
                )
                break
            except subprocess.CalledProcessError as cpe:
                std_err = (cpe.stderr or "").strip()
                std_out = (cpe.stdout or "").strip()
                detail = std_err or std_out or str(cpe)
                run_log("ERROR", f"Generated script failed on attempt {attempt} with return code {cpe.returncode}")
                if std_out:
                    run_log("ERROR", f"STDOUT tail: {std_out[-1200:]}")
                if std_err:
                    run_log("ERROR", f"STDERR tail: {std_err[-1200:]}")

                missing = _extract_missing_faker_method(std_err)
                if missing:
                    fallback = _fallback_faker_method(missing)
                    patched = _patch_generated_faker_method(script_path, missing, fallback)
                    if patched:
                        run_log(
                            "WARN",
                            f"Auto-patched unsupported Faker method fake.{missing}() -> fake.{fallback}() and retrying."
                        )
                        continue

                if _needs_faker_date_parse_patch(std_err):
                    patched_dates = _patch_generated_faker_date_literals(script_path)
                    if patched_dates:
                        run_log(
                            "WARN",
                            "Auto-patched Faker date string literals to datetime/date objects and retrying.",
                        )
                        continue

                if _needs_negative_randrange_patch(std_err):
                    patched_windows = _patch_generated_random_date_windows(script_path)
                    if patched_windows:
                        run_log(
                            "WARN",
                            "Auto-patched negative random date window (randrange) and retrying.",
                        )
                        continue

                if _needs_faker_random_element_weights_patch(std_err):
                    patched_weight_choice = _patch_faker_random_element_weights(script_path)
                    if patched_weight_choice:
                        run_log(
                            "WARN",
                            "Auto-patched fake.random_element(..., weights=...) to random.choices(...)[0] and retrying.",
                        )
                        continue

                if _needs_mixed_date_datetime_patch(std_err):
                    patched_types = _patch_mixed_date_datetime_expressions(script_path)
                    if patched_types:
                        run_log(
                            "WARN",
                            "Auto-patched mixed date/datetime comparisons and retrying.",
                        )
                        continue

                raise RuntimeError(
                    "Generated data script failed. "
                    f"File: {script_path}. "
                    f"Details: {detail[-1500:]}"
                ) from cpe
        if run is None:
            raise RuntimeError("Generated data script failed after auto-retry attempts.")
        run_log("INFO", "Generated script execution completed successfully.")
        if run.stdout:
            run_log("INFO", f"STDOUT tail: {run.stdout[-1200:]}")
        if run.stderr:
            run_log("WARN", f"STDERR tail: {run.stderr[-1200:]}")

        expected_paths: List[Path] = []
        for table in schema_list:
            csv_name = f"{table['table_name']}.csv"
            csv_path = schema_output_dir / csv_name
            if not csv_path.exists():
                run_log("ERROR", f"Missing expected CSV: {csv_name}")
                raise FileNotFoundError(f"Missing generated file: {csv_name}")
            expected_paths.append(csv_path)
            run_log("INFO", f"Generated CSV: {csv_path}")

        _update_status(
            schema,
            dg_bulkdata_gen_status="DONE",
            dg_bulkdata_gen_log=run.stdout[-1000:] if run.stdout else "CSV generation completed.",
            dg_bulkdata_gen_at=_now_utc(),
            dg_sf_upload_status="INPROGRESS",
            dg_sf_upload_log="Running data validations.",
            dg_sf_upload_at=_now_utc(),
        )
        _write_db(db)
        run_log("INFO", "Step 2 done. Starting Step 3: data validation script generation and execution.")

        validation_code = _generate_simple_validation_code(schema_name, schema_list)
        validation_script_path = schema_datavalidtion_script_dir / "validation_code.py"
        validation_script_path.write_text(validation_code, encoding="utf-8")
        validation_report_path = schema_validation_dir / "validation_report.json"
        run_log("INFO", f"Validation code written: {validation_script_path} (chars={len(validation_code)})")

        venv = os.environ.copy()
        venv["CSV_DIR"] = str(schema_output_dir)
        venv["VALIDATION_REPORT_PATH"] = str(validation_report_path)
        run_log("INFO", f"Executing validation script: {sys.executable} {validation_script_path}")
        validation_run = None
        for v_attempt in range(1, 3):
            try:
                validation_run = subprocess.run(
                    [sys.executable, str(validation_script_path)],
                    capture_output=True,
                    text=True,
                    check=True,
                    env=venv,
                    cwd=str(schema_validation_dir),
                )
                break
            except subprocess.CalledProcessError as cpe:
                std_err = (cpe.stderr or "").strip()
                std_out = (cpe.stdout or "").strip()
                detail = std_err or std_out or str(cpe)
                run_log("ERROR", f"Validation script failed on attempt {v_attempt} with return code {cpe.returncode}")
                if std_out:
                    run_log("ERROR", f"Validation STDOUT tail: {std_out[-1200:]}")
                if std_err:
                    run_log("ERROR", f"Validation STDERR tail: {std_err[-1200:]}")

                if _needs_validation_json_patch(std_err):
                    patched = _patch_validation_json_serialization(validation_script_path)
                    if patched:
                        run_log("WARN", "Auto-patched validation JSON serialization for numpy/pandas scalar types and retrying.")
                        continue

                raise RuntimeError(
                    "Validation script failed. "
                    f"File: {validation_script_path}. "
                    f"Details: {detail[-1500:]}"
                ) from cpe

        if validation_run is None:
            run_log("WARN", "Validation script failed after retries. Using fallback validation report.")
            report = _write_fallback_validation_report(
                schema_list,
                schema_output_dir,
                validation_report_path,
                "validation script failed after retries",
            )
        else:
            run_log("INFO", "Validation script execution completed successfully.")
        if validation_run and validation_run.stdout:
            run_log("INFO", f"Validation STDOUT tail: {validation_run.stdout[-1200:]}")
        if validation_run and validation_run.stderr:
            run_log("WARN", f"Validation STDERR tail: {validation_run.stderr[-1200:]}")

        if not validation_report_path.exists():
            run_log("ERROR", "Validation report file missing after validation run.")
            raise FileNotFoundError("Validation script did not create validation_report.json")

        if validation_run is not None:
            try:
                raw_report = json.loads(validation_report_path.read_text(encoding="utf-8"))
                report = _normalize_validation_report_payload(raw_report)
            except Exception as parse_exc:
                run_log("WARN", f"Validation report parse failed ({parse_exc}). Using fallback validation report.")
                report = _write_fallback_validation_report(
                    schema_list,
                    schema_output_dir,
                    validation_report_path,
                    f"validation report parse failed: {parse_exc}",
                )

        table_reports = report.get("tables", [])
        report_by_name = {
            _normalize_table_name(item.get("table_name", "")): item
            for item in table_reports
        }
        for table in schema_list:
            tname = str(table["table_name"])
            matched = report_by_name.get(_normalize_table_name(tname))
            if not matched:
                run_log("ERROR", f"Validation report missing table section: {tname}")
                raise ValueError(f"Validation report missing table: {tname}")
            checks = matched.get("checks", [])
            if len(checks) < 2:
                run_log("ERROR", f"Validation checks less than 2 for table: {tname}")
                raise ValueError(f"Validation checks less than 2 for table: {tname}")
            run_log("INFO", f"Validation checks for table '{tname}': {len(checks)}")

        val_summary = str(report.get("summary", "")).strip()
        if not val_summary:
            val_summary = validation_run.stdout[-500:] if validation_run.stdout else "Validation completed."

        _update_status(
            schema,
            dg_sf_upload_status="DONE",
            dg_sf_upload_log=val_summary[:1000],
            dg_sf_upload_at=_now_utc(),
            schema_gen_status="INPROGRESS",
            schema_gen_log="Finalizing run.",
            schema_gen_last_update=_now_utc(),
        )
        _write_db(db)
        run_log("INFO", "Step 3 done. Starting Step 4 finalization.")

        _update_status(
            schema,
            schema_gen_status="DONE",
            schema_gen_log="Files saved locally.",
            schema_gen_last_update=_now_utc(),
        )
        _write_db(db)
        run_log("INFO", f"Run completed successfully. Validation report: {validation_report_path}")
        return expected_paths, schema_output_dir
    except Exception as exc:
        err = f"{exc}\n{traceback.format_exc()}"
        run_log("ERROR", f"Run failed: {exc}")
        run_log("ERROR", f"Traceback:\n{traceback.format_exc()}")
        dg_code = str(schema.get("dg_code_gen_status", "NEW")).upper()
        dg_bulk = str(schema.get("dg_bulkdata_gen_status", "NEW")).upper()
        dg_val = str(schema.get("dg_sf_upload_status", "NEW")).upper()
        _update_status(
            schema,
            schema_gen_status="ERROR",
            schema_gen_log=f"{str(exc)[:500]} | Log: {run_log_path}",
            dg_code_gen_status="ERROR" if dg_code in {"INPROGRESS", "PROCESSING", "UPLOADING"} else schema.get("dg_code_gen_status"),
            dg_bulkdata_gen_status="ERROR" if dg_bulk in {"INPROGRESS", "PROCESSING", "UPLOADING"} else schema.get("dg_bulkdata_gen_status"),
            dg_sf_upload_status="ERROR" if dg_val in {"INPROGRESS", "PROCESSING", "UPLOADING"} else schema.get("dg_sf_upload_status"),
            dg_sf_upload_log=f"{str(exc)[:500]} | Log: {run_log_path}",
        )
        schema["last_error_trace"] = err
        schema["last_run_log_path"] = str(run_log_path)
        _write_db(db)
        raise
