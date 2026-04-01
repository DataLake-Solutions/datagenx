import json
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


def get_schema(org_id: str) -> Dict[str, Any]:
    db = _read_db()
    return _find_schema(db, org_id)


def create_schema(schema_name: str) -> Dict[str, Any]:
    db = _read_db()
    safe = _safe_name(schema_name)
    names = {str(s.get("org_name", "")).lower() for s in db.get("schemas", [])}
    if safe.lower() in names:
        raise ValueError("Schema name already exists.")

    org_id = str(int(datetime.utcnow().timestamp()))
    new_schema = {
        "p_id": "1",
        "org_id": org_id,
        "org_name": safe,
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

    table_id = str(table_data.get("table_id", "")).strip()
    normalized = {
        "table_id": table_id or str(uuid.uuid4()),
        "table_name": str(table_data.get("table_name", "")).strip(),
        "num_entries": int(table_data.get("num_entries", 0)),
        "ddl": str(table_data.get("ddl", "")).strip(),
        "instructions": table_data.get("instructions", ""),
        "columns_list": table_data.get("columns_list", []),
    }

    if not normalized["table_name"] or not normalized["ddl"] or normalized["num_entries"] <= 0:
        raise ValueError("table_name, ddl and positive num_entries are required.")

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


def _normalize_table_name(value: str) -> str:
    v = str(value or "").strip()
    if v.lower().endswith(".csv"):
        v = v[:-4]
    return v.upper()


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
        "- Do NOT use unsupported/custom Faker calls like fake.version(), fake.semver(), fake.app_version(), or provider-specific methods.\n"
        "- For application version strings, generate with Python/random logic (e.g., f\"{random.randint(1,9)}.{random.randint(0,9)}.{random.randint(0,99)}\") instead of Faker.\n"
        "- For date arithmetic, convert Faker date strings to date/datetime objects before timedelta math.\n"
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

        validation_code = _openai_generate_validation_code(schema_name, schema.get("schema_prompt", ""), schema_list)
        validation_script_path = schema_datavalidtion_script_dir / "validation_code.py"
        validation_script_path.write_text(validation_code, encoding="utf-8")
        validation_report_path = schema_validation_dir / "validation_report.json"
        run_log("INFO", f"Validation code written: {validation_script_path} (chars={len(validation_code)})")

        venv = os.environ.copy()
        venv["CSV_DIR"] = str(schema_output_dir)
        venv["VALIDATION_REPORT_PATH"] = str(validation_report_path)
        run_log("INFO", f"Executing validation script: {sys.executable} {validation_script_path}")
        try:
            validation_run = subprocess.run(
                [sys.executable, str(validation_script_path)],
                capture_output=True,
                text=True,
                check=True,
                env=venv,
                cwd=str(schema_validation_dir),
            )
        except subprocess.CalledProcessError as cpe:
            std_err = (cpe.stderr or "").strip()
            std_out = (cpe.stdout or "").strip()
            detail = std_err or std_out or str(cpe)
            run_log("ERROR", f"Validation script failed with return code {cpe.returncode}")
            if std_out:
                run_log("ERROR", f"Validation STDOUT tail: {std_out[-1200:]}")
            if std_err:
                run_log("ERROR", f"Validation STDERR tail: {std_err[-1200:]}")
            raise RuntimeError(
                "Validation script failed. "
                f"File: {validation_script_path}. "
                f"Details: {detail[-1500:]}"
            ) from cpe
        run_log("INFO", "Validation script execution completed successfully.")
        if validation_run.stdout:
            run_log("INFO", f"Validation STDOUT tail: {validation_run.stdout[-1200:]}")
        if validation_run.stderr:
            run_log("WARN", f"Validation STDERR tail: {validation_run.stderr[-1200:]}")

        if not validation_report_path.exists():
            run_log("ERROR", "Validation report file missing after validation run.")
            raise FileNotFoundError("Validation script did not create validation_report.json")

        report = json.loads(validation_report_path.read_text(encoding="utf-8"))
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
