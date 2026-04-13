import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from data_generation_backend import (
    ensure_storage,
    generate_schema_data,
    get_schema,
    list_schemas,
    sync_schema_from_payload,
)


class TablePayload(BaseModel):
    table_id: str | None = None
    table_name: str
    num_entries: int = Field(gt=0)
    ddl: str
    instructions: str = ""
    columns_list: list[Any] = Field(default_factory=list)


class GenerateRequest(BaseModel):
    schema_name: str
    schema_prompt: str = ""
    tables: list[TablePayload] = Field(min_length=1)
    replace_existing: bool = True


class GenerateResponse(BaseModel):
    org_id: str
    schema_name: str
    output_dir: str
    generated_files: list[str]
    validation_report_path: str
    log_path: str | None = None
    status: str


app = FastAPI(title="DataGenX Headless API", version="1.0.0")
ensure_storage()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/schemas")
def schemas() -> dict[str, list[dict[str, str]]]:
    return {"schemas": list_schemas()}


@app.get("/schemas/{org_id}")
def schema_detail(org_id: str) -> dict[str, Any]:
    try:
        return get_schema(org_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/generate", response_model=GenerateResponse)
def generate(request: GenerateRequest) -> GenerateResponse:
    payload = request.model_dump()
    replace_existing = payload.pop("replace_existing", True)
    try:
        schema = sync_schema_from_payload(payload, replace_existing=replace_existing)
        generated_files, output_dir = generate_schema_data(str(schema["org_id"]))
        final_schema = get_schema(str(schema["org_id"]))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    schema_name = str(final_schema.get("org_name", schema["org_id"]))
    validation_report_path = (
        Path(__file__).resolve().parent
        / "schemas"
        / schema_name
        / "validations"
        / "validation_report.json"
    )

    return GenerateResponse(
        org_id=str(final_schema["org_id"]),
        schema_name=schema_name,
        output_dir=str(output_dir),
        generated_files=[str(path) for path in generated_files],
        validation_report_path=str(validation_report_path),
        log_path=str(final_schema.get("last_run_log_path", "")) or None,
        status=str(final_schema.get("schema_gen_status", "UNKNOWN")),
    )


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "DataGenX Headless API",
        "routes": [
            {"method": "GET", "path": "/health"},
            {"method": "GET", "path": "/schemas"},
            {"method": "GET", "path": "/schemas/{org_id}"},
            {"method": "POST", "path": "/generate"},
        ],
        "example_payload": {
            "schema_name": "Banking",
            "schema_prompt": "Generate realistic banking data with referential integrity.",
            "replace_existing": True,
            "tables": [
                {
                    "table_name": "CUSTOMERS",
                    "num_entries": 1000,
                    "ddl": "CREATE TABLE CUSTOMERS (CUSTOMER_ID BIGINT PRIMARY KEY, FULL_NAME VARCHAR(100));",
                    "instructions": "Generate realistic customer names.",
                }
            ],
        },
    }
