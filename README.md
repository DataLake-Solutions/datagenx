Copyright (c) 2026 DataLake Solutions. All rights reserved.

This source code and all related materials are proprietary to DataLake Solutions.

You may not, without prior written permission from DataLake Solutions:
- copy, modify, distribute, sublicense, sell, publish, or otherwise disclose this code;
- share this code with third parties or post it to public repositories, forums, or websites;
- use this code to create derivative works for external distribution or commercial exploitation.

Unauthorized use, disclosure, or distribution is strictly prohibited.

# datagenx
A scalable engine that generates realistic and structured data from database schemas, enabling automated seeding, testing, and environment setup.
=======

# DataGen Local (Streamlit)
- Local CSV output at `schemas/<schema_name>/Tables_generated/<table_name>.csv`
- Schema metadata JSON at `schemas/<schema_name>/instructions/`

## Setup
1. Create venv:
   - `python -m venv .venv`
2. Install:
   - `python -m pip install -r requirements.txt`
3. Fill `.env`:
   - `OPENAI_API_KEY=<your_key>`
   - `OPENAI_MODEL=gpt-4o-mini` (or any supported model)
4. Run Streamlit UI:
   - `python -m streamlit run app.py`

## Headless FastAPI

The project includes a headless API that accepts one JSON payload with the schema prompt and table definitions, then runs the same backend generation flow used by the UI.

### What this means

You do not need to open the Streamlit UI.

Instead, you:

1. Start the API server.
2. Send one JSON file to the API.
3. The backend generates the data.
4. The backend validates the data.
5. The API returns the output folder and file paths.

### Important note

Use the API server without `--reload` when generating data.

Why:

- this project creates and updates files while it runs
- `--reload` can restart the API in the middle of a run
- that can interrupt generation or validation

### Step 1: Open PowerShell and go to the project folder

Run:

```powershell
cd ..\datagenx
```

### Step 2: Start the API server

Run:

```powershell
.\.venv\Scripts\python.exe -m uvicorn headless_api:app --host 127.0.0.1 --port 8000
```

After startup:

- Swagger UI: `http://127.0.0.1:8000/docs`
- Health check: `http://127.0.0.1:8000/health`

Leave this terminal open.

### Step 3: Open a second PowerShell window

Run:

```powershell
cd ..\datagenx
```

### Step 4: Check that the API is running

Run:

```powershell
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8000/health"
```

Expected result:

```json
{
  "status": "ok"
}
```

### Step 5: Send the JSON payload to the API

Main endpoint:

- `POST /generate`

Payload shape:

```json
{
  "schema_name": "Banking",
  "schema_prompt": "Generate realistic synthetic US banking data with referential integrity.",
  "replace_existing": true,
  "tables": [
    {
      "table_name": "CUSTOMERS",
      "num_entries": 1000,
      "ddl": "CREATE TABLE CUSTOMERS (\n  CUSTOMER_ID BIGINT PRIMARY KEY,\n  FULL_NAME VARCHAR(100) NOT NULL,\n  EMAIL VARCHAR(120) UNIQUE\n);",
      "instructions": "Generate realistic customer names and unique emails."
    },
    {
      "table_name": "ACCOUNTS",
      "num_entries": 1200,
      "ddl": "CREATE TABLE ACCOUNTS (\n  ACCOUNT_ID BIGINT PRIMARY KEY,\n  CUSTOMER_ID BIGINT NOT NULL,\n  ACCOUNT_NUMBER VARCHAR(20) UNIQUE,\n  FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMERS(CUSTOMER_ID)\n);",
      "instructions": "Every CUSTOMER_ID must exist in CUSTOMERS."
    }
  ]
}
```

### Example request

Example payload files are available in the `examples/` folder.

- Retail two-table example: `examples/retail_two_table_payload.json`

PowerShell:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8000/generate" `
  -ContentType "application/json" `
  -InFile "examples\retail_two_table_payload.json"
```

What this command does:

- reads the JSON file from the `examples` folder
- sends it to the API
- runs schema creation, data generation, and validation
- returns the output location and status

cURL:

```bash
curl -X POST "http://127.0.0.1:8000/generate" \
  -H "Content-Type: application/json" \
  --data-binary "@examples/retail_two_table_payload.json"
```

### Other endpoints

- `GET /schemas` to list saved schemas
- `GET /schemas/{org_id}` to inspect one saved schema and its statuses

### Step 6: Read the response

The `POST /generate` response includes:

- `org_id`
- `schema_name`
- `output_dir`
- `generated_files`
- `validation_report_path`
- `log_path`
- `status`

If `status` is `DONE`, the run completed successfully.

### Step 7: Open the generated files

For the retail example, generated files will be under:

- `schemas\Retail\Tables_generated\`
- `schemas\Retail\validations\validation_report.json`
- `schemas\Retail\logs\latest.log`

### Simple summary

For a non-technical user, the headless flow is:

1. Start the API.
2. Check `/health`.
3. Send the JSON file.
4. Wait for the response.
5. Open the generated files.

## Notes

- Generated files are saved in project `schemas/`.
- Per schema, instruction folder contains:
  - `ddl.json`
  - `instructions.json`
  - `schema_prompt.json`
- UI keeps the same core generate/schema-management flow as your existing app.
- Query module and login are intentionally removed per requirement.
