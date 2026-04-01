<<<<<<< HEAD
# datagenx
A scalable engine that generates realistic and structured data from database schemas, enabling automated seeding, testing, and environment setup.
=======

# datagenx
A scalable engine that generates realistic and structured data from database schemas, enabling automated seeding, testing, and environment setup.
=======
# DataGen Local (Streamlit)

Local replica of DataGen generate flow:

- Local CSV output at `schemas/<schema_name>/Tables_generated/<table_name>.csv`
- Schema metadata JSON at `schemas/<schema_name>/instructions/`

## Setup

1. Create venv:
   - `python -m venv .venv`
2. Install:
   - `.\\.venv\\Scripts\\python -m pip install -r requirements.txt`
3. Fill `.env`:
   - `OPENAI_API_KEY=<your_key>`
   - `OPENAI_MODEL=gpt-4o-mini` (or any supported model)
4. Run:
   - `.\\.venv\\Scripts\\python -m streamlit run app.py`

## Notes

- Generated files are saved permanently in project `schemas/`.
- Per schema, instruction folder contains:
  - `ddl.json`
  - `instructions.json`
  - `schema_prompt.json`
- UI keeps the same core generate/schema-management flow as your existing app.
- Query module and login are intentionally removed per requirement.

