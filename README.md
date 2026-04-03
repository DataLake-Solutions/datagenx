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
4. Run:
   - `python -m streamlit run app.py`

## Notes

- Generated files are saved in project `schemas/`.
- Per schema, instruction folder contains:
  - `ddl.json`
  - `instructions.json`
  - `schema_prompt.json`
- UI keeps the same core generate/schema-management flow as your existing app.
- Query module and login are intentionally removed per requirement.

