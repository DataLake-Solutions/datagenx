import csv
import json
import os
import re

CSV_DIR = os.getenv("CSV_DIR", ".")
VALIDATION_REPORT_PATH = os.getenv("VALIDATION_REPORT_PATH", "validation_report.json")
TABLES = [
  {
    "table_name": "INSU_CUSTOMER",
    "expected_rows": 6500,
    "instructions": "Purpose: Master records for insurance customers (individuals or businesses).\n    customer_id: generate UUID string; must be unique.\n    customer_type: choose from ['INDIVIDUAL','BUSINESS'] with distribution ~85/15.\n    gender: if customer_type='INDIVIDUAL', choose from ['MALE','FEMALE','OTHER'] with distribution ~49/49/1; if 'Business', NULL.\n    first_name/last_name: use faker names only when customer_type='INDIVIDUAL'; else 'NOT APPLICABLE'.\n    business_name: faker.company() only when customer_type='BUSINESS'; else 'NOT APPLICABLE'.\n    dob: if customer_type='INDIVIDUAL', random date between 1960-01-01 and 2005-12-31 (adults); if 'Business', NULL.\n    email: faker.email(); ~100% populated; prefer global uniqueness.\n    phone: faker.phone_number(); ~100% populated.\n    country: pick from ['US','IN','AE','UK','CA'] with distribution ~50/20/10/10/10; state/city/postal coherent to country.\n    address_line1: faker.street_address(); address_line2.\n    registration_date: date between 2015-01-01 and current_date.\n    created_at: datetime between registration_date and now (ensure created_at >= registration_date)."
  },
  {
    "table_name": "INSU_AGENT",
    "expected_rows": 500,
    "instructions": "Purpose: Master records for licensed agents/brokers.\n    agent_id: UUID; unique.\n    agent_code: string like 'AGT' + 7\u20139 digits; must be unique.\n    first_name/last_name: faker names.\n    email: faker.email(); ~100% populated; unique-ish.\n    phone: faker.phone_number(); ~100% populated.\n    license_no: string like 'LIC' + 8\u201310 digits; ~98% unique.\n    country/state/city: coherent geography; bias toward ['US','IN','UK'].\n    status: choose from ['ACTIVE','INACTIVE'] with ~85/15.\n    hire_date: between 2010-01-01 and today.\n    created_at: datetime between hire_date and now."
  },
  {
    "table_name": "INSU_POLICY",
    "expected_rows": 10000,
    "instructions": "POLICY_ID \u2192 Generate uuid4(); must be unique.\nPOLICY_NUMBER \u2192 Unique string like PL{YYYY}{8\u201310 digits} (e.g., PL2023 001829374).\nCUSTOMER_ID \u2192 Pick an existing INSU_CUSTOMER.CUSTOMER_ID (NOT NULL).\nPRODUCT_ID \u2192 Pick an existing INSU_PRODUCT.PRODUCT_ID, enforcing policy_type mix Auto/Home/Life/Health = 40%/25%/20%/15% via the product.\nAGENT_ID \u2192 Pick an existing INSU_AGENT.AGENT_ID ~90% of the time, else NULL (direct/online sale).\nSTATUS \u2192 One of ['ACTIVE','LAPSED','CANCELED','EXPIRED'] with ~70% Active (rest distributed).\nEFFECTIVE_DATE \u2192 Random date between 2016-01-01 and today.\nEXPIRATION_DATE \u2192 EFFECTIVE_DATE + 1 year \u00b1 60 days (ensure >= EFFECTIVE_DATE).\nPREMIUM_BILLING_FREQ \u2192 One of ['MONTHLY','QUARTERLY','ANNUAL'] with ~70% Monthly (20% Quarterly, 10% Annual).\nPREMIUM_AMOUNT \u2192 Annual premium > 0; suggest ranges by policy_type (Auto 300\u20132000, Home 500\u20133000, Life 200\u20135000, Health 200\u20134000) with \u00b115% noise; round 2 decimals.\nCURRENCY \u2192 Choose from ['USD','INR','AED','GBP','CAD'], default from customer country 80\u201390% of time (10\u201320% cross-currency noise).\nCREATED_AT \u2192 Timestamp between EFFECTIVE_DATE and LEAST(EXPIRATION_DATE, NOW())."
  },
  {
    "table_name": "INSU_RISK_OBJECT",
    "expected_rows": 16500,
    "instructions": "Purpose: Insured risk objects linked to policies (vehicles, properties, persons, equipment).\n    object_id: UUID; unique.\n    policy_id: FK to INSU_POLICY.policy_id; ~1\u20132 risk objects per policy on average to reach target rows.\n    object_type: choose from ['VEHICLE','PROPERTY','PERSON','EQUIPMENT'] with ~50/35/10/5.\n    description: faker.sentence(4\u201310 words).\n    year_made: for Vehicle/Equipment, integer between 1980 and 2025;\n    serial_no: ~100% populated for VEHICLE/PROPERTY;\n    address_line1/address_line2/city/state/postal_code/country: mostly for Property (~80% filled when object_type='PROPERTY'); for other types, fill ~20\u201330% only.\n    sum_insured: if available, pick between linked product.coverage_min and product.coverage_max; otherwise 20000\u20132000000.\n    created_at: datetime between policy.effective_date and policy.expiration_date (or now if expiration in past)."
  },
  {
    "table_name": "INSU_CLAIM",
    "expected_rows": 3500,
    "instructions": "Purpose: Claims raised against policies.\n    claim_id: UUID; unique.\n    claim_number: string like 'CL' + 9\u201312 digits; must be unique.\n    policy_id: FK to INSU_POLICY.policy_id.\n    customer_id: FK to INSU_CUSTOMER.customer_id (should match policy.customer_id).\n    claim_type: choose from ['ACCIDENT','THEFT','FIRE','MEDICAL','NATURAL DISASTER','LIABILITY','OTHERS'] with ~45/10/8/15/7/10/5.\n    incident_date: date between policy.effective_date and min(policy.expiration_date, today).\n    reported_date: incident_date + 0\u201330 days (>= incident_date).\n    status: choose from ['OPEN','IN-REVIEW','APPROVED','REJECTED','CLOSED','WITHDRAWN'] with ~25/20/25/10/18/2.\n    loss_estimate: 500\u2013500000 with long-tail; ensure >= 0.\n    deductible: 0\u20135000; often <= product.deductible_max if accessible.\n    approved_amount: if status in ['Approved','Closed'] then 50%\u2013100% of loss_estimate; if 'Rejected' then 0; if others then NULL.\n    closed_date: only when status='Closed'; pick between reported_date and reported_date + 365 days; else NULL.\n    created_at: datetime is now."
  },
  {
    "table_name": "INSU_PAYMENT",
    "expected_rows": 13500,
    "instructions": "Purpose: Monetary transactions for policies and claims (premiums, payouts, refunds).\n    payment_id: UUID; unique.\n    policy_id: FK to INSU_POLICY.policy_id.\n    claim_id: optional FK to INSU_CLAIM.claim_id; present for payouts (~30%), NULL for premium payments (~65%) and policy refunds (~5%).\n    direction: choose from ['Premium','Payout','Refund'] with ~65/30/5 (must align with claim_id presence).\n    status: choose from ['Completed','Pending','Failed'] with ~90/7/3.\n    method: choose from ['ACH','Card','Check','Wire','Cash','UPI'] with ~35/30/10/15/5/5.\n    amount: > 0; random amount from  policy.premium_amount \u00b120% \n    currency: pick based on policy/customer country mapping ['USD','INR','AED','GBP','CAD'].\n    payment_date: if claim_id present, between claim.reported_date and (claim.closed_date or claim.reported_date + 60 days); else within [policy.effective_date .. policy.expiration_date].\n    transaction_ref: string like 'TX' + 10\u201314 digits; mostly unique.\n    created_at: datetime between payment_date and now."
  }
]


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
        target = {}
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
            rules.append({"column": col, "target": target})
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
    csv_path = os.path.join(CSV_DIR, f"{table_name}.csv")

    checks = []
    exists = os.path.exists(csv_path)
    checks.append({
        "name": "CSV file exists",
        "passed": bool(exists),
        "details": csv_path,
    })

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
            checks.append({
                "name": "CSV readable",
                "passed": False,
                "details": str(exc),
            })
            return {"table_name": table_name, "checks": checks}

    row_count = len(rows)
    checks.append({
        "name": "CSV has header",
        "passed": bool(header),
        "details": f"header_columns={len(header)}",
    })
    checks.append({
        "name": "CSV has rows",
        "passed": bool(row_count > 0),
        "details": f"rows={row_count}",
    })

    if expected_rows > 0:
        checks.append({
            "name": "Row count matches expected num_entries",
            "passed": bool(row_count == expected_rows),
            "details": f"expected={expected_rows}, actual={row_count}",
        })

    # Phone format checks (instruction-aware)
    if "phone_number" in header and row_count > 0:
        mode = _phone_expectation_from_instructions(instructions)
        valid = 0
        for r in rows:
            raw = str(r.get("phone_number", "")).strip()
            if mode == "e164":
                ok = bool(re.match(r"^\+[1-9]\d(7, 14)$", raw))
            else:
                digits = re.sub(r"\D", "", raw)
                ok = len(digits) >= 10
            if ok:
                valid += 1
        rate = _ratio(valid, row_count)
        checks.append({
            "name": "Phone number format",
            "passed": bool(rate >= 0.95),
            "details": f"valid={valid}/{row_count} ({rate:.1%}), mode={mode}",
        })

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
        checks.append({
            "name": "Email format",
            "passed": bool(passed),
            "details": f"valid_non_empty={valid}/{non_empty}",
        })

    # Uniqueness checks for key-like columns (if present)
    for key_col in ["customer_id", "government_id", "phone_number"]:
        if key_col in header and row_count > 0:
            values = [str(r.get(key_col, "")).strip() for r in rows]
            non_empty = [v for v in values if v]
            unique_count = len(set(non_empty))
            passed = (len(non_empty) == row_count) and (unique_count == row_count)
            checks.append({
                "name": f"Unique/non-null {key_col}",
                "passed": bool(passed),
                "details": f"non_empty={len(non_empty)}, unique={unique_count}, rows={row_count}",
            })

    # Numeric consistency/range checks (if present)
    if "credit_score" in header and row_count > 0:
        vals = [_to_float(r.get("credit_score")) for r in rows]
        finite = [v for v in vals if v is not None]
        in_range = [v for v in finite if 300 <= v <= 900]
        passed = bool(finite) and (len(in_range) == len(finite))
        checks.append({
            "name": "Credit score range (300-900)",
            "passed": bool(passed),
            "details": f"in_range={len(in_range)}/{len(finite)}",
        })

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
        checks.append({
            "name": "Annual income = monthly_income * 12",
            "passed": bool(passed),
            "details": f"matched={matched}/{comparable}",
        })

    # Baseline distribution sanity
    for dist_col in ["gender", "lifecycle_stage", "account_status"]:
        if dist_col in header and row_count > 1:
            vals = [str(r.get(dist_col, "")).strip() for r in rows if str(r.get(dist_col, "")).strip()]
            distinct = len(set(vals))
            checks.append({
                "name": f"Distribution sanity for {dist_col}",
                "passed": bool(distinct >= 2),
                "details": f"distinct_values={distinct}",
            })

    # Instruction-driven distribution checks
    rules = _parse_distribution_rules(instructions)
    for rule in rules:
        col = str(rule.get("column", ""))
        target = rule.get("target", {})
        if not col or col not in header or not target or row_count == 0:
            continue

        values = [str(r.get(col, "")).strip() for r in rows if str(r.get(col, "")).strip()]
        total = len(values)
        if total == 0:
            checks.append({
                "name": f"Distribution rule for {col}",
                "passed": False,
                "details": "no non-empty values",
            })
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
            parts.append(f"{expected_val} actual={actual_pct:.1f}% target={float(expected_pct):.1f}%")

        checks.append({
            "name": f"Distribution rule for {col}",
            "passed": bool(ok_all),
            "details": "; ".join(parts) + f"; tolerance=+/-{tolerance:.1f}%",
        })

    return {"table_name": table_name, "checks": checks}


def main() -> None:
    report = {"summary": "", "tables": []}
    total_checks = 0
    passed_checks = 0

    for table in TABLES:
        table_report = _check_table(table)
        checks = table_report.get("checks", [])
        total_checks += len(checks)
        passed_checks += sum(1 for c in checks if bool(c.get("passed", False)))
        report["tables"].append(table_report)

    report["summary"] = (
        f"Validation for schema 'Insurance': {passed_checks}/{total_checks} checks passed."
    )

    out_dir = os.path.dirname(VALIDATION_REPORT_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(VALIDATION_REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)


if __name__ == "__main__":
    main()
