import csv
import json
import os
import re

CSV_DIR = os.getenv("CSV_DIR", ".")
VALIDATION_REPORT_PATH = os.getenv("VALIDATION_REPORT_PATH", "validation_report.json")
TABLES = [
  {
    "table_name": "RETAIL_CUSTOMERS",
    "expected_rows": 2000,
    "instructions": "Generate realistic US retail customers with unique emails and plausible phone numbers. CUSTOMER_SEGMENT should be one of {NEW,REGULAR,VIP} with a reasonable business distribution. LOYALTY_TIER should be one of {BRONZE,SILVER,GOLD,PLATINUM} and may be null for some NEW customers. COUNTRY_CODE should be mostly 'US'. CREATED_AT must be in the past and UPDATED_AT must be on or after CREATED_AT when present."
  },
  {
    "table_name": "RETAIL_ORDERS",
    "expected_rows": 5000,
    "instructions": "Every CUSTOMER_ID must exist in RETAIL_CUSTOMERS. ORDER_STATUS should be one of {PLACED,SHIPPED,DELIVERED,CANCELLED,RETURNED} with DELIVERED being most common. SALES_CHANNEL should be one of {ONLINE,STORE,MOBILE}. PAYMENT_METHOD should be one of {CARD,CASH,WALLET,GIFTCARD}. CURRENCY_CODE should be 'USD' for almost all rows. Monetary values must be positive where appropriate and TOTAL_AMOUNT must equal SUBTOTAL_AMOUNT + TAX_AMOUNT + SHIPPING_AMOUNT. ORDER_DATE and CREATED_AT must be in the past, and CREATED_AT should be on or after ORDER_DATE."
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
        f"Validation for schema 'Retail': {passed_checks}/{total_checks} checks passed."
    )

    out_dir = os.path.dirname(VALIDATION_REPORT_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(VALIDATION_REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)


if __name__ == "__main__":
    main()
