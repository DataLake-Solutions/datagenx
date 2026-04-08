import csv
import json
import os
import re

CSV_DIR = os.getenv("CSV_DIR", ".")
VALIDATION_REPORT_PATH = os.getenv("VALIDATION_REPORT_PATH", "validation_report.json")
TABLES = [
  {
    "table_name": "BIAN_PARTY",
    "expected_rows": 50000,
    "instructions": "PARTY_ID \u2192 Generate UUID; must be unique.\nFULL_NAME \u2192 Faker full name.\nDATE_OF_BIRTH \u2192 Random date between 1970-01-01 and 1990-12-31.\nGENDER \u2192 Choose from ['MALE','FEMALE','OTHER'] with ~70/29/1 distribution.\nNATIONALITY \u2192 Choose from ['US','IN','AE','UK','CA'] with ~50/20/10/10/10.\nSSN \u2192 Faker SSN-like string; ensure uniqueness per COUNTRY rules if needed.\nEMAIL \u2192 Faker email; prefer globally unique values.\nPHONE_NUMBER \u2192 Faker phone number (E.164 if you need consistency).\nADDRESS_LINE1 \u2192 Faker street address.\nADDRESS_LINE2 \u2192 Faker secondary address or empty ~40% of time.\nCITY \u2192 Faker city.\nSTATE \u2192 Faker state/region.\nPOSTAL_CODE \u2192 Faker postal code.\nCOUNTRY \u2192 Mirror NATIONALITY distribution ['US','IN','AE','UK','CA'] ~50/20/10/10/10.\nCREATED_AT \u2192 Random timestamp within last 10 years."
  },
  {
    "table_name": "BIAN_ACCOUNTS",
    "expected_rows": 10000,
    "instructions": "ACCOUNT_ID \u2192 Generate UUID; unique.\nPARTY_ID \u2192 Randomly pick from existing BIAN_PARTY(PARTY_ID).\nACCOUNT_TYPE \u2192 60% \"Checking\", 30% \"Savings\", 10% \"Credit Card\".\nACCOUNT_STATUS \u2192 70% \"Active\", 20% \"Dormant\", 10% \"Closed\".\nOPEN_DATE \u2192 Random date within last 15 years.\nBALANCE \u2192 If ACCOUNT_TYPE=\"Credit Card\": 20% negative (\u22125000 to \u2212100), else positive (100 to 100000).\nCURRENCY \u2192 Choose from [\"USD\",\"EUR\",\"GBP\",\"INR\",\"AED\"] weighted by PARTY.COUNTRY if available.\nINTEREST_RATE \u2192 0.1%\u20135% for Checking/Savings, 10%\u201325% for Credit Card.\nBRANCH_ID \u2192 \"BR\" + 3\u20134 digit code (e.g., BR001\u2013BR050).\nCREATED_AT \u2192 Random timestamp on or after OPEN_DATE (spread up to ~10 years)"
  },
  {
    "table_name": "BIAN_TRANSACTION",
    "expected_rows": 10000,
    "instructions": "TRANSACTION_ID \u2192 Generate UUID; unique.\nACCOUNT_ID \u2192 Randomly pick from existing BIAN_ACCOUNTS(ACCOUNT_ID).\nTRANSACTION_TYPE \u2192 Choose from [\"DEPOSIT\",\"WITHDRAWAL\",\"PAYMENT\",\"TRANSFER\",\"FEE\",\"REFUND\"] with realistic skew (e.g., DEPOSIT/WITHDRAWAL heavy).\nAMOUNT \u2192 > 0; typical range 1\u20135000 (tail up to 20000 for high-value).\nCURRENCY \u2192 Inherit from the linked account\u2019s CURRENCY when possible; else pick from [\"USD\",\"EUR\",\"GBP\",\"INR\",\"AED\"].\nTRANSACTION_TIMESTAMP \u2192 Faker datetime between 2010-01-01 and now.\nMERCHANT_NAME \u2192 Faker company (blank for non-merchant types like TRANSFER/FEE).\nMERCHANT_CATEGORY \u2192 Pick from [\"GROCERY\",\"RESTAURANT\",\"FUEL\",\"ONLINE\",\"TRAVEL\",\"UTILITY\",\"OTHER\"]; empty for TRANSFER/FEE.\nLOCATION \u2192 City + Country for merchanted transactions; else empty.\nCHANNEL \u2192 Choose from [\"POS\",\"ONLINE\",\"MOBILE\",\"ATM\",\"BRANCH\"].\nSTATUS \u2192 92% \"POSTED\", 6% \"PENDING\", 2% \"REVERSED\".\nCREATED_AT \u2192 Timestamp on or after TRANSACTION_TIMESTAMP (within +0\u20133 days).\nBehavioral rules \u2192 ~80% non-fraud normal mix; ~10% \u201cstructuring\u201d sequences: per selected ACCOUNT_ID, emit 3\u20135 DEPOSITs of 1000\u20139999 within 1\u20133 days; ~10% high-velocity bursts: 2\u20134 small PAYMENTS/WITHDRAWALS within 30\u2013120 minutes."
  },
  {
    "table_name": "BIAN_KYC_PROFILE",
    "expected_rows": 10000,
    "instructions": "KYC_ID \u2192 UUID; unique.\nPARTY_ID \u2192 Random from BIAN_PARTY(PARTY_ID).\nKYC_STATUS \u2192 70% \"Verified\", 20% \"Pending\", 10% \"Rejected\".\nREVIEW_DATE \u2192 Random date within last 3 years (bias to last 90 days if \"Pending\").\nRISK_LEVEL \u2192 25% \"High\", 50% \"Medium\", 25% \"Low\" (increase \"High\" odds when KYC_STATUS=\"Rejected\").\nREVIEWER_ID \u2192 From pool EMP1001\u2013EMP1020.\nLAST_UPDATED \u2192 Timestamp on or after REVIEW_DATE (very recent if \"Pending\")."
  },
  {
    "table_name": "BIAN_DOCUMENT",
    "expected_rows": 5000,
    "instructions": "DOC_ID \u2192 UUID; unique.\nPARTY_ID \u2192 Random from BIAN_PARTY(PARTY_ID).\nDOC_TYPE \u2192 50% \"Passport\", 30% \"Driver License\", 20% \"National ID\".\nDOC_NUMBER \u2192 Pattern by DOC_TYPE (e.g., Passport: 2 letters + 7 digits; Driver License: alnum 8\u201312; National ID: digits 10\u201312).\nISSUE_DATE \u2192 Random date within last 15 years.\nEXPIRY_DATE \u2192 After ISSUE_DATE: 5\u201310 years for Passport/Driver License, ~10 years for National ID.\nISSUING_AUTHORITY \u2192 From [\"Passport Office\",\"DMV\",\"National ID Authority\"] by DOC_TYPE.\nDOC_STATUS \u2192 85% \"Active\", 15% \"Expired\" (force \"Expired\" if EXPIRY_DATE < today).\nUPLOADED_AT \u2192 Timestamp within \u00b17 days of ISSUE_DATE."
  },
  {
    "table_name": "BIAN_RISK_ASSESSMENT",
    "expected_rows": 10000,
    "instructions": "RISK_ID \u2192 UUID; unique.\nPARTY_ID \u2192 Random from BIAN_PARTY(PARTY_ID).\nRISK_MODEL_NAME \u2192 Choose from [\"MODEL_A\",\"MODEL_B\",\"MODEL_C\"].\nRISK_SCORE \u2192 Float 0\u2013100 with 20% > 80 (high-risk).\nRISK_CATEGORY \u2192 Derive: \"High\" if >80, \"Medium\" if 40\u201380, \"Low\" if <40.\nREVIEWED_BY \u2192 EMP2001\u2013EMP2020.\nREVIEW_DATE \u2192 Date within last 2 years.\nRECOMMENDATION \u2192 From [\"Enhanced Due Diligence\",\"Monitor\",\"Proceed\"] based on RISK_CATEGORY.\nCREATED_AT \u2192 Timestamp on or after REVIEW_DATE."
  },
  {
    "table_name": "BIAN_DEVICE_ACCESS",
    "expected_rows": 50000,
    "instructions": "DEVICE_ID \u2192 UUID; unique.\nPARTY_ID \u2192 Random from BIAN_PARTY(PARTY_ID).\nDEVICE_TYPE \u2192 60% \"Mobile\", 30% \"Desktop\", 10% \"Tablet\".\nDEVICE_OS \u2192 By DEVICE_TYPE: Mobile \u2192 [\"iOS\",\"Android\"]; Desktop \u2192 [\"Windows\",\"macOS\",\"Linux\"]; Tablet \u2192 [\"iPadOS\",\"Android\"].\nIP_ADDRESS \u2192 Valid IPv4 or IPv6.\nLOCATION \u2192 City + Country.\nLOGIN_TIME \u2192 Timestamp within last 6 months.\nLOGOUT_TIME \u2192 After LOGIN_TIME (0.1\u20138 hours later); 5% sessions may omit LOGOUT_TIME to simulate active sessions.\nAUTH_METHOD \u2192 15% \"Biometric\", 50% \"Password\", 35% \"Two-Factor\".\nSESSION_STATUS \u2192 \"Active\" if no LOGOUT_TIME else \"Closed\"."
  },
  {
    "table_name": "BIAN_FRAUD_ALERT",
    "expected_rows": 17000,
    "instructions": "ALERT_ID \u2192 UUID; unique.\nTRANSACTION_ID \u2192 Random from BIAN_TRANSACTION(TRANSACTION_ID).\nPARTY_ID \u2192 Random from BIAN_PARTY(PARTY_ID) (ideally matching the party behind the linked account).\nALERT_TYPE \u2192 Choose from [\"Unusual Location\",\"Multiple Failed Logins\",\"High-Value Transaction\",\"Suspicious IP\"].\nTRIGGERED_AT \u2192 Timestamp within last 1 year.\nRESOLUTION_STATUS \u2192 60% \"Open\", 30% \"Resolved\", 10% \"Escalated\".\nRESOLVED_BY \u2192 If RESOLUTION_STATUS in (\"Resolved\",\"Escalated\"): EMP3001\u2013EMP3050 else NULL."
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
        f"Validation for schema 'BIAN': {passed_checks}/{total_checks} checks passed."
    )

    out_dir = os.path.dirname(VALIDATION_REPORT_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(VALIDATION_REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)


if __name__ == "__main__":
    main()
