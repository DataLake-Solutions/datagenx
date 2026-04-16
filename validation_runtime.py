import csv
import json
import os
import re
import statistics
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional


def _normalize_name(value: str) -> str:
    text = str(value or "").strip()
    if text.lower().endswith(".csv"):
        text = text[:-4]
    if "." in text:
        text = text.split(".")[-1]
    return text.strip("\"'`[] ").upper()


def _normalize_cell(value: Any) -> str:
    return str(value or "").strip()


def _normalize_value_token(value: Any) -> str:
    return _normalize_cell(value).upper()


def _normalize_instruction_text(text: str) -> str:
    return (
        str(text or "")
        .replace("â†’", "->")
        .replace("→", "->")
        .replace("\u2212", "-")
        .replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
    )


def _split_identifier_list(expr: str) -> List[str]:
    parts = [part.strip() for part in str(expr or "").split(",") if part.strip()]
    return [_normalize_name(part) for part in parts]


def _extract_ddl_body(ddl: str) -> str:
    text = str(ddl or "")
    if "(" not in text or ")" not in text:
        return ""
    return text.split("(", 1)[1].rsplit(")", 1)[0]


def _parse_table_meta(table: Dict[str, Any]) -> Dict[str, Any]:
    ddl = str(table.get("ddl", "") or "")
    meta = {
        "table_name": _normalize_name(table.get("table_name", "")),
        "expected_rows": int(table.get("expected_rows", table.get("num_entries", 0)) or 0),
        "instructions": str(table.get("instructions", "") or ""),
        "ddl": ddl,
        "columns": [],
        "primary_key": [],
        "unique_columns": [],
        "foreign_keys": [],
    }

    for raw_line in _extract_ddl_body(ddl).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line:
            continue
        upper = line.upper()

        pk_match = re.match(r"PRIMARY\s+KEY\s*\(([^)]+)\)", line, flags=re.IGNORECASE)
        if pk_match:
            meta["primary_key"] = _split_identifier_list(pk_match.group(1))
            continue

        fk_match = re.match(
            r"FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+([A-Za-z0-9_.$\"`[\]]+)\s*\(([^)]+)\)",
            line,
            flags=re.IGNORECASE,
        )
        if fk_match:
            meta["foreign_keys"].append(
                {
                    "child_columns": _split_identifier_list(fk_match.group(1)),
                    "parent_table": _normalize_name(fk_match.group(2)),
                    "parent_columns": _split_identifier_list(fk_match.group(3)),
                }
            )
            continue

        first_token = line.split()[0]
        column_name = _normalize_name(first_token)
        if not column_name:
            continue
        meta["columns"].append(column_name)

        if " PRIMARY KEY" in upper or upper.endswith("PRIMARY KEY"):
            if column_name not in meta["primary_key"]:
                meta["primary_key"].append(column_name)
        if " UNIQUE" in upper:
            if column_name not in meta["unique_columns"]:
                meta["unique_columns"].append(column_name)

        inline_ref_match = re.search(
            r"REFERENCES\s+([A-Za-z0-9_.$\"`[\]]+)\s*\(([^)]+)\)",
            line,
            flags=re.IGNORECASE,
        )
        if inline_ref_match:
            meta["foreign_keys"].append(
                {
                    "child_columns": [column_name],
                    "parent_table": _normalize_name(inline_ref_match.group(1)),
                    "parent_columns": _split_identifier_list(inline_ref_match.group(2)),
                }
            )

    meta["columns"] = list(dict.fromkeys(meta["columns"]))
    meta["primary_key"] = list(dict.fromkeys(meta["primary_key"]))
    meta["unique_columns"] = list(dict.fromkeys(meta["unique_columns"]))
    return meta


def _load_table_csv(csv_dir: str, table_meta: Dict[str, Any]) -> Dict[str, Any]:
    table_name = str(table_meta.get("table_name", ""))
    csv_path = os.path.join(csv_dir, f"{table_name}.csv")
    info = {
        "path": csv_path,
        "exists": os.path.exists(csv_path),
        "header": [],
        "rows": [],
        "read_error": "",
    }
    if not info["exists"]:
        return info

    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            info["header"] = [_normalize_name(name) for name in (reader.fieldnames or [])]
            for row in reader:
                normalized_row = {}
                for key, value in row.items():
                    normalized_row[_normalize_name(key)] = _normalize_cell(value)
                info["rows"].append(normalized_row)
    except Exception as exc:
        info["read_error"] = str(exc)
    return info


def _extract_enum_values(text: str) -> List[str]:
    match = re.search(r"\{([^{}]+)\}|\[([^\[\]]+)\]", text or "")
    if not match:
        return []
    payload = match.group(1) or match.group(2) or ""
    values = []
    for part in payload.split(","):
        token = part.strip().strip("\"'")
        if token:
            values.append(token)
    return values


def _extract_explicit_percentages(text: str) -> Dict[str, float]:
    matches = re.findall(
        r"(\d+(?:\.\d+)?)%\s*[\"']([^\"']+)[\"']",
        text or "",
        flags=re.IGNORECASE,
    )
    result: Dict[str, float] = {}
    for pct, value in matches:
        key = _normalize_value_token(value)
        if key:
            result[key] = float(pct)
    return result


def _extract_ratio_distribution(text: str, enum_values: List[str]) -> Dict[str, float]:
    match = re.search(
        r"~?\s*(\d+(?:\.\d+)?(?:/\d+(?:\.\d+)?)+)\s*(?:distribution|split)",
        text or "",
        flags=re.IGNORECASE,
    )
    if not match or not enum_values:
        return {}
    ratios = [float(item) for item in match.group(1).split("/") if item.strip()]
    if len(ratios) != len(enum_values):
        return {}
    total = sum(ratios) or 1.0
    return {
        _normalize_value_token(enum_values[idx]): (ratios[idx] * 100.0 / total)
        for idx in range(len(enum_values))
    }


def _parse_number(value: Any) -> Optional[float]:
    text = _normalize_cell(value).replace(",", "")
    if not text:
        return None
    if text.endswith("%"):
        text = text[:-1]
    try:
        return float(text)
    except ValueError:
        return None


def _parse_range_text(text: str) -> Optional[Dict[str, float]]:
    clean = _normalize_instruction_text(text)
    match = re.search(r"(-?\d+(?:\.\d+)?)\s*(?:to|-)\s*(-?\d+(?:\.\d+)?)", clean)
    if not match:
        return None
    low = float(match.group(1))
    high = float(match.group(2))
    return {"min": min(low, high), "max": max(low, high)}


def _value_matches_range(value: Optional[float], numeric_range: Optional[Dict[str, float]]) -> bool:
    if value is None:
        return False
    if not numeric_range:
        return True
    return numeric_range["min"] <= value <= numeric_range["max"]


def _split_instruction_line(line: str) -> Optional[tuple[str, str]]:
    normalized = _normalize_instruction_text(line)
    if "->" not in normalized:
        match = re.match(
            r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+("
            r"in\s+[\{\[].+|"
            r"between\s+.+|"
            r"(?:<=|>=|=|<|>)\s*.+|"
            r"If\s+.+"
            r")$",
            normalized,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        return _normalize_name(match.group(1)), match.group(2).strip()
    left, right = normalized.split("->", 1)
    column = _normalize_name(left)
    if not column:
        return None
    return column, right.strip()


def _parse_by_category_domain_rule(column: str, right: str) -> Optional[Dict[str, Any]]:
    match = re.match(r"By\s+([A-Za-z0-9_]+)\s*:\s*(.+)", right, flags=re.IGNORECASE)
    if not match:
        return None
    condition_column = _normalize_name(match.group(1))
    mappings = []
    for segment in [part.strip() for part in match.group(2).strip().rstrip(".").split(";") if part.strip()]:
        if "->" not in segment:
            continue
        label_text, values_text = segment.split("->", 1)
        labels = [_normalize_value_token(token) for token in re.split(r"/|,", label_text) if token.strip()]
        allowed = [_normalize_value_token(value) for value in _extract_enum_values(values_text)]
        if labels and allowed:
            mappings.append({"labels": labels, "allowed": allowed})
    if not mappings:
        return None
    return {"column": column, "condition_column": condition_column, "mappings": mappings}


def _parse_if_else_numeric_rule(column: str, right: str) -> Optional[Dict[str, Any]]:
    clean = _normalize_instruction_text(right)
    match = re.search(
        r'If\s+([A-Za-z0-9_]+)\s*=\s*"([^"]+)"\s*:\s*(\d+(?:\.\d+)?)%\s+([A-Za-z]+)\s*\(([^)]+)\)\s*,\s*else\s+([A-Za-z]+)\s*\(([^)]+)\)',
        clean,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return {
        "column": column,
        "condition_column": _normalize_name(match.group(1)),
        "condition_value": _normalize_value_token(match.group(2)),
        "expected_pct": float(match.group(3)),
        "condition_kind": match.group(4).strip().lower(),
        "condition_range": _parse_range_text(match.group(5)),
        "else_kind": match.group(6).strip().lower(),
        "else_range": _parse_range_text(match.group(7)),
    }


def _parse_threshold_numeric_rule(column: str, right: str) -> Optional[Dict[str, Any]]:
    clean = _normalize_instruction_text(right)
    pct_match = re.search(r"(\d+(?:\.\d+)?)%\s*([<>]=?)\s*(-?\d+(?:\.\d+)?)", clean)
    if not pct_match:
        return None
    return {
        "column": column,
        "expected_pct": float(pct_match.group(1)),
        "operator": pct_match.group(2),
        "threshold": float(pct_match.group(3)),
        "overall_range": _parse_range_text(clean),
    }


def _parse_range_by_category_rule(column: str, right: str) -> Optional[Dict[str, Any]]:
    clean = _normalize_instruction_text(right)
    matches = re.findall(
        r"(-?\d+(?:\.\d+)?)%?\s*-\s*(-?\d+(?:\.\d+)?)%?\s+for\s+([A-Za-z0-9_ /-]+?)(?=,|$)",
        clean,
        flags=re.IGNORECASE,
    )
    if not matches:
        return None
    mappings = []
    for low, high, labels_text in matches:
        labels = [_normalize_value_token(part) for part in labels_text.split("/") if part.strip()]
        if labels:
            mappings.append(
                {
                    "labels": labels,
                    "range": {"min": min(float(low), float(high)), "max": max(float(low), float(high))},
                }
            )
    if not mappings:
        return None
    return {"column": column, "condition_column": None, "mappings": mappings}


def _parse_instruction_rules(instructions: str) -> Dict[str, List[Dict[str, Any]]]:
    rules: Dict[str, List[Dict[str, Any]]] = {"distribution": [], "domain": []}
    for raw_line in str(instructions or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        separator = None
        for token in ("→", "->"):
            if token in line:
                separator = token
                break
        if separator is None:
            continue

        left, right = line.split(separator, 1)
        column = _normalize_name(left)
        if not column:
            continue

        enum_values = _extract_enum_values(right)
        explicit = _extract_explicit_percentages(right)
        target = explicit or _extract_ratio_distribution(right, enum_values)

        if target:
            rules["distribution"].append({"column": column, "target": target})
        elif enum_values:
            rules["domain"].append(
                {
                    "column": column,
                    "allowed": [_normalize_value_token(value) for value in enum_values],
                }
            )
    return rules


def _parse_instruction_rules_v2(instructions: str) -> Dict[str, List[Dict[str, Any]]]:
    rules: Dict[str, List[Dict[str, Any]]] = {
        "distribution": [],
        "domain": [],
        "conditional_domain": [],
        "conditional_numeric": [],
        "threshold_numeric": [],
        "range_by_category": [],
    }
    for raw_line in str(instructions or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        parts = _split_instruction_line(line)
        if not parts:
            continue
        column, right = parts

        conditional_domain = _parse_by_category_domain_rule(column, right)
        if conditional_domain:
            rules["conditional_domain"].append(conditional_domain)
            continue

        conditional_numeric = _parse_if_else_numeric_rule(column, right)
        if conditional_numeric:
            rules["conditional_numeric"].append(conditional_numeric)
            continue

        range_by_category = _parse_range_by_category_rule(column, right)
        if range_by_category:
            rules["range_by_category"].append(range_by_category)
            continue

        threshold_numeric = _parse_threshold_numeric_rule(column, right)
        if threshold_numeric:
            rules["threshold_numeric"].append(threshold_numeric)

        enum_values = _extract_enum_values(right)
        explicit = _extract_explicit_percentages(right)
        target = explicit or _extract_ratio_distribution(right, enum_values)

        if target:
            rules["distribution"].append({"column": column, "target": target})
        elif enum_values:
            rules["domain"].append(
                {
                    "column": column,
                    "allowed": [_normalize_value_token(value) for value in enum_values],
                }
            )
    return rules


def _add_check(checks: List[Dict[str, Any]], name: str, passed: bool, details: str) -> None:
    checks.append({"name": name, "passed": bool(passed), "details": str(details)})


def _check_primary_and_unique_keys(table_meta: Dict[str, Any], table_data: Dict[str, Any], checks: List[Dict[str, Any]]) -> None:
    header = set(table_data.get("header", []))
    rows = table_data.get("rows", [])
    pk_cols = list(table_meta.get("primary_key", []))
    if pk_cols and all(col in header for col in pk_cols):
        tuples = [tuple(row.get(col, "") for col in pk_cols) for row in rows]
        missing = sum(1 for item in tuples if any(not value for value in item))
        unique_count = len(set(item for item in tuples if all(value for value in item)))
        passed = (missing == 0) and (unique_count == len(rows))
        if len(pk_cols) > 1:
            label = f"Composite primary key is unique and complete ({', '.join(pk_cols)})"
        else:
            label = f"{pk_cols[0]} is unique and populated"
        details = f"Checked {len(rows)} rows. Unique key combinations: {unique_count}. Rows with missing key values: {missing}."
        _add_check(checks, label, passed, details)

    for col in table_meta.get("unique_columns", []):
        if col not in header:
            continue
        values = [row.get(col, "") for row in rows]
        non_empty = [value for value in values if value]
        unique_count = len(set(non_empty))
        passed = (len(non_empty) == len(rows)) and (unique_count == len(rows))
        details = f"Checked {len(rows)} rows. Populated values: {len(non_empty)}. Unique values: {unique_count}."
        _add_check(checks, f"{col} values are unique", passed, details)


def _build_parent_key_set(parent_rows: List[Dict[str, Any]], parent_columns: List[str]) -> set[tuple[str, ...]]:
    keys = set()
    for row in parent_rows:
        key = tuple(row.get(col, "") for col in parent_columns)
        if all(key):
            keys.add(key)
    return keys


def _evaluate_relationships(
    schema_meta: Dict[str, Dict[str, Any]],
    loaded_tables: Dict[str, Dict[str, Any]],
    table_reports: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    relationship_checks = []
    for child_table, table_meta in schema_meta.items():
        child_rows = loaded_tables.get(child_table, {}).get("rows", [])
        child_header = set(loaded_tables.get(child_table, {}).get("header", []))
        child_checks = table_reports[child_table]["checks"]
        for fk in table_meta.get("foreign_keys", []):
            child_columns = list(fk.get("child_columns", []))
            parent_table = str(fk.get("parent_table", ""))
            parent_columns = list(fk.get("parent_columns", []))
            parent_rows = loaded_tables.get(parent_table, {}).get("rows", [])
            parent_header = set(loaded_tables.get(parent_table, {}).get("header", []))

            relationship_name = f"{child_table} -> {parent_table}"
            if not child_columns or not parent_columns:
                continue
            if not all(col in child_header for col in child_columns):
                _add_check(
                    child_checks,
                    f"No orphaned records for {relationship_name}",
                    False,
                    f"Validation could not run because child columns are missing: {child_columns}.",
                )
                relationship_checks.append(
                    {
                        "relationship": relationship_name,
                        "child_columns": child_columns,
                        "parent_columns": parent_columns,
                        "passed": False,
                        "details": "missing child columns",
                    }
                )
                continue
            if not all(col in parent_header for col in parent_columns):
                _add_check(
                    child_checks,
                    f"No orphaned records for {relationship_name}",
                    False,
                    f"Validation could not run because parent columns are missing: {parent_columns}.",
                )
                relationship_checks.append(
                    {
                        "relationship": relationship_name,
                        "child_columns": child_columns,
                        "parent_columns": parent_columns,
                        "passed": False,
                        "details": "missing parent columns",
                    }
                )
                continue

            parent_key_set = _build_parent_key_set(parent_rows, parent_columns)
            total_children = len(child_rows)
            matched_children = 0
            orphan_children = 0
            null_children = 0
            for row in child_rows:
                key = tuple(row.get(col, "") for col in child_columns)
                if not all(key):
                    null_children += 1
                    continue
                if key in parent_key_set:
                    matched_children += 1
                else:
                    orphan_children += 1

            passed = orphan_children == 0
            details = (
                f"Matched {matched_children} of {total_children} child rows to {parent_table}. "
                f"Orphan rows: {orphan_children}. Child rows with null key values: {null_children}."
            )
            _add_check(child_checks, f"No orphaned records for {relationship_name}", passed, details)
            relationship_checks.append(
                {
                    "relationship": relationship_name,
                    "child_table": child_table,
                    "parent_table": parent_table,
                    "child_columns": child_columns,
                    "parent_columns": parent_columns,
                    "total_children": total_children,
                    "matched_children": matched_children,
                    "orphan_children": orphan_children,
                    "null_children": null_children,
                    "passed": passed,
                }
            )
    return relationship_checks


def _condition_matches(row: Dict[str, Any], column: str, allowed_values: Iterable[str]) -> bool:
    return _normalize_value_token(row.get(column, "")) in set(allowed_values)


def _infer_condition_column(
    table_header: set[str],
    table_rows: List[Dict[str, Any]],
    mappings: List[Dict[str, Any]],
    explicit_column: Optional[str] = None,
) -> Optional[str]:
    if explicit_column and explicit_column in table_header:
        return explicit_column
    labels = {label for mapping in mappings for label in mapping.get("labels", [])}
    if not labels:
        return None

    best_column = None
    best_score = -1
    for column in table_header:
        sample_values = {_normalize_value_token(row.get(column, "")) for row in table_rows if row.get(column, "")}
        if not sample_values or len(sample_values) > 50:
            continue
        overlap = len(sample_values & labels)
        if overlap > best_score:
            best_score = overlap
            best_column = column
        if overlap == len(labels):
            return column
    return best_column if best_score > 0 else None


def _evaluate_distribution_rule(
    table_name: str,
    table_rows: List[Dict[str, Any]],
    checks: List[Dict[str, Any]],
    column: str,
    target: Dict[str, float],
) -> Dict[str, Any]:
    values = [_normalize_value_token(row.get(column, "")) for row in table_rows if row.get(column, "")]
    total = len(values)
    if total == 0:
        passed = False
        actual_distribution: List[Dict[str, Any]] = []
        details = f"No populated values were available to validate {column} distribution."
    else:
        counts = Counter(values)
        actual_distribution = []
        parts = []
        passed = True
        tolerance = 10.0
        for value, expected_pct in target.items():
            count = counts.get(value, 0)
            actual_pct = (100.0 * count) / total
            actual_distribution.append(
                {
                    "value": value,
                    "count": count,
                    "pct": round(actual_pct, 2),
                    "target_pct": round(float(expected_pct), 2),
                }
            )
            if abs(actual_pct - float(expected_pct)) > tolerance:
                passed = False
            parts.append(f"{value}: actual={actual_pct:.2f}% target={float(expected_pct):.2f}%")
        unexpected = sorted(value for value in counts if value not in target)
        if unexpected:
            passed = False
            parts.append(f"unexpected_values={unexpected}")
        details = f"Validated {column} on {total} populated rows. " + "; ".join(parts)

    _add_check(checks, f"{column} distribution matches instruction", passed, details)
    return {
        "table_name": table_name,
        "column": column,
        "total_rows": len(table_rows),
        "non_empty_rows": total,
        "expected_distribution": [{"value": value, "pct": round(float(pct), 2)} for value, pct in target.items()],
        "actual_distribution": actual_distribution,
        "passed": passed,
        "details": details,
    }


def _build_group_profile(rows: List[Dict[str, Any]], target_column: str, group_by: List[str]) -> List[Dict[str, Any]]:
    buckets: Dict[tuple[str, ...], List[float]] = {}
    for row in rows:
        number = _parse_number(row.get(target_column, ""))
        if number is None:
            continue
        key = tuple(_normalize_value_token(row.get(col, "")) or "(BLANK)" for col in group_by)
        buckets.setdefault(key, []).append(number)

    profiles = []
    for key, values in sorted(buckets.items()):
        group = {group_by[idx]: key[idx] for idx in range(len(group_by))}
        profiles.append(
            {
                "group": group,
                "count": len(values),
                "avg": round(sum(values) / len(values), 2),
                "median": round(float(statistics.median(values)), 2),
                "min": round(min(values), 2),
                "max": round(max(values), 2),
            }
        )
    return profiles


def _match_operator(value: float, operator: str, threshold: float) -> bool:
    if operator == ">":
        return value > threshold
    if operator == ">=":
        return value >= threshold
    if operator == "<":
        return value < threshold
    if operator == "<=":
        return value <= threshold
    return False


def _evaluate_instruction_rules(
    schema_meta: Dict[str, Dict[str, Any]],
    loaded_tables: Dict[str, Dict[str, Any]],
    table_reports: Dict[str, Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    distribution_checks = []
    domain_checks = []

    for table_name, table_meta in schema_meta.items():
        table_rows = loaded_tables.get(table_name, {}).get("rows", [])
        table_header = set(loaded_tables.get(table_name, {}).get("header", []))
        checks = table_reports[table_name]["checks"]
        rules = _parse_instruction_rules(table_meta.get("instructions", ""))

        for rule in rules.get("domain", []):
            column = str(rule.get("column", ""))
            allowed = list(rule.get("allowed", []))
            if column not in table_header or not allowed:
                continue
            values = [_normalize_value_token(row.get(column, "")) for row in table_rows if row.get(column, "")]
            invalid = [value for value in values if value not in allowed]
            passed = len(invalid) == 0
            details = f"Allowed values: {allowed}. Invalid rows found: {len(invalid)}."
            _add_check(checks, f"{column} values match allowed instruction set", passed, details)
            domain_checks.append(
                {
                    "table_name": table_name,
                    "column": column,
                    "allowed_values": allowed,
                    "invalid_count": len(invalid),
                    "passed": passed,
                }
            )

        for rule in rules.get("distribution", []):
            column = str(rule.get("column", ""))
            target = dict(rule.get("target", {}))
            if column not in table_header or not target:
                continue
            values = [_normalize_value_token(row.get(column, "")) for row in table_rows if row.get(column, "")]
            total = len(values)
            if total == 0:
                passed = False
                actual_distribution = []
                details = f"column={column}, no non-empty values"
            else:
                counts = Counter(values)
                actual_distribution = []
                parts = []
                passed = True
                tolerance = 10.0
                for value, expected_pct in target.items():
                    count = counts.get(value, 0)
                    actual_pct = (100.0 * count) / total
                    actual_distribution.append(
                        {
                            "value": value,
                            "count": count,
                            "pct": round(actual_pct, 2),
                            "target_pct": round(float(expected_pct), 2),
                        }
                    )
                    if abs(actual_pct - float(expected_pct)) > tolerance:
                        passed = False
                    parts.append(
                        f"{value}: actual={actual_pct:.2f}% target={float(expected_pct):.2f}%"
                    )
                unexpected = sorted(value for value in counts if value not in target)
                if unexpected:
                    passed = False
                    parts.append(f"unexpected_values={unexpected}")
                details = f"column={column}, total={total}, " + "; ".join(parts)

            _add_check(checks, f"Distribution for {column}", passed, details)
            distribution_checks.append(
                {
                    "table_name": table_name,
                    "column": column,
                    "total_rows": len(table_rows),
                    "non_empty_rows": total,
                    "expected_distribution": [
                        {"value": value, "pct": round(float(pct), 2)}
                        for value, pct in target.items()
                    ],
                    "actual_distribution": actual_distribution,
                    "passed": passed,
                    "details": details,
                }
            )

    return {
        "distribution_checks": distribution_checks,
        "domain_checks": domain_checks,
    }


def _evaluate_instruction_rules_v2(
    schema_meta: Dict[str, Dict[str, Any]],
    loaded_tables: Dict[str, Dict[str, Any]],
    table_reports: Dict[str, Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    distribution_checks = []
    domain_checks = []
    conditional_checks = []
    numeric_profiles = []

    for table_name, table_meta in schema_meta.items():
        table_rows = loaded_tables.get(table_name, {}).get("rows", [])
        table_header = set(loaded_tables.get(table_name, {}).get("header", []))
        checks = table_reports[table_name]["checks"]
        rules = _parse_instruction_rules_v2(table_meta.get("instructions", ""))

        for rule in rules.get("domain", []):
            column = str(rule.get("column", ""))
            allowed = list(rule.get("allowed", []))
            if column not in table_header or not allowed:
                continue
            values = [_normalize_value_token(row.get(column, "")) for row in table_rows if row.get(column, "")]
            invalid = [value for value in values if value not in allowed]
            passed = len(invalid) == 0
            details = f"column={column}, allowed={allowed}, invalid_count={len(invalid)}"
            _add_check(checks, f"Allowed values for {column}", passed, details)
            domain_checks.append(
                {
                    "table_name": table_name,
                    "column": column,
                    "allowed_values": allowed,
                    "invalid_count": len(invalid),
                    "passed": passed,
                }
            )

        for rule in rules.get("conditional_domain", []):
            column = str(rule.get("column", ""))
            mappings = list(rule.get("mappings", []))
            condition_column = _infer_condition_column(table_header, table_rows, mappings, rule.get("condition_column"))
            if column not in table_header or not condition_column:
                continue

            group_results = []
            passed = True
            total_invalid = 0
            for mapping in mappings:
                labels = list(mapping.get("labels", []))
                allowed = list(mapping.get("allowed", []))
                scoped_rows = [row for row in table_rows if _condition_matches(row, condition_column, labels)]
                invalid = [
                    _normalize_value_token(row.get(column, ""))
                    for row in scoped_rows
                    if row.get(column, "") and _normalize_value_token(row.get(column, "")) not in allowed
                ]
                mapping_passed = len(invalid) == 0
                passed = passed and mapping_passed
                total_invalid += len(invalid)
                group_results.append(
                    {
                        "when": {condition_column: labels},
                        "allowed_values": allowed,
                        "rows": len(scoped_rows),
                        "invalid_count": len(invalid),
                        "passed": mapping_passed,
                    }
                )

            details = (
                f"Validated {column} against allowed values by {condition_column}. "
                f"Groups checked: {len(group_results)}. Invalid rows found: {total_invalid}."
            )
            _add_check(checks, f"{column} values match instruction by {condition_column}", passed, details)
            domain_checks.append(
                {
                    "table_name": table_name,
                    "column": column,
                    "condition_column": condition_column,
                    "groups": group_results,
                    "invalid_count": total_invalid,
                    "passed": passed,
                }
            )

        for rule in rules.get("distribution", []):
            column = str(rule.get("column", ""))
            target = dict(rule.get("target", {}))
            if column not in table_header or not target:
                continue
            distribution_checks.append(_evaluate_distribution_rule(table_name, table_rows, checks, column, target))

        for rule in rules.get("conditional_numeric", []):
            column = str(rule.get("column", ""))
            condition_column = str(rule.get("condition_column", ""))
            condition_value = str(rule.get("condition_value", ""))
            if column not in table_header or condition_column not in table_header:
                continue

            condition_rows = [row for row in table_rows if _normalize_value_token(row.get(condition_column, "")) == condition_value]
            else_rows = [row for row in table_rows if _normalize_value_token(row.get(condition_column, "")) != condition_value]

            def _matches_kind(row: Dict[str, Any], kind: str, numeric_range: Optional[Dict[str, float]]) -> bool:
                number = _parse_number(row.get(column, ""))
                if number is None:
                    return False
                if kind == "negative" and number >= 0:
                    return False
                if kind == "positive" and number <= 0:
                    return False
                return _value_matches_range(number, numeric_range)

            condition_match_count = sum(1 for row in condition_rows if _matches_kind(row, str(rule.get("condition_kind", "")), rule.get("condition_range")))
            condition_total = len(condition_rows)
            condition_pct = (100.0 * condition_match_count / condition_total) if condition_total else 0.0
            condition_expected_pct = float(rule.get("expected_pct", 0.0))
            condition_passed = condition_total > 0 and abs(condition_pct - condition_expected_pct) <= 10.0

            else_match_count = sum(1 for row in else_rows if _matches_kind(row, str(rule.get("else_kind", "")), rule.get("else_range")))
            else_total = len(else_rows)
            else_pct = (100.0 * else_match_count / else_total) if else_total else 0.0
            else_passed = else_total == 0 or else_pct >= 95.0

            passed = condition_passed and else_passed
            details = (
                f"column={column}, if {condition_column}={condition_value}: "
                f"actual={condition_pct:.2f}% target={condition_expected_pct:.2f}% "
                f"(matching={condition_match_count}/{condition_total}); "
                f"else actual_in_range={else_pct:.2f}% (matching={else_match_count}/{else_total})"
            )
            _add_check(checks, f"{column} conditional distribution matches instruction", passed, details)
            conditional_checks.append(
                {
                    "table_name": table_name,
                    "column": column,
                    "type": "if_else_numeric_distribution",
                    "condition_column": condition_column,
                    "condition_value": condition_value,
                    "condition_result": {
                        "rows": condition_total,
                        "matching_rows": condition_match_count,
                        "actual_pct": round(condition_pct, 2),
                        "target_pct": round(condition_expected_pct, 2),
                        "expected_kind": rule.get("condition_kind"),
                        "expected_range": rule.get("condition_range"),
                        "passed": condition_passed,
                    },
                    "else_result": {
                        "rows": else_total,
                        "matching_rows": else_match_count,
                        "actual_pct": round(else_pct, 2),
                        "target_pct": 100.0,
                        "expected_kind": rule.get("else_kind"),
                        "expected_range": rule.get("else_range"),
                        "passed": else_passed,
                    },
                    "passed": passed,
                    "details": details,
                }
            )
            numeric_profiles.append(
                {
                    "table_name": table_name,
                    "column": column,
                    "group_by": [condition_column],
                    "profiles": _build_group_profile(table_rows, column, [condition_column]),
                }
            )
            if "CURRENCY" in table_header and condition_column != "CURRENCY":
                numeric_profiles.append(
                    {
                        "table_name": table_name,
                        "column": column,
                        "group_by": [condition_column, "CURRENCY"],
                        "profiles": _build_group_profile(table_rows, column, [condition_column, "CURRENCY"]),
                    }
                )

        for rule in rules.get("range_by_category", []):
            column = str(rule.get("column", ""))
            mappings = list(rule.get("mappings", []))
            condition_column = _infer_condition_column(table_header, table_rows, mappings, rule.get("condition_column"))
            if column not in table_header or not condition_column:
                continue

            group_results = []
            passed = True
            for mapping in mappings:
                labels = list(mapping.get("labels", []))
                numeric_range = dict(mapping.get("range", {}))
                scoped_rows = [row for row in table_rows if _condition_matches(row, condition_column, labels)]
                numeric_values = [_parse_number(row.get(column, "")) for row in scoped_rows]
                numeric_values = [value for value in numeric_values if value is not None]
                in_range = sum(1 for value in numeric_values if _value_matches_range(value, numeric_range))
                total = len(numeric_values)
                pct = (100.0 * in_range / total) if total else 0.0
                mapping_passed = total > 0 and pct >= 95.0
                passed = passed and mapping_passed
                group_results.append(
                    {
                        "when": {condition_column: labels},
                        "rows": total,
                        "matching_rows": in_range,
                        "actual_pct": round(pct, 2),
                        "target_pct": 100.0,
                        "expected_range": numeric_range,
                        "passed": mapping_passed,
                    }
                )

            details = f"Validated {column} ranges by {condition_column}. Groups checked: {len(group_results)}."
            _add_check(checks, f"{column} range matches instruction by {condition_column}", passed, details)
            conditional_checks.append(
                {
                    "table_name": table_name,
                    "column": column,
                    "type": "range_by_category",
                    "condition_column": condition_column,
                    "groups": group_results,
                    "passed": passed,
                    "details": details,
                }
            )
            numeric_profiles.append(
                {
                    "table_name": table_name,
                    "column": column,
                    "group_by": [condition_column],
                    "profiles": _build_group_profile(table_rows, column, [condition_column]),
                }
            )

        for rule in rules.get("threshold_numeric", []):
            column = str(rule.get("column", ""))
            if column not in table_header:
                continue
            numeric_values = [_parse_number(row.get(column, "")) for row in table_rows]
            numeric_values = [value for value in numeric_values if value is not None]
            if not numeric_values:
                continue

            overall_range = rule.get("overall_range")
            range_pct = (
                100.0
                * sum(1 for value in numeric_values if _value_matches_range(value, overall_range))
                / len(numeric_values)
                if overall_range
                else 100.0
            )
            operator = str(rule.get("operator", ""))
            threshold = float(rule.get("threshold", 0.0))
            expected_pct = float(rule.get("expected_pct", 0.0))
            match_count = sum(1 for value in numeric_values if _match_operator(value, operator, threshold))
            actual_pct = 100.0 * match_count / len(numeric_values)
            passed = abs(actual_pct - expected_pct) <= 10.0 and (range_pct >= 95.0 if overall_range else True)
            details = (
                f"{actual_pct:.2f}% of values satisfied {column} {operator} {threshold}; "
                f"target was {expected_pct:.2f}%. Values within expected range: {range_pct:.2f}%."
            )
            _add_check(checks, f"{column} threshold distribution matches instruction", passed, details)
            conditional_checks.append(
                {
                    "table_name": table_name,
                    "column": column,
                    "type": "threshold_distribution",
                    "operator": operator,
                    "threshold": threshold,
                    "actual_pct": round(actual_pct, 2),
                    "target_pct": round(expected_pct, 2),
                    "overall_range": overall_range,
                    "in_range_pct": round(range_pct, 2),
                    "passed": passed,
                    "details": details,
                }
            )
            numeric_profiles.append(
                {
                    "table_name": table_name,
                    "column": column,
                    "group_by": [],
                    "profiles": _build_group_profile(table_rows, column, []),
                }
            )

    return {
        "distribution_checks": distribution_checks,
        "domain_checks": domain_checks,
        "conditional_checks": conditional_checks,
        "numeric_profiles": numeric_profiles,
    }


def run_validation(
    schema_name: str,
    tables_meta: List[Dict[str, Any]],
    csv_dir: str,
    validation_report_path: str,
) -> Dict[str, Any]:
    schema_meta = {
        _normalize_name(table.get("table_name", "")): _parse_table_meta(table)
        for table in tables_meta
    }
    loaded_tables = {
        table_name: _load_table_csv(csv_dir, table_meta)
        for table_name, table_meta in schema_meta.items()
    }

    table_reports: Dict[str, Dict[str, Any]] = {}
    for table_name, table_meta in schema_meta.items():
        table_data = loaded_tables[table_name]
        checks: List[Dict[str, Any]] = []
        if not table_data.get("exists", False):
            _add_check(checks, f"Generated data available for {table_name}", False, "Expected generated output file was not found for validation.")
            table_reports[table_name] = {"table_name": table_name, "checks": checks}
            continue
        if table_data.get("read_error"):
            _add_check(checks, f"Generated data readable for {table_name}", False, table_data["read_error"])
            table_reports[table_name] = {"table_name": table_name, "checks": checks}
            continue

        header = table_data.get("header", [])
        rows = table_data.get("rows", [])
        expected_rows = int(table_meta.get("expected_rows", 0) or 0)
        if expected_rows > 0:
            _add_check(
                checks,
                "Generated row count matches requested volume",
                len(rows) == expected_rows,
                f"Expected {expected_rows} rows and found {len(rows)} rows.",
            )
        elif not rows:
            _add_check(checks, f"Generated data contains rows for {table_name}", False, "No rows were available to validate.")
        _check_primary_and_unique_keys(table_meta, table_data, checks)
        table_reports[table_name] = {"table_name": table_name, "checks": checks}

    relationship_checks = _evaluate_relationships(schema_meta, loaded_tables, table_reports)
    rule_outputs = _evaluate_instruction_rules_v2(schema_meta, loaded_tables, table_reports)

    report = {
        "summary": "",
        "tables": [table_reports[name] for name in schema_meta.keys()],
        "relationship_checks": relationship_checks,
        "distribution_checks": rule_outputs["distribution_checks"],
        "domain_checks": rule_outputs["domain_checks"],
        "conditional_checks": rule_outputs["conditional_checks"],
        "numeric_profiles": rule_outputs["numeric_profiles"],
    }

    all_checks = []
    for table in report["tables"]:
        all_checks.extend(table.get("checks", []))
    passed_checks = sum(1 for check in all_checks if bool(check.get("passed", False)))
    total_checks = len(all_checks)
    report["summary"] = (
        f"Validation for schema '{schema_name}': {passed_checks}/{total_checks} checks passed. "
        f"Relationships={len(relationship_checks)}, distributions={len(rule_outputs['distribution_checks'])}, "
        f"conditionals={len(rule_outputs['conditional_checks'])}, profiles={len(rule_outputs['numeric_profiles'])}."
    )

    out_dir = os.path.dirname(validation_report_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(validation_report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    return report
