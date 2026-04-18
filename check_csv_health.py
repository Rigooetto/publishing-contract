#!/usr/bin/env python3
"""
Diagnostic script: scan all Believe CSVs and report skip reasons + revenue impact.

Usage:
    python check_csv_health.py path/to/folder/
    python check_csv_health.py path/to/file.csv
"""

import csv
import datetime
import decimal
import os
import re
import sys


COL_ALIASES = {
    "isrc":            ["isrc"],
    "platform":        ["platform"],
    "country":         ["country / region", "country/region", "country"],
    "sales_type":      ["sales type", "salestype", "sale type"],
    "reporting_month": ["reporting month", "reporting_month", "report month"],
    "sales_month":     ["sales month", "sales_month", "sale month"],
    "quantity":        ["quantity", "qty", "units"],
    "gross_revenue":   ["gross revenue", "gross_revenue", "gross"],
    "net_revenue":     ["net revenue", "net_revenue", "net"],
    "mechanical_fee":  ["mechanical fee", "mechanical_fee", "mechanical"],
}


def parse_decimal(val):
    val = val.strip().strip('"').replace('\xa0', '').replace(' ', '')
    if not val:
        return decimal.Decimal(0)
    if re.match(r'^-?\d{1,3}(\.\d{3})+(,\d+)?$', val):
        val = val.replace('.', '').replace(',', '.')
    else:
        val = val.replace(',', '')
    try:
        return decimal.Decimal(val)
    except Exception:
        return decimal.Decimal(0)


def parse_date(val):
    val = val.strip().strip('"')
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.datetime.strptime(val, fmt).date()
        except ValueError:
            pass
    m = re.match(r"^(\d{4})[/\-](\d{2})$", val)
    if m:
        return datetime.date(int(m.group(1)), int(m.group(2)), 1)
    return None


def detect_columns(header):
    header_lower = [h.lower() for h in header]
    col = {}
    for field, aliases in COL_ALIASES.items():
        for alias in aliases:
            if alias in header_lower:
                col[field] = header_lower.index(alias)
                break
    required = {"isrc", "platform", "country", "sales_type",
                "reporting_month", "sales_month", "net_revenue"}
    missing = required - set(col.keys())
    if missing:
        raise ValueError(f"Missing columns: {missing}  |  Headers: {header[:20]}")
    return col


def check_file(csv_path):
    fname = os.path.basename(csv_path)
    size_mb = os.path.getsize(csv_path) / 1_048_576

    with open(csv_path, encoding="utf-8-sig", errors="replace") as fh:
        first_line = fh.readline()
    delimiter = ";" if first_line.count(";") >= first_line.count(",") else ","

    rows_read = 0
    rows_ok = 0
    net_ok = decimal.Decimal(0)

    skip_blank_isrc     = 0
    skip_bad_date       = 0
    skip_exception      = 0
    skip_net_blank_isrc = decimal.Decimal(0)
    skip_net_bad_date   = decimal.Decimal(0)
    skip_net_exception  = decimal.Decimal(0)

    bad_date_examples   = []
    exception_examples  = []

    try:
        with open(csv_path, encoding="utf-8-sig", errors="replace", newline="") as fh:
            reader = csv.reader(fh, delimiter=delimiter, quotechar='"')
            raw_header = next(reader, None)
            if raw_header is None:
                return {"file": fname, "error": "Empty file"}
            header = [h.strip().strip('"') for h in raw_header]
            col = detect_columns(header)

            for raw_row in reader:
                rows_read += 1
                try:
                    # Try to get net revenue for skip accounting
                    try:
                        net_val = parse_decimal(raw_row[col["net_revenue"]].strip())
                    except Exception:
                        net_val = decimal.Decimal(0)

                    isrc = raw_row[col["isrc"]].strip().strip('"').upper()
                    if not isrc:
                        skip_blank_isrc += 1
                        skip_net_blank_isrc += net_val
                        continue

                    rep_month = parse_date(raw_row[col["reporting_month"]])
                    sal_month = parse_date(raw_row[col["sales_month"]])
                    if not rep_month or not sal_month:
                        skip_bad_date += 1
                        skip_net_bad_date += net_val
                        if len(bad_date_examples) < 3:
                            bad_date_examples.append({
                                "isrc": isrc,
                                "reporting_month": raw_row[col["reporting_month"]],
                                "sales_month": raw_row[col["sales_month"]],
                                "net": str(net_val),
                            })
                        continue

                    rows_ok += 1
                    net_ok += net_val

                except Exception as e:
                    skip_exception += 1
                    skip_net_exception += net_val
                    if len(exception_examples) < 3:
                        exception_examples.append({"row": rows_read, "error": str(e)})
                    continue

    except Exception as e:
        return {"file": fname, "error": str(e)}

    total_skipped = skip_blank_isrc + skip_bad_date + skip_exception
    total_skip_net = skip_net_blank_isrc + skip_net_bad_date + skip_net_exception

    return {
        "file":              fname,
        "size_mb":           round(size_mb, 1),
        "rows_read":         rows_read,
        "rows_ok":           rows_ok,
        "net_ok":            net_ok,
        "total_skipped":     total_skipped,
        "skip_net_total":    total_skip_net,
        "skip_blank_isrc":   skip_blank_isrc,
        "skip_net_blank_isrc": skip_net_blank_isrc,
        "skip_bad_date":     skip_bad_date,
        "skip_net_bad_date": skip_net_bad_date,
        "skip_exception":    skip_exception,
        "skip_net_exception": skip_net_exception,
        "bad_date_examples": bad_date_examples,
        "exception_examples": exception_examples,
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python check_csv_health.py <file_or_folder>")
        sys.exit(1)

    target = sys.argv[1]
    if os.path.isfile(target):
        files = [target]
    elif os.path.isdir(target):
        files = sorted(
            os.path.join(target, f)
            for f in os.listdir(target)
            if f.lower().endswith(".csv")
        )
    else:
        print(f"Path not found: {target}")
        sys.exit(1)

    grand_net_ok    = decimal.Decimal(0)
    grand_skip_net  = decimal.Decimal(0)
    grand_rows      = 0
    grand_skipped   = 0

    print(f"\n{'FILE':<45} {'ROWS':>8} {'SKIPPED':>8} {'SKIP $':>12} {'TOTAL NET $':>14}")
    print("-" * 95)

    all_results = []
    for fpath in files:
        print(f"  scanning {os.path.basename(fpath)}...", end="\r", flush=True)
        r = check_file(fpath)
        all_results.append(r)

        if "error" in r:
            print(f"{'  ERROR: ' + r['file']:<45}  {r['error']}")
            continue

        grand_net_ok   += r["net_ok"]
        grand_skip_net += r["skip_net_total"]
        grand_rows     += r["rows_read"]
        grand_skipped  += r["total_skipped"]

        skip_flag = "  <<<" if r["total_skipped"] > 0 else ""
        print(f"{r['file']:<45} {r['rows_read']:>8,} {r['total_skipped']:>8,} "
              f"{float(r['skip_net_total']):>12,.2f} {float(r['net_ok']):>14,.2f}{skip_flag}")

    print("-" * 95)
    print(f"{'TOTAL':<45} {grand_rows:>8,} {grand_skipped:>8,} "
          f"{float(grand_skip_net):>12,.2f} {float(grand_net_ok):>14,.2f}")
    print(f"\nGrand total (ok + skipped): ${float(grand_net_ok + grand_skip_net):,.2f}")

    # Detail section for files with skips
    problems = [r for r in all_results if "error" not in r and r["total_skipped"] > 0]
    if problems:
        print(f"\n{'─'*60}")
        print("SKIP DETAIL (files with skipped rows):")
        for r in problems:
            print(f"\n  {r['file']}")
            if r["skip_blank_isrc"]:
                print(f"    blank ISRC:  {r['skip_blank_isrc']:,} rows  ${float(r['skip_net_blank_isrc']):,.2f}")
            if r["skip_bad_date"]:
                print(f"    bad date:    {r['skip_bad_date']:,} rows  ${float(r['skip_net_bad_date']):,.2f}")
                for ex in r["bad_date_examples"]:
                    print(f"      e.g. ISRC={ex['isrc']}  rep={ex['reporting_month']}  sal={ex['sales_month']}  net={ex['net']}")
            if r["skip_exception"]:
                print(f"    parse error: {r['skip_exception']:,} rows  ${float(r['skip_net_exception']):,.2f}")
                for ex in r["exception_examples"]:
                    print(f"      row {ex['row']}: {ex['error']}")


if __name__ == "__main__":
    main()
