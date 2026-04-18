#!/usr/bin/env python3
"""
Standalone Believe streaming-royalty CSV importer.

Usage:
    # Single file
    python import_streaming.py path/to/file.csv

    # Entire folder (all *.csv files, sorted by name)
    python import_streaming.py path/to/folder/

    # Override database URL
    python import_streaming.py path/to/folder/ --db postgresql://user:pass@host/db

DATABASE_URL is read from (in order):
  1. --db argument
  2. DATABASE_URL environment variable
  3. .env file in this directory
"""

import argparse
import csv
import datetime
import decimal
import os
import re
import sys
import time

import psycopg2
import psycopg2.extras


# ── Config ────────────────────────────────────────────────────────────────────

FLUSH_EVERY = 10_000   # unique agg keys before flushing to DB
BATCH_SIZE  = 500      # rows per INSERT statement

COL_ALIASES = {
    "isrc":               ["isrc"],
    "platform":           ["platform"],
    "country":            ["country / region", "country/region", "country"],
    "sales_type":         ["sales type", "salestype", "sale type"],
    "reporting_month":    ["reporting month", "reporting_month", "report month"],
    "sales_month":        ["sales month", "sales_month", "sale month"],
    "quantity":           ["quantity", "qty", "units"],
    "gross_revenue":      ["gross revenue", "gross_revenue", "gross"],
    "net_revenue":        ["net revenue", "net_revenue", "net"],
    "mechanical_fee":     ["mechanical fee", "mechanical_fee", "mechanical"],
    "artist_name":        ["artist name", "artist_name", "artist"],
    "track_title":        ["track title", "track_title", "track name", "title"],
    "label_name":         ["label name", "label_name", "label"],
    "release_title":      ["release title", "release_title", "release name", "album"],
    "upc":                ["upc"],
    "streaming_sub_type": ["streaming subscription type", "streaming_subscription_type", "subscription type"],
    "release_type":       ["release type", "release_type"],
    "currency":           ["client payment currency", "currency", "payment currency"],
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_db_url():
    """Read DATABASE_URL from .env file."""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        return None
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("DATABASE_URL"):
                _, _, val = line.partition("=")
                return val.strip().strip('"').strip("'")
    return None


def parse_date(val):
    val = val.strip().strip('"')
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.datetime.strptime(val, fmt).date()
        except ValueError:
            pass
    # Try partial "YYYY/MM" or "YYYY-MM"
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
                "reporting_month", "sales_month", "quantity",
                "gross_revenue", "net_revenue", "mechanical_fee"}
    missing = required - set(col.keys())
    if missing:
        raise ValueError(f"Missing required columns: {missing}\nHeaders found: {header[:20]}")
    return col


def progress(msg):
    print(f"\r{msg}", end="", flush=True)


def println(msg):
    print(f"\r{msg}")


# ── ISRC → track_id lookup ────────────────────────────────────────────────────

def isrc_to_track_map(cur, isrc_set):
    if not isrc_set:
        return {}
    cur.execute(
        "SELECT isrc, id FROM track WHERE isrc = ANY(%s)",
        (list(isrc_set),)
    )
    return {row[0]: row[1] for row in cur.fetchall()}


# ── Flush aggregated batch to DB (UPSERT) ────────────────────────────────────

UPSERT_SQL = """
INSERT INTO streaming_royalty (
    import_id, isrc, platform, country, sales_type,
    reporting_month, sales_month,
    artist_name_csv, track_title_csv, label_name, release_title,
    upc, streaming_sub_type, release_type, currency,
    total_quantity, total_gross_revenue, total_net_revenue, total_mechanical_fee,
    track_id, created_at
) VALUES %s
ON CONFLICT ON CONSTRAINT uq_streaming_royalty_agg_key DO UPDATE SET
    total_quantity       = streaming_royalty.total_quantity       + EXCLUDED.total_quantity,
    total_gross_revenue  = streaming_royalty.total_gross_revenue  + EXCLUDED.total_gross_revenue,
    total_net_revenue    = streaming_royalty.total_net_revenue    + EXCLUDED.total_net_revenue,
    total_mechanical_fee = streaming_royalty.total_mechanical_fee + EXCLUDED.total_mechanical_fee
"""

def flush_agg(conn, cur, import_id, agg, meta, track_map):
    now = datetime.datetime.utcnow()
    rows = []
    for key, vals in agg.items():
        isrc, platform, country, sales_type, rep_iso, sal_iso = key
        m = meta[key]
        rows.append((
            import_id, isrc, platform, country, sales_type,
            datetime.date.fromisoformat(rep_iso),
            datetime.date.fromisoformat(sal_iso),
            m["artist_name"], m["track_title"], m["label_name"], m["release_title"],
            m["upc"], m["streaming_sub_type"], m["release_type"], m["currency"],
            vals["qty"], vals["gross"], vals["net"], vals["mech"],
            track_map.get(isrc),
            now,
        ))
    for i in range(0, len(rows), BATCH_SIZE):
        psycopg2.extras.execute_values(cur, UPSERT_SQL, rows[i:i + BATCH_SIZE])
    conn.commit()


# ── Process one CSV file ───────────────────────────────────────────────────────

def process_file(conn, cur, csv_path, file_index, total_files):
    fname = os.path.basename(csv_path)
    file_size_mb = os.path.getsize(csv_path) / 1_048_576
    prefix = f"[{file_index}/{total_files}] {fname} ({file_size_mb:.0f} MB)"

    # Create StreamingImport record
    cur.execute("""
        INSERT INTO streaming_import
            (original_filename, file_path, status, uploaded_by, uploaded_at)
        VALUES (%s, %s, 'processing', 'import_script', %s)
        RETURNING id
    """, (fname, csv_path, datetime.datetime.utcnow()))
    import_id = cur.fetchone()[0]
    conn.commit()

    agg  = {}
    meta = {}
    rows_read = 0
    rows_skipped = 0
    rows_aggregated_total = 0
    reporting_month = None
    start = time.time()

    try:
        # Detect delimiter
        with open(csv_path, encoding="utf-8-sig", errors="replace") as fh:
            first_line = fh.readline()
        delimiter = ";" if first_line.count(";") >= first_line.count(",") else ","

        with open(csv_path, encoding="utf-8-sig", errors="replace", newline="") as fh:
            reader = csv.reader(fh, delimiter=delimiter, quotechar='"')
            raw_header = next(reader, None)
            if raw_header is None:
                raise ValueError("Empty file")
            header = [h.strip().strip('"') for h in raw_header]
            col = detect_columns(header)

            for raw_row in reader:
                rows_read += 1

                try:
                    isrc = raw_row[col["isrc"]].strip().strip('"').upper()
                    if not isrc:
                        rows_skipped += 1
                        continue

                    rep_month = parse_date(raw_row[col["reporting_month"]])
                    sal_month = parse_date(raw_row[col["sales_month"]])
                    if not rep_month or not sal_month:
                        rows_skipped += 1
                        continue

                    if reporting_month is None:
                        reporting_month = rep_month

                    platform   = raw_row[col["platform"]].strip().strip('"')
                    country    = raw_row[col["country"]].strip().strip('"')
                    sales_type = raw_row[col["sales_type"]].strip().strip('"')
                    qty        = int(float(raw_row[col["quantity"]].strip().strip('"') or "0"))
                    gross      = decimal.Decimal(raw_row[col["gross_revenue"]].strip().strip('"') or "0")
                    net        = decimal.Decimal(raw_row[col["net_revenue"]].strip().strip('"') or "0")
                    mech       = decimal.Decimal(raw_row[col["mechanical_fee"]].strip().strip('"') or "0")

                    key = (isrc, platform, country, sales_type,
                           rep_month.isoformat(), sal_month.isoformat())

                    if key not in agg:
                        agg[key]  = {"qty": 0, "gross": decimal.Decimal(0),
                                     "net": decimal.Decimal(0), "mech": decimal.Decimal(0)}
                        meta[key] = {
                            "artist_name":      raw_row[col["artist_name"]].strip().strip('"') if "artist_name" in col else "",
                            "track_title":      raw_row[col["track_title"]].strip().strip('"') if "track_title" in col else "",
                            "label_name":       raw_row[col["label_name"]].strip().strip('"') if "label_name" in col else "",
                            "release_title":    raw_row[col["release_title"]].strip().strip('"') if "release_title" in col else "",
                            "upc":              raw_row[col["upc"]].strip().strip('"') if "upc" in col else "",
                            "streaming_sub_type": raw_row[col["streaming_sub_type"]].strip().strip('"') if "streaming_sub_type" in col else "",
                            "release_type":     raw_row[col["release_type"]].strip().strip('"') if "release_type" in col else "",
                            "currency":         raw_row[col["currency"]].strip().strip('"') if "currency" in col else "EUR",
                        }

                    agg[key]["qty"]   += qty
                    agg[key]["gross"] += gross
                    agg[key]["net"]   += net
                    agg[key]["mech"]  += mech

                except Exception:
                    rows_skipped += 1
                    continue

                # Periodic flush to keep memory bounded
                if len(agg) >= FLUSH_EVERY:
                    track_map = isrc_to_track_map(cur, {k[0] for k in agg})
                    flush_agg(conn, cur, import_id, agg, meta, track_map)
                    rows_aggregated_total += len(agg)
                    agg.clear()
                    meta.clear()

                # Progress every 25K rows
                if rows_read % 25_000 == 0:
                    elapsed = time.time() - start
                    rate = rows_read / elapsed if elapsed else 0
                    progress(f"{prefix} | {rows_read:,} rows  {rows_aggregated_total:,} agg  {rate:,.0f} r/s")

        # Final flush
        if agg:
            track_map = isrc_to_track_map(cur, {k[0] for k in agg})
            flush_agg(conn, cur, import_id, agg, meta, track_map)
            rows_aggregated_total += len(agg)

        elapsed = time.time() - start
        cur.execute("""
            UPDATE streaming_import
               SET status='done', finished_at=%s, rows_read=%s,
                   rows_aggregated=%s, rows_skipped=%s, reporting_month=%s
             WHERE id=%s
        """, (datetime.datetime.utcnow(), rows_read, rows_aggregated_total,
              rows_skipped, reporting_month, import_id))
        conn.commit()
        println(f"{prefix} | DONE  {rows_read:,} rows → {rows_aggregated_total:,} agg  ({elapsed:.1f}s)")

    except Exception as e:
        conn.rollback()
        cur.execute("""
            UPDATE streaming_import
               SET status='error', finished_at=%s, error_message=%s
             WHERE id=%s
        """, (datetime.datetime.utcnow(), str(e)[:2000], import_id))
        conn.commit()
        println(f"{prefix} | ERROR: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Import Believe streaming CSVs into LabelMind")
    parser.add_argument("path", help="CSV file or folder containing CSV files")
    parser.add_argument("--db", help="PostgreSQL DATABASE_URL (overrides env/.env)")
    args = parser.parse_args()

    db_url = args.db or os.environ.get("DATABASE_URL") or load_db_url()
    if not db_url:
        print("ERROR: No DATABASE_URL found. Pass --db or set DATABASE_URL in .env")
        sys.exit(1)

    # Collect files
    target = args.path
    if os.path.isfile(target):
        files = [target]
    elif os.path.isdir(target):
        files = sorted(
            os.path.join(target, f)
            for f in os.listdir(target)
            if f.lower().endswith(".csv")
        )
        if not files:
            print(f"No CSV files found in {target}")
            sys.exit(1)
    else:
        print(f"Path not found: {target}")
        sys.exit(1)

    total = len(files)
    print(f"Found {total} file(s) to import")
    print(f"Connecting to database...")

    conn = psycopg2.connect(db_url)
    cur  = conn.cursor()
    print(f"Connected.\n")

    overall_start = time.time()
    for i, fpath in enumerate(files, 1):
        process_file(conn, cur, fpath, i, total)

    cur.close()
    conn.close()
    elapsed = time.time() - overall_start
    print(f"\nAll done in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
