#!/usr/bin/env python3
"""
One-time fix: rename a canonical artist name in artist_name_map + artist_royalty_split.

Usage:
    python fix_artist_name.py "Manue Pena" "Manuel Peña"
    python fix_artist_name.py "Manue Pena" "Manuel Peña" --db postgresql://...
"""

import argparse
import os
import sys

import psycopg2


def _load_env_key(key):
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        return None
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line.startswith(key + "="):
                return line.partition("=")[2].strip().strip('"').strip("'")
    return None


def main():
    parser = argparse.ArgumentParser(description="Rename canonical artist name in royalties DB")
    parser.add_argument("old_name", help="Current canonical name (exact)")
    parser.add_argument("new_name", help="New canonical name")
    parser.add_argument("--db", help="ROYALTIES_DATABASE_URL (overrides env/.env)")
    args = parser.parse_args()

    old, new = args.old_name.strip(), args.new_name.strip()
    if old == new:
        print("ERROR: old and new names are identical.")
        sys.exit(1)

    db_url = args.db or os.environ.get("ROYALTIES_DATABASE_URL") or _load_env_key("ROYALTIES_DATABASE_URL")
    if not db_url:
        print("ERROR: No ROYALTIES_DATABASE_URL. Pass --db or set it in .env")
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # 1. Rename all canonical_name references
    cur.execute(
        "UPDATE artist_name_map SET canonical_name = %s WHERE canonical_name = %s",
        (new, old)
    )
    map_canonical_count = cur.rowcount
    print(f"artist_name_map canonical_name updated: {map_canonical_count} row(s)")

    # 2. Rename self-mapping row (raw_name == old canonical)
    cur.execute(
        "UPDATE artist_name_map SET raw_name = %s, canonical_name = %s "
        "WHERE raw_name = %s AND canonical_name = %s",
        (new, new, old, old)
    )
    map_self_count = cur.rowcount
    print(f"artist_name_map self-mapping updated: {map_self_count} row(s)")

    # 3. Rename artist_royalty_split
    cur.execute(
        "UPDATE artist_royalty_split SET artist_name = %s WHERE artist_name = %s",
        (new, old)
    )
    split_count = cur.rowcount
    print(f"artist_royalty_split updated: {split_count} row(s)")

    # 4. Also rename artist_royalty_detail (ARD) — avoids needing a full rebuild
    cur.execute(
        "UPDATE artist_royalty_detail SET artist_name = %s WHERE artist_name = %s",
        (new, old)
    )
    ard_count = cur.rowcount
    print(f"artist_royalty_detail updated: {ard_count} row(s)")

    # 5. Also rename artist_label_detail (ALD)
    cur.execute(
        "UPDATE artist_label_detail SET artist_name = %s WHERE artist_name = %s",
        (new, old)
    )
    ald_count = cur.rowcount
    print(f"artist_label_detail updated: {ald_count} row(s)")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nDone. '{old}' → '{new}'")
    print("Dashboard cache will serve stale data until the next import or manual ARD rebuild.")
    print("To clear it immediately, use the 'Manual ARD Rebuild' button in Imports.")


if __name__ == "__main__":
    main()
