#!/usr/bin/env python3
"""
Compact a DuckDB database by rewriting it into a fresh file.

DuckDB never returns freed blocks to the OS, so files.db slowly bloats across
crawl/prune cycles. COPY FROM DATABASE rewrites every table — and preserves
sequence state — into a new file with no dead space.

Usage: uv run compact_db.py SRC DST

SRC is attached read-only (so queries against it stay available); DST must
not already exist. Used by query_app.py's Maintenance panel (Compact DB),
which swaps DST into place on success.
"""

import os
import sys

import duckdb


def _q(path: str) -> str:
    """Quote a path as a SQL string literal (ATTACH takes no parameters)."""
    return "'" + path.replace("'", "''") + "'"


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: compact_db.py SRC DST")
    src, dst = sys.argv[1], sys.argv[2]
    if os.path.exists(dst):
        sys.exit(f"refusing to overwrite existing {dst}")

    print(f"Compacting {src} ({os.path.getsize(src):,} bytes)…", flush=True)
    con = duckdb.connect()  # in-memory shell; both DBs attached
    con.execute(f"ATTACH {_q(src)} AS src (READ_ONLY)")
    con.execute(f"ATTACH {_q(dst)} AS dst")
    con.execute("COPY FROM DATABASE src TO dst")
    con.execute("CHECKPOINT dst")
    rows = con.execute("SELECT count(*) FROM dst.files").fetchone()[0]
    con.close()
    print(f"Done: {rows:,} rows, {os.path.getsize(dst):,} bytes "
          f"(was {os.path.getsize(src):,}).")


if __name__ == "__main__":
    main()
