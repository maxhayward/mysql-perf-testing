"""Stream rows out of MySQL and measure throughput.

The whole point of the PoC: how many rows/second can we pull off the wire?

Performance design:
  1. Server-side unbuffered cursor (SSCursor) -> rows stream as we read them;
     the full result set is never materialised client-side.
  2. fetchmany(batch) -> amortises Python-level call overhead over many rows.
  3. Minimal per-row work -> by default we serialise each row to a tab-
     separated line and write it to /dev/null (a real-ish consumer doing I/O).
     `--sink none` skips serialisation entirely to show the raw read ceiling.

Examples:
  uv run python -m mysql_perf.benchmark                    # all rows -> /dev/null
  uv run python -m mysql_perf.benchmark --sink none        # raw read ceiling
  uv run python -m mysql_perf.benchmark --driver mysqlclient --limit 2000000
"""

from __future__ import annotations

import argparse
import sys
import time

from .config import TABLE, DbConfig
from .db import DRIVERS, connect

TARGET_ROWS_PER_SEC = 20_000


def build_query(table: str, columns: str, limit: int) -> str:
    query = f"SELECT {columns} FROM {table}"
    if limit > 0:
        query += f" LIMIT {limit}"
    return query


def _encode(value) -> bytes:
    if value is None:
        return b"\\N"
    if isinstance(value, bytes):
        return value
    return str(value).encode()


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Stream rows from MySQL and measure throughput.")
    ap.add_argument("--driver", default="pymysql", choices=DRIVERS)
    ap.add_argument("--table", default=TABLE)
    ap.add_argument("--columns", default="*", help="columns to SELECT (default: *)")
    ap.add_argument("--limit", type=int, default=0, help="max rows; 0 = all")
    ap.add_argument("--batch", type=int, default=10_000, help="fetchmany() size")
    ap.add_argument(
        "--sink",
        default="/dev/null",
        help="output path for serialised rows; 'none' = count only (raw ceiling); '-' = stdout",
    )
    ap.add_argument(
        "--progress-every",
        type=int,
        default=200_000,
        help="print a progress line every N rows (0 = never)",
    )
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    cfg = DbConfig.from_env()
    query = build_query(args.table, args.columns, args.limit)

    # Resolve the output sink.
    write = None
    sink = None
    count_only = args.sink == "none"
    if not count_only:
        sink = sys.stdout.buffer if args.sink == "-" else open(args.sink, "wb", buffering=1 << 20)
        write = sink.write

    print(
        f"driver={args.driver}  query={query!r}  batch={args.batch:,}  "
        f"sink={'none (count only)' if count_only else args.sink}",
        file=sys.stderr,
    )

    conn, ss_cursor = connect(cfg, args.driver)
    rows = 0
    bytes_out = 0
    sep, nl = b"\t", b"\n"

    t0 = time.perf_counter()
    last_t, last_rows = t0, 0
    try:
        cur = conn.cursor(ss_cursor)
        cur.arraysize = args.batch
        cur.execute(query)
        while True:
            chunk = cur.fetchmany(args.batch)
            if not chunk:
                break

            if write is not None:
                blob = nl.join(sep.join(_encode(v) for v in row) for row in chunk) + nl
                write(blob)
                bytes_out += len(blob)

            rows += len(chunk)

            if args.progress_every and rows - last_rows >= args.progress_every:
                now = time.perf_counter()
                inst = (rows - last_rows) / (now - last_t)
                avg = rows / (now - t0)
                print(
                    f"  {rows:,} rows  inst={inst:,.0f}/s  avg={avg:,.0f}/s",
                    end="\r",
                    file=sys.stderr,
                )
                last_t, last_rows = now, rows
        cur.close()
    finally:
        conn.close()
        if sink is not None and sink is not sys.stdout.buffer:
            sink.close()

    dt = time.perf_counter() - t0
    rps = rows / dt if dt else 0.0

    print("\n" + "-" * 60, file=sys.stderr)
    print(f"rows:        {rows:,}", file=sys.stderr)
    print(f"elapsed:     {dt:.3f}s", file=sys.stderr)
    print(f"throughput:  {rps:,.0f} rows/s", file=sys.stderr)
    if not count_only:
        mb = bytes_out / (1 << 20)
        print(f"data out:    {mb:,.1f} MB  ({mb / dt:,.1f} MB/s)", file=sys.stderr)
    verdict = "PASS" if rps >= TARGET_ROWS_PER_SEC else "BELOW TARGET"
    print(f"target:      {TARGET_ROWS_PER_SEC:,} rows/s  ->  {verdict}", file=sys.stderr)

    return 0 if rps >= TARGET_ROWS_PER_SEC else 1


if __name__ == "__main__":
    raise SystemExit(main())
