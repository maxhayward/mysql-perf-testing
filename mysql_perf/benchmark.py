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
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    ap.add_argument(
        "--compress",
        action="store_true",
        help="enable MySQL protocol compression (mysqlclient only)",
    )
    ap.add_argument(
        "--probe",
        type=int,
        default=0,
        metavar="N",
        help="measure round-trip latency: run 'SELECT 1' N times, report RTT, then exit",
    )
    ap.add_argument(
        "--parallel",
        type=int,
        default=1,
        metavar="N",
        help="stream N primary-key ranges concurrently over N connections (1 = single stream)",
    )
    ap.add_argument(
        "--shard-key",
        default="id",
        help="integer column to range-split on for --parallel (default: id)",
    )
    return ap.parse_args(argv)


def run_probe(conn, n: int) -> int:
    """Time N round-trips of 'SELECT 1' to measure network RTT to the server."""
    samples = []
    cur = conn.cursor()
    cur.execute("SELECT 1")  # warm up (auth/first-query effects)
    cur.fetchall()
    for _ in range(n):
        t = time.perf_counter()
        cur.execute("SELECT 1")
        cur.fetchall()
        samples.append((time.perf_counter() - t) * 1000.0)
    cur.close()
    conn.close()
    samples.sort()
    avg = sum(samples) / len(samples)
    p50 = samples[len(samples) // 2]
    print("\n" + "-" * 60, file=sys.stderr)
    print(f"RTT over {n} round-trips (SELECT 1):", file=sys.stderr)
    print(
        f"  min={samples[0]:.2f}ms  p50={p50:.2f}ms  avg={avg:.2f}ms  max={samples[-1]:.2f}ms",
        file=sys.stderr,
    )
    print(
        "  single-stream ceiling at this RTT, per 1 MB TCP window: "
        f"~{(1.0 / (avg / 1000.0)):,.0f} MB/s",
        file=sys.stderr,
    )
    return 0


def _open_worker_sink(sink_arg: str, idx: int):
    """Per-worker output. /dev/null is shared-safe; regular paths get a .N suffix."""
    if sink_arg == "none":
        return None
    if sink_arg == "-":
        raise ValueError("--sink - (stdout) is not supported with --parallel; use /dev/null or none")
    path = sink_arg if sink_arg == "/dev/null" else f"{sink_arg}.{idx}"
    return open(path, "wb", buffering=1 << 20)


def _drain(cur, batch: int, sink) -> tuple[int, int]:
    """Read every row from cur, optionally serialising to sink. Returns (rows, bytes)."""
    rows = 0
    bytes_out = 0
    sep, nl = b"\t", b"\n"
    write = sink.write if sink is not None else None
    while True:
        chunk = cur.fetchmany(batch)
        if not chunk:
            break
        if write is not None:
            blob = nl.join(sep.join(_encode(v) for v in row) for row in chunk) + nl
            write(blob)
            bytes_out += len(blob)
        rows += len(chunk)
    return rows, bytes_out


def _stream_shard(idx: int, args, cfg, lo: int, hi: int) -> tuple[int, int, int, float]:
    """Worker: stream one half-open key range [lo, hi) over its own connection.

    Each worker gets a dedicated connection (= dedicated TCP stream), so N
    workers get ~N independent window/RTT budgets. mysqlclient releases the GIL
    around its C network calls, so threads genuinely overlap network I/O.
    """
    conn, ss_cursor = connect(cfg, args.driver, compress=args.compress)
    sink = _open_worker_sink(args.sink, idx)
    t = time.perf_counter()
    try:
        cur = conn.cursor(ss_cursor)
        cur.arraysize = args.batch
        cur.execute(
            f"SELECT {args.columns} FROM {args.table} "
            f"WHERE {args.shard_key} >= {lo} AND {args.shard_key} < {hi}"
        )
        rows, bytes_out = _drain(cur, args.batch, sink)
        cur.close()
    finally:
        conn.close()
        if sink is not None:
            sink.close()
    return idx, rows, bytes_out, time.perf_counter() - t


def run_parallel(args, cfg) -> int:
    n = args.parallel

    # Find the key range to split across workers.
    conn, _ = connect(cfg, args.driver)
    cur = conn.cursor()
    cur.execute(f"SELECT MIN({args.shard_key}), MAX({args.shard_key}) FROM {args.table}")
    bounds = cur.fetchone()
    cur.close()
    conn.close()
    if not bounds or bounds[0] is None:
        print("table is empty — nothing to stream", file=sys.stderr)
        return 1
    lo, hi = int(bounds[0]), int(bounds[1])

    # Half-open integer ranges [edges[i], edges[i+1]) covering [lo, hi].
    span = hi - lo + 1
    edges = [lo + span * i // n for i in range(n + 1)]
    ranges = [(edges[i], edges[i + 1]) for i in range(n)]

    print(
        f"driver={args.driver}  table={args.table}  parallel={n}  "
        f"shard_key={args.shard_key} in [{lo}, {hi}]  batch={args.batch:,}  "
        f"compress={args.compress}  sink={args.sink}",
        file=sys.stderr,
    )
    if args.limit:
        print(
            "note: --limit is ignored with --parallel (each worker streams its full range)",
            file=sys.stderr,
        )

    t0 = time.perf_counter()
    results: list[tuple[int, int, int, float]] = []
    with ThreadPoolExecutor(max_workers=n) as ex:
        futures = [ex.submit(_stream_shard, i, args, cfg, a, b) for i, (a, b) in enumerate(ranges)]
        for fut in as_completed(futures):
            results.append(fut.result())
    dt = time.perf_counter() - t0

    results.sort()
    total_rows = sum(r for _, r, _, _ in results)
    total_bytes = sum(b for _, _, b, _ in results)
    rps = total_rows / dt if dt else 0.0

    print("\n" + "-" * 60, file=sys.stderr)
    for idx, r, _b, wdt in results:
        print(f"  worker {idx}: {r:,} rows in {wdt:.2f}s ({r / wdt if wdt else 0:,.0f} rows/s)", file=sys.stderr)
    print(f"workers:        {n}", file=sys.stderr)
    print(f"rows:           {total_rows:,}", file=sys.stderr)
    print(f"elapsed:        {dt:.3f}s  (wall clock)", file=sys.stderr)
    print(f"throughput:     {rps:,.0f} rows/s  (aggregate)", file=sys.stderr)
    if args.sink != "none":
        mb = total_bytes / (1 << 20)
        print(f"data out:       {mb:,.1f} MB  ({mb / dt:,.1f} MB/s)", file=sys.stderr)
    verdict = "PASS" if rps >= TARGET_ROWS_PER_SEC else "BELOW TARGET"
    print(f"target:         {TARGET_ROWS_PER_SEC:,} rows/s  ->  {verdict}", file=sys.stderr)
    return 0 if rps >= TARGET_ROWS_PER_SEC else 1


def main(argv=None) -> int:
    args = parse_args(argv)
    cfg = DbConfig.from_env()
    if args.parallel > 1:
        return run_parallel(args, cfg)
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
        f"compress={args.compress}  "
        f"sink={'none (count only)' if count_only else args.sink}",
        file=sys.stderr,
    )

    # Time the connection handshake separately — it's the first sign of latency.
    t_connect0 = time.perf_counter()
    conn, ss_cursor = connect(cfg, args.driver, compress=args.compress)
    connect_s = time.perf_counter() - t_connect0

    if args.probe:
        return run_probe(conn, args.probe)

    rows = 0
    bytes_out = 0
    sep, nl = b"\t", b"\n"
    first_row_s = None  # time from execute() to the first batch arriving

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
            if first_row_s is None:
                first_row_s = time.perf_counter() - t0

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
    # Transfer-only rate excludes connect + time-to-first-row, so it reflects
    # steady-state streaming throughput rather than fixed startup latency.
    transfer_s = dt - (first_row_s or 0.0)
    transfer_rps = rows / transfer_s if transfer_s > 0 else 0.0

    print("\n" + "-" * 60, file=sys.stderr)
    print(f"rows:           {rows:,}", file=sys.stderr)
    print(f"connect:        {connect_s * 1000:.1f}ms", file=sys.stderr)
    print(f"time-to-1st-row:{(first_row_s or 0.0) * 1000:.1f}ms  (query submit -> first batch)", file=sys.stderr)
    print(f"transfer:       {transfer_s:.3f}s", file=sys.stderr)
    print(f"elapsed:        {dt:.3f}s", file=sys.stderr)
    print(f"throughput:     {rps:,.0f} rows/s  (transfer-only {transfer_rps:,.0f} rows/s)", file=sys.stderr)
    if not count_only:
        mb = bytes_out / (1 << 20)
        print(f"data out:       {mb:,.1f} MB  ({mb / dt:,.1f} MB/s)", file=sys.stderr)
    verdict = "PASS" if rps >= TARGET_ROWS_PER_SEC else "BELOW TARGET"
    print(f"target:         {TARGET_ROWS_PER_SEC:,} rows/s  ->  {verdict}", file=sys.stderr)

    return 0 if rps >= TARGET_ROWS_PER_SEC else 1


if __name__ == "__main__":
    raise SystemExit(main())
