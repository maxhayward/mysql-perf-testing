"""Stream rows out of MySQL and measure throughput.

The whole point of the PoC: how many rows/second can we pull off the wire — and,
with a pluggable target, how fast can we extract from MySQL and load elsewhere?

Performance design:
  1. Server-side unbuffered cursor (SSCursor) -> rows stream as we read them;
     the full result set is never materialised client-side.
  2. fetchmany(batch) -> amortises Python-level call overhead over many rows.
  3. Pluggable target -> rows go to a discard sink (/dev/null or count-only) to
     measure raw read rate, or to Snowflake to measure extract+load rate.

Examples:
  uv run python -m mysql_perf.benchmark                          # all rows -> /dev/null
  uv run python -m mysql_perf.benchmark --sink none              # raw read ceiling
  uv run python -m mysql_perf.benchmark --driver mysqlclient --compress --repeat 5
  uv run python -m mysql_perf.benchmark --driver mysqlclient --parallel 8 --sink none
  uv run python -m mysql_perf.benchmark --driver mysqlclient --compress --target snowflake
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import TABLE, DbConfig
from .db import DRIVERS, connect, resolve_driver

TARGET_ROWS_PER_SEC = 20_000


def build_query(table: str, columns: str, limit: int) -> str:
    query = f"SELECT {columns} FROM {table}"
    if limit > 0:
        query += f" LIMIT {limit}"
    return query


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Stream rows from MySQL and measure throughput.")
    ap.add_argument(
        "--driver",
        default="auto",
        choices=["auto", *DRIVERS],
        help="auto = mysqlclient if available, else pymysql (default: auto)",
    )
    ap.add_argument("--table", default=TABLE)
    ap.add_argument("--columns", default="*", help="columns to SELECT (default: *)")
    ap.add_argument("--limit", type=int, default=0, help="max rows; 0 = all")
    ap.add_argument("--batch", type=int, default=10_000, help="fetchmany() size")
    ap.add_argument(
        "--target",
        default="discard",
        choices=["discard", "snowflake"],
        help="where extracted rows go (default: discard)",
    )
    ap.add_argument(
        "--sink",
        default="/dev/null",
        help="discard target output: path, 'none' = count only, '-' = stdout",
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
        "--repeat",
        type=int,
        default=1,
        metavar="N",
        help="run the benchmark N times and report min/median/max (default: 1)",
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
    # Snowflake target options.
    ap.add_argument("--sf-table", default="", help="Snowflake target table (default: source --table)")
    ap.add_argument(
        "--sf-batch",
        type=int,
        default=100_000,
        help="rows per Snowflake COPY batch (default: 100,000)",
    )
    ap.add_argument(
        "--sf-recreate",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="DROP+recreate the Snowflake target table before loading (default: on)",
    )
    ap.add_argument(
        "--sf-create",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="CREATE IF NOT EXISTS the Snowflake warehouse/database/schema (default: on)",
    )
    ap.add_argument(
        "--sf-pipeline",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="load on a background thread, overlapping extract+load (default: on)",
    )
    ap.add_argument(
        "--sf-queue-depth",
        type=int,
        default=2,
        help="max in-flight Snowflake load batches when pipelining (default: 2)",
    )
    return ap.parse_args(argv)


def make_target(args, columns, worker_idx: int | None = None):
    """Construct the pluggable output target for this run/worker."""
    if args.target == "snowflake":
        if worker_idx is not None:
            raise ValueError("--target snowflake is single-stream only in this version; drop --parallel")
        from .config import SnowflakeConfig
        from .targets import SnowflakeTarget

        sf = SnowflakeConfig.from_env()
        return SnowflakeTarget(
            sf,
            args.sf_table or args.table,
            columns,
            args.sf_batch,
            args.sf_recreate,
            args.sf_create,
            args.sf_pipeline,
            args.sf_queue_depth,
        )
    from .targets import DiscardTarget

    return DiscardTarget(args.sink, worker_idx)


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


def run_single(args, cfg) -> float:
    """One single-stream extraction. Prints a timing breakdown; returns rows/s."""
    query = build_query(args.table, args.columns, args.limit)
    dest = f"target={args.target}" + ("" if args.target == "snowflake" else f" sink={args.sink}")
    print(
        f"driver={args.driver}  query={query!r}  batch={args.batch:,}  compress={args.compress}  {dest}",
        file=sys.stderr,
    )

    t_connect0 = time.perf_counter()
    conn, ss_cursor = connect(cfg, args.driver, compress=args.compress)
    connect_s = time.perf_counter() - t_connect0

    rows = 0
    first_row_s = None  # time from execute() to the first batch arriving
    setup_s = 0.0  # time to build the target (e.g. Snowflake connect + provision)
    target = None
    t0 = time.perf_counter()
    last_t, last_rows = t0, 0
    try:
        cur = conn.cursor(ss_cursor)
        cur.arraysize = args.batch
        cur.execute(query)
        columns = [d[0] for d in cur.description]
        t_setup0 = time.perf_counter()
        target = make_target(args, columns)
        setup_s = time.perf_counter() - t_setup0
        while True:
            chunk = cur.fetchmany(args.batch)
            if not chunk:
                break
            if first_row_s is None:
                first_row_s = time.perf_counter() - t0
            target.write_batch(chunk)
            rows += len(chunk)

            if args.progress_every and rows - last_rows >= args.progress_every:
                now = time.perf_counter()
                inst = (rows - last_rows) / (now - last_t)
                avg = rows / (now - t0)
                print(f"  {rows:,} rows  inst={inst:,.0f}/s  avg={avg:,.0f}/s", end="\r", file=sys.stderr)
                last_t, last_rows = now, rows
        cur.close()
    finally:
        if target is not None:
            target.close()
        conn.close()

    dt = time.perf_counter() - t0
    rps = rows / dt if dt else 0.0
    transfer_s = dt - (first_row_s or 0.0)
    transfer_rps = rows / transfer_s if transfer_s > 0 else 0.0

    print("\n" + "-" * 60, file=sys.stderr)
    print(f"rows:           {rows:,}", file=sys.stderr)
    print(f"connect:        {connect_s * 1000:.1f}ms", file=sys.stderr)
    print(f"time-to-1st-row:{(first_row_s or 0.0) * 1000:.1f}ms  (query submit -> first batch)", file=sys.stderr)
    print(f"transfer:       {transfer_s:.3f}s", file=sys.stderr)
    print(f"elapsed:        {dt:.3f}s", file=sys.stderr)
    print(f"throughput:     {rps:,.0f} rows/s  (transfer-only {transfer_rps:,.0f} rows/s)", file=sys.stderr)
    if args.target == "snowflake":
        loaded = getattr(target, "rows_loaded", rows)
        load_s = getattr(target, "load_seconds", 0.0)
        pipelined = getattr(target, "pipelined", False)
        print(
            f"snowflake:      loaded {loaded:,} rows into {args.sf_table or args.table}"
            f"  ({'pipelined' if pipelined else 'serialised'} extract+load)",
            file=sys.stderr,
        )
        print(f"  setup (connect+provision): {setup_s * 1000:,.0f}ms", file=sys.stderr)
        if pipelined:
            print(f"  load (COPY, overlapped):   {load_s:.2f}s  (ran concurrently with extract)", file=sys.stderr)
        else:
            extract_s = max(0.0, dt - setup_s - load_s)
            print(f"  extract (mysql read):      {extract_s:.2f}s", file=sys.stderr)
            print(f"  load (snowflake COPY):     {load_s:.2f}s   <- serialised after extract", file=sys.stderr)
        if load_s:
            print(f"  load-only rate:            {loaded / load_s:,.0f} rows/s", file=sys.stderr)
    elif getattr(target, "bytes_out", 0):
        mb = target.bytes_out / (1 << 20)
        print(f"data out:       {mb:,.1f} MB  ({mb / dt:,.1f} MB/s)", file=sys.stderr)
    return rps


def _stream_shard(idx: int, args, cfg, lo: int, hi: int) -> tuple[int, int, int, float]:
    """Worker: stream one half-open key range [lo, hi) over its own connection.

    Each worker gets a dedicated connection (= dedicated TCP stream). mysqlclient
    releases the GIL around its C network calls, so threads overlap network I/O.
    """
    conn, ss_cursor = connect(cfg, args.driver, compress=args.compress)
    target = make_target(args, columns=None, worker_idx=idx)
    rows = 0
    t = time.perf_counter()
    try:
        cur = conn.cursor(ss_cursor)
        cur.arraysize = args.batch
        cur.execute(
            f"SELECT {args.columns} FROM {args.table} "
            f"WHERE {args.shard_key} >= {lo} AND {args.shard_key} < {hi}"
        )
        while True:
            chunk = cur.fetchmany(args.batch)
            if not chunk:
                break
            target.write_batch(chunk)
            rows += len(chunk)
        cur.close()
    finally:
        target.close()
        conn.close()
    return idx, rows, target.bytes_out, time.perf_counter() - t


def run_parallel(args, cfg) -> float:
    """N concurrent range scans. Prints per-worker + aggregate stats; returns rows/s."""
    if args.target == "snowflake":
        raise ValueError("--target snowflake is single-stream only in this version; drop --parallel")
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
        return 0.0
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
        print("note: --limit is ignored with --parallel (each worker streams its full range)", file=sys.stderr)

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
    return rps


def main(argv=None) -> int:
    args = parse_args(argv)
    cfg = DbConfig.from_env()
    args.driver = resolve_driver(args.driver)  # 'auto' -> mysqlclient or pymysql

    if args.probe:
        conn, _ = connect(cfg, args.driver, compress=args.compress)
        return run_probe(conn, args.probe)

    runner = run_parallel if args.parallel > 1 else run_single
    n = max(1, args.repeat)
    rps_list: list[float] = []
    try:
        # Validate the target's config up front so a misconfigured destination
        # fails fast — before we open MySQL connections or start extracting.
        if args.target == "snowflake":
            from .config import SnowflakeConfig

            SnowflakeConfig.from_env()
        for i in range(n):
            if n > 1:
                print(f"\n===== run {i + 1}/{n} =====", file=sys.stderr)
            rps_list.append(runner(args, cfg))
    except (ValueError, ImportError) as exc:
        # Expected config/usage problems (missing creds, bad flag combo, missing
        # optional deps) — show a clean message rather than a traceback.
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if n > 1:
        median = statistics.median(rps_list)
        print("\n" + "=" * 60, file=sys.stderr)
        print(
            f"repeat {n}:      min={min(rps_list):,.0f}  median={median:,.0f}  "
            f"max={max(rps_list):,.0f} rows/s",
            file=sys.stderr,
        )
        result = median
    else:
        result = rps_list[0]

    verdict = "PASS" if result >= TARGET_ROWS_PER_SEC else "BELOW TARGET"
    print(f"target:         {TARGET_ROWS_PER_SEC:,} rows/s  ->  {verdict}", file=sys.stderr)
    return 0 if result >= TARGET_ROWS_PER_SEC else 1


if __name__ == "__main__":
    raise SystemExit(main())
