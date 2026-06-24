# mysql-performance

Proof-of-concept: how fast can we stream rows out of MySQL? Target: **20,000 rows/s**.

## Stack & why

- **Python** (managed by **mise** + **uv**) — aligned with existing tooling; 20k rows/s
  is well within reach.
- **MySQL 8.4 in Docker** — self-contained, tuned to keep the working set in RAM so
  we measure streaming, not disk.
- **`mysqlclient` driver by default** (C extension, ~2× faster row parsing), with
  **`PyMySQL`** (pure Python) as an automatic fallback where the C build isn't available —
  controlled by `--driver auto` (the default). Both use `SSCursor` (server-side,
  *unbuffered* cursor) so rows stream off the socket instead of buffering the whole
  result set. mysqlclient is a core dependency, so `uv sync` always installs and keeps it
  (needs libmysqlclient: `brew install mysql-client pkg-config`, or the mise `mysql` tool).

The three levers that actually move throughput: an **unbuffered cursor**, **batched
`fetchmany`**, and **minimal per-row work**.

## Quick start

```bash
mise install          # python 3.13 + uv
mise run install      # uv sync (installs mysqlclient + PyMySQL)
mise run db-up        # start MySQL 8.4, wait until healthy
mise run seed         # load 1,000,000 synthetic rows (ROWS=N to change)
mise run bench        # stream all rows -> /dev/null, report rows/s
```

## Benchmark options

```bash
# Raw read ceiling (count rows, no serialisation/output):
uv run python -m mysql_perf.benchmark --sink none

# Force the pure-Python driver (mysqlclient is the default via --driver auto):
uv run python -m mysql_perf.benchmark --driver pymysql

# Limit rows, change fetch batch, select specific columns:
uv run python -m mysql_perf.benchmark --limit 500000 --batch 20000 --columns "id,email,score"
```

```bash
# Repeat several times on a noisy link and report min/median/max:
uv run python -m mysql_perf.benchmark --driver mysqlclient --compress --repeat 5

# Parallel sharded reads (N connections over N primary-key ranges):
uv run python -m mysql_perf.benchmark --driver mysqlclient --parallel 8 --sink none

# Measure round-trip latency to the server, then exit:
uv run python -m mysql_perf.benchmark --probe 50
```

Flags: `--driver {auto,pymysql,mysqlclient}` (default auto) · `--table` · `--columns` · `--limit` (0 = all) ·
`--batch` (fetchmany size) · `--sink {/dev/null|none|-|PATH}` · `--compress` (mysqlclient) ·
`--repeat N` · `--parallel N` · `--shard-key` · `--probe N` · `--progress-every N`.

The benchmark exits non-zero if throughput (median, with `--repeat`) is below the 20k target.

## Targets: extract → load

Rows go to a pluggable **target**. The default `discard` target measures pure read rate
(to `/dev/null` or count-only). The `snowflake` target measures **extract+load** rate by
micro-batching streamed rows and loading each batch via `write_pandas` (stage + `COPY INTO`
— the high-throughput path; row-by-row INSERTs are deliberately avoided).

```bash
uv sync --extra snowflake          # snowflake-connector-python[pandas]
# set SNOWFLAKE_* in .env (see .env.example), then:
uv run python -m mysql_perf.benchmark --driver mysqlclient --compress \
    --target snowflake --sf-table BENCH --sf-batch 100000
```

Snowflake target flags: `--target snowflake` · `--sf-table` (default: source `--table`) ·
`--sf-batch` (rows per COPY) · `--sf-recreate/--no-sf-recreate`. Auth is via `SNOWFLAKE_*`
env vars, preferring key-pair (`SNOWFLAKE_PRIVATE_KEY_PATH`) over password. Notes: it's
single-stream only for now (no `--parallel`), and extract/load are serialised — a flush
blocks the read loop — so the rate is genuine end-to-end. Pipelining the load onto a
background thread is the obvious next step if load time dominates.

## Layout

```
docker-compose.yml      MySQL 8.4, tuned for hot in-memory reads
.mise.toml              python/uv tooling + tasks (db-up, seed, bench, ...)
pyproject.toml          deps (mysqlclient + PyMySQL; extra: 'snowflake')
mysql_perf/
  config.py             MySQL + Snowflake config from env
  db.py                 driver abstraction -> (connection, SSCursor)
  seed.py               synthetic data generator / bulk loader
  targets.py            pluggable output targets (discard, snowflake)
  benchmark.py          streaming SELECT + throughput measurement
```
