# mysql-performance

Proof-of-concept: how fast can we stream rows out of MySQL? Target: **20,000 rows/s**.

## Stack & why

- **Python** (managed by **mise** + **uv**) — aligned with existing tooling; 20k rows/s
  is well within reach.
- **MySQL 8.4 in Docker** — self-contained, tuned to keep the working set in RAM so
  we measure streaming, not disk.
- **`PyMySQL` driver with `SSCursor`** (server-side, *unbuffered* cursor) — rows stream
  off the socket as we read them instead of buffering the whole result set in memory.
  An optional faster C driver (`mysqlclient`) is wired in behind `--driver mysqlclient`.

The three levers that actually move throughput: an **unbuffered cursor**, **batched
`fetchmany`**, and **minimal per-row work**.

## Quick start

```bash
mise install          # python 3.13 + uv
mise run install      # uv sync (installs PyMySQL)
mise run db-up        # start MySQL 8.4, wait until healthy
mise run seed         # load 1,000,000 synthetic rows (ROWS=N to change)
mise run bench        # stream all rows -> /dev/null, report rows/s
```

## Benchmark options

```bash
# Raw read ceiling (count rows, no serialisation/output):
uv run python -m mysql_perf.benchmark --sink none

# Try the faster C driver (needs: brew install mysql-client pkg-config; uv sync --extra fast):
uv run python -m mysql_perf.benchmark --driver mysqlclient

# Limit rows, change fetch batch, select specific columns:
uv run python -m mysql_perf.benchmark --limit 500000 --batch 20000 --columns "id,email,score"
```

Flags: `--driver {pymysql,mysqlclient}` · `--table` · `--columns` · `--limit` (0 = all) ·
`--batch` (fetchmany size) · `--sink {/dev/null|none|-|PATH}` · `--progress-every N`.

The benchmark exits non-zero if throughput falls below the 20k target.

## Layout

```
docker-compose.yml      MySQL 8.4, tuned for hot in-memory reads
.mise.toml              python/uv tooling + tasks (db-up, seed, bench, ...)
pyproject.toml          deps (PyMySQL; optional 'fast' extra = mysqlclient)
mysql_perf/
  config.py             connection config from env (defaults = compose)
  db.py                 driver abstraction -> (connection, SSCursor)
  seed.py               synthetic data generator / bulk loader
  benchmark.py          streaming SELECT + throughput measurement
```
