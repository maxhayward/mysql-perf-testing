"""Seed the benchmark table with synthetic rows.

Usage (via mise):   mise run seed
With a row count:    ROWS=5000000 mise run seed

Writes are batched into multi-row INSERTs inside transactions. Seeding speed is
not the metric we care about here; it just needs to be fast enough to fill the
table. The benchmark (benchmark.py) is what measures read throughput.
"""

from __future__ import annotations

import os
import random
import string
import sys
import time
import uuid
from datetime import datetime, timedelta

import pymysql

from .config import DbConfig, TABLE

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  uuid       CHAR(36)        NOT NULL,
  name       VARCHAR(64)     NOT NULL,
  email      VARCHAR(128)    NOT NULL,
  age        INT             NOT NULL,
  score      DOUBLE          NOT NULL,
  status     VARCHAR(16)     NOT NULL,
  payload    VARCHAR(255)    NOT NULL,
  created_at DATETIME        NOT NULL
) ENGINE=InnoDB
"""

INSERT = (
    f"INSERT INTO {TABLE} "
    "(uuid, name, email, age, score, status, payload, created_at) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
)

STATUSES = ("active", "inactive", "pending", "archived")
_ALPHABET = string.ascii_letters + string.digits
_EPOCH = datetime(2020, 1, 1)


def gen_row(i: int) -> tuple:
    return (
        str(uuid.uuid4()),
        f"user_{i}",
        f"user_{i}@example.com",
        random.randint(18, 90),
        round(random.random() * 1000.0, 4),
        random.choice(STATUSES),
        "".join(random.choices(_ALPHABET, k=200)),
        _EPOCH + timedelta(seconds=i),
    )


def main() -> None:
    rows = int(os.getenv("ROWS", "1000000"))
    batch = int(os.getenv("SEED_BATCH", "5000"))
    truncate = os.getenv("SEED_TRUNCATE", "1") == "1"
    cfg = DbConfig.from_env()

    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        autocommit=False,
    )

    print(
        f"seeding {rows:,} rows into `{cfg.database}`.`{TABLE}` "
        f"(batch={batch:,}, truncate={truncate}) ...",
        file=sys.stderr,
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            if truncate:
                cur.execute(f"TRUNCATE TABLE {TABLE}")
            conn.commit()

            t0 = time.perf_counter()
            buf: list[tuple] = []
            done = 0
            for i in range(1, rows + 1):
                buf.append(gen_row(i))
                if len(buf) >= batch:
                    cur.executemany(INSERT, buf)
                    conn.commit()
                    done += len(buf)
                    buf.clear()
                    elapsed = time.perf_counter() - t0
                    print(
                        f"  {done:,}/{rows:,} rows "
                        f"({done / elapsed:,.0f} rows/s)",
                        end="\r",
                        file=sys.stderr,
                    )
            if buf:
                cur.executemany(INSERT, buf)
                conn.commit()
                done += len(buf)

    dt = time.perf_counter() - t0
    print(
        f"\nseeded {done:,} rows in {dt:.1f}s ({done / dt:,.0f} rows/s)",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
