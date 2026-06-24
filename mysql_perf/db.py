"""Driver abstraction.

Both supported drivers implement PEP 249 and ship an ``SSCursor`` — a
server-side, *unbuffered* cursor. That is the single most important choice for
streaming throughput: rows are pulled off the socket as we iterate, instead of
the client buffering the entire result set in memory before we see row one.

  - pymysql      pure Python, zero system deps, always available.
  - mysqlclient  C extension over libmysqlclient; markedly faster row parsing.
"""

from __future__ import annotations

from typing import Any

from .config import DbConfig

DRIVERS = ("pymysql", "mysqlclient")


def connect(cfg: DbConfig, driver: str) -> tuple[Any, type]:
    """Open a connection and return ``(connection, SSCursorClass)``."""
    if driver == "pymysql":
        import pymysql
        import pymysql.cursors

        conn = pymysql.connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            password=cfg.password,
            database=cfg.database,
            charset="utf8mb4",
        )
        return conn, pymysql.cursors.SSCursor

    if driver == "mysqlclient":
        import MySQLdb
        import MySQLdb.cursors

        conn = MySQLdb.connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            passwd=cfg.password,
            db=cfg.database,
            charset="utf8mb4",
        )
        return conn, MySQLdb.cursors.SSCursor

    raise ValueError(f"unknown driver {driver!r}; choose one of {DRIVERS}")
