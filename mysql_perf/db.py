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


def resolve_driver(driver: str) -> str:
    """Resolve 'auto' to mysqlclient when it's importable, else pymysql.

    Defaulting to 'auto' gives the faster C driver wherever it's installed, while
    keeping a zero-dependency pure-Python fallback for environments where the
    mysqlclient build isn't available.
    """
    if driver != "auto":
        return driver
    try:
        import MySQLdb  # noqa: F401

        return "mysqlclient"
    except ImportError:
        import sys

        print(
            "note: mysqlclient C extension not importable, falling back to pymysql "
            "(slower). Ensure libmysqlclient is installed (macOS: brew install "
            "mysql-client pkg-config) and re-run: uv sync",
            file=sys.stderr,
        )
        return "pymysql"


def connect(cfg: DbConfig, driver: str, compress: bool = False) -> tuple[Any, type]:
    """Open a connection and return ``(connection, SSCursorClass)``.

    ``compress`` enables MySQL protocol compression (zlib). On a bandwidth-
    limited WAN link this trades client/server CPU for fewer bytes on the wire,
    which can raise effective row throughput — but only if the data actually
    compresses (random/high-entropy columns won't benefit).
    """
    if driver == "pymysql":
        import pymysql
        import pymysql.cursors

        if compress:
            raise ValueError("--compress is only supported with --driver mysqlclient")
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

        kwargs = dict(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            passwd=cfg.password,
            db=cfg.database,
            charset="utf8mb4",
        )
        # Footgun: mysqlclient uses -1 as the "unset" sentinel for `compress`,
        # so passing compress=False (-> 0, which is != -1) actually ENABLES
        # compression. Only pass the kwarg when we truly want it on; otherwise
        # omit it entirely so the connection stays uncompressed.
        if compress:
            kwargs["compress"] = True
        conn = MySQLdb.connect(**kwargs)
        return conn, MySQLdb.cursors.SSCursor

    raise ValueError(f"unknown driver {driver!r}; choose one of {DRIVERS}")
