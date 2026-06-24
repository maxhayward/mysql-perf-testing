"""Connection configuration, sourced from the environment.

Defaults match the docker-compose MySQL instance, so the app runs out of the
box with no .env file. Override any value via environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls) -> "DbConfig":
        return cls(
            host=os.getenv("MYSQL_HOST", "127.0.0.1"),
            port=int(os.getenv("MYSQL_PORT", "3307")),
            user=os.getenv("MYSQL_USER", "bench"),
            password=os.getenv("MYSQL_PASSWORD", "benchpw"),
            database=os.getenv("MYSQL_DB", "bench"),
        )


# Table the seeder creates and the benchmark reads from.
TABLE = os.getenv("BENCH_TABLE", "bench")
