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


def _load_private_key(path: str, passphrase: str | None) -> bytes:
    """Load a PEM private key and return PKCS8 DER bytes (Snowflake key-pair auth)."""
    from cryptography.hazmat.primitives import serialization

    with open(path, "rb") as fh:
        key = serialization.load_pem_private_key(
            fh.read(), password=passphrase.encode() if passphrase else None
        )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    warehouse: str
    database: str
    schema: str
    role: str | None = None
    password: str | None = None
    private_key_path: str | None = None
    private_key_passphrase: str | None = None

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        required = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(
                "missing Snowflake env vars: "
                + ", ".join(f"SNOWFLAKE_{k.upper()}" for k in missing)
            )
        return cls(
            account=required["account"],
            user=required["user"],
            warehouse=required["warehouse"],
            database=required["database"],
            schema=required["schema"],
            role=os.getenv("SNOWFLAKE_ROLE") or None,
            password=os.getenv("SNOWFLAKE_PASSWORD") or None,
            private_key_path=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or None,
            private_key_passphrase=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None,
        )

    def connect_kwargs(self) -> dict:
        """Build kwargs for snowflake.connector.connect, preferring key-pair auth."""
        kw: dict = dict(
            account=self.account,
            user=self.user,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )
        if self.role:
            kw["role"] = self.role
        # Snowflake is phasing out single-factor password auth, so prefer
        # key-pair when a key is provided; fall back to password otherwise.
        if self.private_key_path:
            kw["private_key"] = _load_private_key(self.private_key_path, self.private_key_passphrase)
        elif self.password:
            kw["password"] = self.password
        else:
            raise ValueError("set SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH")
        return kw
