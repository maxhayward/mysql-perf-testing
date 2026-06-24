"""Pluggable output targets for the benchmark.

A Target consumes batches of rows streamed out of MySQL. The benchmark counts
rows itself; the Target decides what to *do* with them. This is the seam for
testing extract+load pipelines: swap the destination without touching the
extraction loop.

  - DiscardTarget    count-only, or serialise to a file / /dev/null (the
                     "extract, don't store" baseline used to measure read rate).
  - SnowflakeTarget  micro-batch rows and load via write_pandas (stage + COPY),
                     i.e. the realistic high-throughput EL path into Snowflake.
"""

from __future__ import annotations

import sys
from typing import Protocol, Sequence


class Target(Protocol):
    bytes_out: int

    def write_batch(self, rows: Sequence[tuple]) -> None: ...

    def close(self) -> None: ...


def _encode(value) -> bytes:
    if value is None:
        return b"\\N"
    if isinstance(value, bytes):
        return value
    return str(value).encode()


class DiscardTarget:
    """Discard rows: count only (sink='none') or serialise TSV to a file/dev-null.

    With --parallel, each worker gets its own handle; a regular output path gets
    a per-worker '.N' suffix so workers don't clobber each other (/dev/null is
    shared-safe, stdout is rejected).
    """

    def __init__(self, sink: str, worker_idx: int | None = None):
        self.bytes_out = 0
        self._sep = b"\t"
        self._nl = b"\n"
        self._is_stdout = False
        if sink == "none":
            self._fh = None
        elif sink == "-":
            if worker_idx is not None:
                raise ValueError("--sink - (stdout) is not supported with --parallel; use /dev/null or none")
            self._fh = sys.stdout.buffer
            self._is_stdout = True
        else:
            path = sink if (sink == "/dev/null" or worker_idx is None) else f"{sink}.{worker_idx}"
            self._fh = open(path, "wb", buffering=1 << 20)

    def write_batch(self, rows: Sequence[tuple]) -> None:
        if self._fh is None:
            return
        blob = self._nl.join(self._sep.join(_encode(v) for v in row) for row in rows) + self._nl
        self._fh.write(blob)
        self.bytes_out += len(blob)

    def close(self) -> None:
        if self._fh is not None and not self._is_stdout:
            self._fh.close()


class SnowflakeTarget:
    """Load streamed rows into Snowflake via the stage + COPY fast path.

    Rows are buffered until `batch_rows` accumulate, then handed to write_pandas,
    which writes a Parquet file, PUTs it to a temp stage, and runs COPY INTO.
    Row-by-row INSERTs are deliberately avoided — they are orders of magnitude
    slower on a columnar warehouse.

    NB: extraction and loading are serialised in this first cut (a flush blocks
    the read loop), so the reported rate is genuine end-to-end extract+load.
    Pipelining the two (load on a background thread/queue) is the obvious next
    step if load time dominates.
    """

    def __init__(self, connect_kwargs: dict, table: str, columns: Sequence[str], batch_rows: int, recreate: bool):
        try:
            import snowflake.connector  # noqa: F401
            from snowflake.connector.pandas_tools import write_pandas  # noqa: F401
            import pandas  # noqa: F401
        except ImportError as exc:  # pragma: no cover - depends on optional extra
            raise ImportError(
                "Snowflake target needs the optional deps. Install with: uv sync --extra snowflake"
            ) from exc

        import snowflake.connector

        self.bytes_out = 0  # not meaningful for the COPY path; rows_loaded is the metric
        self.rows_loaded = 0
        self.table = table
        self.columns = list(columns)
        self.batch_rows = batch_rows
        self._buf: list[tuple] = []
        self._created = False
        self.conn = snowflake.connector.connect(**connect_kwargs)
        if recreate:
            with self.conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        # The table is auto-created from the DataFrame on the first flush.

    def write_batch(self, rows: Sequence[tuple]) -> None:
        self._buf.extend(rows)
        if len(self._buf) >= self.batch_rows:
            self._flush()

    def _flush(self) -> None:
        if not self._buf:
            return
        import pandas as pd
        from snowflake.connector.pandas_tools import write_pandas

        df = pd.DataFrame(self._buf, columns=self.columns)
        success, _chunks, nrows, _output = write_pandas(
            self.conn,
            df,
            self.table,
            auto_create_table=not self._created,
            quote_identifiers=False,
        )
        if not success:
            raise RuntimeError(f"write_pandas failed loading into {self.table}")
        self._created = True
        self.rows_loaded += nrows
        self._buf.clear()

    def close(self) -> None:
        try:
            self._flush()
        finally:
            self.conn.close()
