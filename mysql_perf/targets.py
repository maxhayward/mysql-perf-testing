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

import queue
import sys
import threading
import time
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

    By default the load runs on a background thread (pipeline=True): the main
    thread keeps extracting from MySQL while the loader COPYs, so end-to-end time
    approaches max(extract, load) instead of their sum. A bounded queue applies
    backpressure to cap memory. Set pipeline=False for the simpler serialised
    path (a flush blocks the read loop).
    """

    def __init__(self, sf, table: str, columns: Sequence[str], batch_rows: int, recreate: bool, create_context: bool = True, pipeline: bool = True, queue_depth: int = 2):
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
        self.load_seconds = 0.0  # cumulative time spent in write_pandas (stage + COPY)
        self.database = sf.database
        self.schema = sf.schema
        self.table = table
        self.columns = list(columns)
        self.batch_rows = batch_rows
        self._buf: list[tuple] = []
        self._created = False
        self.conn = snowflake.connector.connect(**sf.connect_kwargs())

        # Establish session context. Don't assume the landing area exists: a load
        # test should provision its own warehouse/database/schema. Names aren't
        # quoted, so Snowflake applies its normal upper-casing (matches the
        # unquoted table that write_pandas creates).
        with self.conn.cursor() as cur:
            if create_context:
                cur.execute(
                    f"CREATE WAREHOUSE IF NOT EXISTS {sf.warehouse} "
                    "WAREHOUSE_SIZE=XSMALL AUTO_SUSPEND=60 AUTO_RESUME=TRUE "
                    "INITIALLY_SUSPENDED=FALSE"
                )
                cur.execute(f"CREATE DATABASE IF NOT EXISTS {sf.database}")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {sf.database}.{sf.schema}")
                print(
                    f"snowflake: ensured warehouse {sf.warehouse}, database "
                    f"{sf.database}, schema {sf.schema}",
                    file=sys.stderr,
                )
            cur.execute(f"USE WAREHOUSE {sf.warehouse}")
            cur.execute(f"USE DATABASE {sf.database}")
            cur.execute(f"USE SCHEMA {sf.schema}")
            if recreate:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
        # The table is auto-created from the DataFrame on the first load.

        # Pipeline: a background thread runs the COPY loads while the main thread
        # keeps extracting, so total time approaches max(extract, load) rather
        # than their sum. The bounded queue applies backpressure to cap memory.
        self.pipelined = pipeline
        self._error: Exception | None = None
        self._queue: queue.Queue | None = None
        self._loader: threading.Thread | None = None
        if pipeline:
            self._queue = queue.Queue(maxsize=max(1, queue_depth))
            self._loader = threading.Thread(target=self._loader_loop, name="sf-loader", daemon=True)
            self._loader.start()

    def write_batch(self, rows: Sequence[tuple]) -> None:
        self._buf.extend(rows)
        if len(self._buf) >= self.batch_rows:
            self._dispatch()

    def _dispatch(self) -> None:
        """Hand the buffered rows off to be loaded (queued, or inline if serial)."""
        if not self._buf:
            return
        batch = self._buf
        self._buf = []
        if self.pipelined:
            if self._error:  # loader already failed; stop feeding it
                raise self._error
            self._queue.put(batch)  # blocks when the loader is behind (backpressure)
        else:
            self._load(batch)

    def _loader_loop(self) -> None:
        """Background consumer: COPY each queued batch into Snowflake."""
        while True:
            batch = self._queue.get()
            try:
                if batch is None:  # shutdown sentinel
                    return
                if self._error is None:
                    self._load(batch)
            except Exception as exc:  # capture; re-raised on the main thread
                self._error = exc
            finally:
                self._queue.task_done()

    def _load(self, batch: list) -> None:
        import pandas as pd
        from snowflake.connector.pandas_tools import write_pandas

        df = pd.DataFrame(batch, columns=self.columns)
        t = time.perf_counter()
        success, _chunks, nrows, _output = write_pandas(
            self.conn,
            df,
            self.table,
            database=self.database,
            schema=self.schema,
            auto_create_table=not self._created,
            quote_identifiers=False,
        )
        self.load_seconds += time.perf_counter() - t
        if not success:
            raise RuntimeError(f"write_pandas failed loading into {self.table}")
        self._created = True
        self.rows_loaded += nrows

    def close(self) -> None:
        try:
            self._dispatch()  # hand off any remaining rows
            if self.pipelined:
                self._queue.put(None)  # tell the loader to stop
                self._loader.join()
            if self._error:
                raise self._error
        finally:
            self.conn.close()
