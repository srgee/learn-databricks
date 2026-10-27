"""Specification dataclasses for the medallion pipeline.

This module is the single source of truth for spec shapes (dataclasses).
Actual spec instances (SILVER_STREAMS, GOLD_STREAMS)
live in their respective `*_specs.py` modules.
"""

from collections.abc import Callable
from dataclasses import dataclass

from delta._typing import ColumnMapping
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


@dataclass(frozen=True)
class MergeSpec:
    join_on: str
    insert: bool = True  # When not matched then insert
    update: bool = False  # when matched (and cond) then update
    update_condition: str | None = None
    update_set: ColumnMapping | None = None  # None means all cols (set *)


@dataclass(frozen=True)
class SilverSpec:
    """Specification for a Silver target.

    Attributes:
        name: Target table short name.
        source: Source table short name (a Bronze table, or another Silver
            table for derived silver targets).
        merge: MERGE semantics (join condition, update policy).
        transform: Optional ``df -> df`` applied to the streaming source
            before watermark/dedupe/stage. Pure function; must not read
            external tables or capture the SparkSession.
        batch_transform: Optional ``df -> df`` applied to the stage
            DataFrame (batch) before the MERGE. Useful for last-writer-wins
            deduplication that needs the full batch view.
        dedupe_keys: Columns used for ``dropDuplicates`` in the streaming
            phase. Requires ``watermark_col`` for state cleanup.
        watermark_col: Event-time column for watermarking.
        watermark_delay: Watermark delay (default 30 seconds).
        ignore_changes: If True, read source with ``ignoreChanges=true``
            (for sources that may emit updates, e.g. other Silver tables).
            If False (default), read with ``ignoreDeletes=true``.
        output_mode: IGNORED. Stage always uses ``append`` (requirement of
            ``writeStream.toTable`` with Delta). Kept for backward
            compatibility with existing specs; will be removed in a
            follow-up cleanup.
        build_df: Optional ``((spark, starting_version)) -> DataFrame``
            builder for specs that need to construct the stream from
            scratch (stream-stream joins, aggregations). When present,
            ``source`` and ``transform`` are ignored.
    """

    name: str
    source: str
    merge: MergeSpec
    target_schema: StructType  # NEW: explicit schema contract for the target table
    transform: Callable[[DataFrame], DataFrame] | None = None
    batch_transform: Callable[[DataFrame], DataFrame] | None = None
    dedupe_keys: tuple[str, ...] = ()
    watermark_col: str | None = None
    watermark_delay: str = '30 seconds'
    ignore_changes: bool = False
    output_mode: str = 'update'  # ignored; kept for backward compat
    build_df: Callable[[SparkSession, int], DataFrame] | None = None


@dataclass(frozen=True)
class GoldSpec:
    """Specification for a Gold target.

    Gold targets are immutable aggregates built from Silver sources.
    The spec is intentionally minimal: 4 fields, no optional transforms
    or watermarks. If a Gold target needs source-side transformation,
    that logic belongs inside ``build_df``.

    Attributes:
        name: Target table short name.
        source: Source table short name (a Silver table). Kept for
            documentation and traceability; ``build_df`` is responsible
            for actually reading it.
        target_schema: Explicit schema contract for the target table.
            Declared as a StructType so the DDL can be derived by
            ``ddl_for_spec`` and validated against ``build_df`` output
            by ``GoldBuilder._merge_stage_to_target`` (Commit 6).
        build_df: ``((spark, starting_version)) -> DataFrame`` builder
            that constructs the streaming DataFrame from scratch.
            Typically involves stream-stream joins or aggregations.
        merge: MERGE semantics. Gold MERGEs are insert-only by design;
            the ``update`` flag of ``MergeSpec`` is honored if set, but
            the engine's merge logic does not include the update branch
            (it falls back to insert-only behavior).
    """

    name: str
    source: str
    target_schema: StructType
    build_df: Callable[[SparkSession, int], DataFrame]
    merge: MergeSpec
