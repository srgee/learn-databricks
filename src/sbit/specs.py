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
    """'''''
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

    name: str  # Short name for target table
    source: str  # Source table short name (a Bronze table, or another Silver table)
    merge: MergeSpec  # MERGE semantics (join condition, update policy)
    target_schema: StructType  # Explicit schema contract for the target table
    transform: Callable[[DataFrame], DataFrame] | None = (
        None  # Optional df -> df applied to src stream before watermark/dedupe/stage
    )
    batch_transform: Callable[[DataFrame], DataFrame] | None = (
        None  # Optional df -> df applied to the stage df (batch) before MERGE
    )
    dedupe_keys: tuple[
        str, ...
    ] = ()  # Columns used for dropDuplicates in the streaming phase. Requires watermark_col for state cleanup
    watermark_col: str | None = None  # Event-time column for watermarking
    watermark_delay: str = '30 seconds'
    output_mode: str = 'update'  # ignored; kept for backward compat

    # If True, read source with ignoreChanges=true (for sources that may emit updates).
    # If False, read with ignoreDeletes=true.
    ignore_changes: bool = False

    # Optional for specs that need to construct the stream from scratch (stream-stream joins, aggregations).
    # When present, source and transform are ignored.
    build_df: Callable[[SparkSession, int], DataFrame] | None = None


@dataclass(frozen=True)
class GoldSpec:
    name: str  # Short name for target table
    source: str  # Source table short name (a Silver table)
    target_schema: StructType  # Explicit schema contract for the target table

    # Construct the stream from scratch (stream-stream joins, aggregations).
    build_df: Callable[[SparkSession, int], DataFrame]
    merge: MergeSpec  # MERGE semantics. Gold MERGEs are insert-only by design
