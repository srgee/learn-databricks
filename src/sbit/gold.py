"""Gold layer: build aggregates from Silver, append to Gold targets.

Architecture:
    Each Gold target is produced by:
    1. Streaming: ``build_df`` constructs the stream from scratch (typically
       stream-stream joins or aggregations over Silver sources), and
       appends to an ephemeral ``{name}_stage`` Delta table.
    2. Batch MERGE: when the stream drains, MERGE the stage into the
       target using the DeltaTable API. MERGE is insert-only by design:
       Gold rows are immutable facts keyed by natural identifiers, so
       re-runs of the same source data converge to the same target state.

This module is the Gold-layer counterpart to ``silver.py``. The two
engines share the stage + batch-MERGE pattern (which replaced the
original ``foreachBatch`` approach for Databricks Serverless
compatibility) but do NOT share a base class: each layer owns its spec
and engine, with only the fields and merge semantics that layer needs.
"""

from collections.abc import Callable
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming.query import StreamingQuery

from sbit.specs import MergeSpec  # import temporal hasta mover MergeSpec a un módulo común


# ============================================================
# Spec
# ============================================================
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
    build_df: Callable[[SparkSession, int], DataFrame]
    merge: MergeSpec


# ============================================================
# Engine
# ============================================================
class GoldBuilder:
    """Stage + batch-MERGE pipeline for the Gold layer.

    See module docstring for the architecture overview.

    Args:
        spark: Active SparkSession.
        checkpoint_root: UC volume path where stream offsets are persisted
            (e.g. ``/Volumes/cat/db/vol/checkpoint_zone``).
        starting_version: Delta log version to start streaming from
            (``0`` = from table creation). Passed to ``build_df``.
    """

    def __init__(
        self,
        spark: SparkSession,
        checkpoint_root: str,
        starting_version: int = 0,
    ) -> None:
        self.spark = spark
        self.checkpoint_root = checkpoint_root
        self.starting_version = starting_version

    # ---------- Nombres ----------

    @staticmethod
    def _stage_name(name: str) -> str:
        return f'{name}_stage'

    def _checkpoint_path(self, s: GoldSpec) -> str:
        # 'gold_' hardcoded: this engine only handles gold.
        return f'{self.checkpoint_root}/gold_{s.name}'

    # ---------- Fase 1: stream → stage ----------

    def start(self, s: GoldSpec, once: bool = True, processing_time: str = '15 seconds') -> StreamingQuery:
        """Start the stream that writes the build_df output to the stage table."""
        df = s.build_df(self.spark, self.starting_version)

        stage_table = self._stage_name(s.name)
        checkpoint = self._checkpoint_path(s)

        # Drop a leftover stage table from an aborted previous run so the
        # stream regenerates it with the current schema. The checkpoint
        # is respected: the stream only processes new changes.
        self.spark.sql(f'DROP TABLE IF EXISTS {stage_table}')

        writer = (
            df.writeStream.format('delta')
            .outputMode('append')
            .option('checkpointLocation', checkpoint)
            .queryName(f'{s.name}_stage_stream')
        )
        writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime=processing_time)

        return writer.toTable(stage_table)

    # ---------- Fase 2: batch MERGE stage → target ----------
    def _merge_stage_to_target(self, s: GoldSpec) -> None:
        """Insert-only MERGE stage → target (native DeltaTable API).

        Gold rows are immutable: the same natural key always maps to the
        same row content, so matched rows are silently skipped and only
        new rows are inserted. This makes re-runs idempotent without
        needing an update branch.
        """
        stage_df = self.spark.read.table(self._stage_name(s.name))
        target = DeltaTable.forName(self.spark, s.name)
        target.alias('t').merge(stage_df.alias('s'), s.merge.join_on).whenNotMatchedInsertAll().execute()

    def _drop_stage(self, s: GoldSpec) -> None:
        self.spark.sql(f'DROP TABLE IF EXISTS {self._stage_name(s.name)}')

    # ---------- Orquestación ----------

    def run(
        self,
        specs: dict[str, GoldSpec],
        phases: list[list[str]],
        once: bool = True,
        processing_time: str = '15 seconds',
    ) -> None:
        for phase in phases:
            queries = [self.start(specs[name], once, processing_time) for name in phase]
            if once:
                for q in queries:
                    q.awaitTermination()
            for name in phase:
                self._merge_stage_to_target(specs[name])
                self._drop_stage(specs[name])
