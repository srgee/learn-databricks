"""DDL generation helpers.

Transforms a spec's ``target_schema`` (StructType) into backtick-quoted
DDL strings suitable for execution against Databricks SQL. Backticks are
mandatory because the Databricks parser drops or renames reserved
identifiers (e.g. ``time``, ``update_type``, ``sex``) when they appear
unquoted in CTAS or CREATE TABLE statements.

This module is the bridge between the declarative spec layer
(``specs.py``) and the imperative setup layer (``setup.py``). It contains
no Spark session dependency and is fully unit-testable without a cluster.
"""

from __future__ import annotations

from typing import Protocol

from pyspark.sql.types import StructType


class _SpecWithTargetSchema(Protocol):
    """Protocol for any spec that exposes a target table schema.

    Both ``SilverSpec`` and ``GoldSpec`` satisfy this protocol once they
    declare ``target_schema: StructType``. Using a Protocol (instead of
    ``SilverSpec | GoldSpec``) keeps this module decoupled from the
    spec dataclasses — if a new spec type is added later, it works
    automatically as long as it has ``name`` and ``target_schema``.
    """

    name: str
    target_schema: StructType


def ddl_for_spec(spec: _SpecWithTargetSchema, catalogue: str, db: str) -> str:
    """Generate ``CREATE TABLE IF NOT EXISTS`` DDL from a spec's target_schema.

    Each column name is wrapped in backticks so that reserved SQL keywords
    (e.g. ``time``, ``state``, ``update_type``, ``sex``) are preserved
    exactly as declared in the spec. Without backticks, the Databricks
    SQL parser has been observed to silently drop ``time`` and rename
    ``heartrate`` to ``heart_rate`` (collision with the target table name)
    when the DDL is generated via CTAS or ``DataFrameWriter.saveAsTable``.

    Args:
        spec: A spec (SilverSpec or GoldSpec) with a populated
            ``target_schema``.
        catalogue: Unity Catalog catalogue name (e.g. ``dev``).
        db: Schema (database) name within the catalogue (e.g. ``db``).

    Returns:
        A DDL string ready for ``spark.sql(...)``. Example::

            CREATE TABLE IF NOT EXISTS dev.db.heart_rate
            (`device_id` bigint, `time` timestamp, `heartrate` double, `valid` boolean)
            USING DELTA
    """
    cols_ddl = ', '.join(f'`{f.name}` {f.dataType.simpleString()}' for f in spec.target_schema.fields)
    return f'CREATE TABLE IF NOT EXISTS {catalogue}.{db}.{spec.name} ({cols_ddl}) USING DELTA'
