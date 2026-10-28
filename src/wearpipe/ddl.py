"""DDL generation helpers.

Transforms a spec's target_schema (StructType) into backtick-quoted
DDL strings suitable for execution against Databricks SQL. Backticks are
mandatory because the Databricks parser drops or renames reserved
identifiers (e.g. ``time``, ``update_type``, ``sex``) when they appear
unquoted in CTAS or CREATE TABLE statements.

This module is the bridge between the declarative spec layer
(specs.py) and the imperative setup layer (``setup.py``). It contains
no Spark session dependency and is fully unit-testable without a cluster.
"""

from __future__ import annotations

from typing import Protocol

from pyspark.sql.types import StructType


class _SpecWithTargetSchema(Protocol):
    """Protocol for any spec that exposes a target table schema.

    Both SilverSpec and GoldSpec satisfy this protocol once they
    declare target_schema (StructType). Using a Protocol keeps this module
    decoupled from the spec dataclasses. If a new spec type is added later, it works
    automatically as long as it has name and target_schema.

    Properties are declared (rather than plain attributes) to mark them
    as read-only, which is required to satisfy frozen dataclasses like
    SilverSpec. Without this, static type checkers reject the
    assignment because protocols default to writable attributes and
    frozen dataclasses only expose read-only ones.
    """

    @property
    def name(self) -> str: ...

    @property
    def target_schema(self) -> StructType: ...


def ddl_for_spec(spec: _SpecWithTargetSchema, catalogue: str, db: str) -> str:
    """Generate ``CREATE TABLE IF NOT EXISTS`` DDL from a spec's target_schema."""
    cols_ddl = ', '.join(f'`{f.name}` {f.dataType.simpleString()}' for f in spec.target_schema.fields)
    return f'CREATE TABLE IF NOT EXISTS {catalogue}.{db}.{spec.name} ({cols_ddl}) USING DELTA'
