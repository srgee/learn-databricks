"""End-to-end test for the bronze layer with two data-set iterations.

Verifies that BronzeIngester correctly lands all five feeds across
two consecutive data-set productions. Iteration 2 expects cumulative
counts (set 1 + set 2 records already in the bronze tables).
"""

import pytest
from tests.bronze_assertions import validate_bronze_layer

from sbit.bronze import BronzeIngester
from sbit.config import Config


def _volume_subpath(config: Config, sub: str) -> str:
    return f'/Volumes/{config.catalogue_name}/{config.db_name}/{config.volume_name}/{sub}'


def _build_ingester(spark, config: Config) -> BronzeIngester:
    return BronzeIngester(
        spark=spark,
        source_path=_volume_subpath(config, config.base_data_path),
        checkpoint_path=_volume_subpath(config, config.base_checkpoint_path),
    )


@pytest.mark.parametrize('set_num', [1, 2])
def test_bronze_layer_iteration(
    set_num: int,
    spark,
    config: Config,
    data_producer,
    bronze_tables,  # fixture: ensures tables start empty per set
) -> None:
    """One test per iteration. Run in order; bronze_tables must scope
    per-set so cumulative counts make sense."""
    data_producer.produce(set_num)
    ingester = _build_ingester(spark, config)
    ingester.run()  # blocking call until micro-batches catch up
    validate_bronze_layer(spark, config, set_num)


def test_bronze_layer_two_iterations_sequential(
    spark,
    config: Config,
    data_producer,
) -> None:
    """Alternative: single test that runs both iterations sequentially.

    Use this if you need the second iteration to start from the state
    left by the first (cumulative bronze tables) and don't want
    pytest's parametrize to isolate them.
    """
    ingester = _build_ingester(spark, config)

    for set_num in (1, 2):
        logger.info('=== Iteration %s ===', set_num)
        data_producer.produce(set_num)
        ingester.run()
        validate_bronze_layer(spark, config, set_num)
