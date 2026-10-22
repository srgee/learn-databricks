from unittest.mock import MagicMock

from sbit.streaming import StreamSpec, UpsertPipeline
from sbit.upserters import MergeSpec


def test_pipeline_start_configures_stream():
    spark_mock = MagicMock()
    merge_spec = MergeSpec(join_on='t.id = s.id')
    spec = StreamSpec(
        name='target_table',
        source='source_table',
        merge=merge_spec,
        watermark_col='timestamp',
        dedupe_keys=('id', 'timestamp'),
        output_mode='update',
    )

    pipeline = UpsertPipeline(spark=spark_mock, checkpoint_root='s3://chk', starting_version=1)
    pipeline.start(spec, once=True)

    spark_mock.readStream.options.assert_called_once_with(startingVersion=1, ignoreDeletes='true')
    spark_mock.readStream.options().table.assert_called_once_with('source_table')
