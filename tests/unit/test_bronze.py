from unittest.mock import MagicMock

from sbit.bronze import BronzeIngester
from sbit.bronze_specs import SourceFeed


def test_ingest_constructs_stream_and_writer():
    spark_mock = MagicMock()
    feed = SourceFeed(
        name='test_bz', file_format='json', schema='id long, value string', options={'maxFilesPerTrigger': '1'}
    )

    ingester = BronzeIngester(spark=spark_mock, base_dir='s3://bucket', checkpoint_path='s3://chk')
    ingester.ingest(feed, once=True)

    spark_mock.readStream.format.assert_called_once_with('cloudFiles')
    spark_mock.readStream.format().schema.assert_called_once_with('id long, value string')
    spark_mock.readStream.format().schema().option.assert_any_call('cloudFiles.format', 'json')
    spark_mock.readStream.format().schema().option().options.assert_called_once_with(maxFilesPerTrigger='1')
    spark_mock.readStream.format().schema().option().options().load.assert_called_once_with('s3://bucket/test_bz')
