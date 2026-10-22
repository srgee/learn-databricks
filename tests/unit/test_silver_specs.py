import pytest

from sbit.silver_specs import PHASES, SILVER_STREAMS
from sbit.streaming import StreamSpec
from sbit.upserters import MergeSpec


def test_silver_streams_are_defined():
    assert len(SILVER_STREAMS) > 0


@pytest.mark.parametrize('stream_name, spec', SILVER_STREAMS.items())
def test_stream_spec_structure(stream_name: str, spec: StreamSpec):
    assert spec.name == stream_name
    assert isinstance(spec.source, str) and spec.source
    assert isinstance(spec.merge, MergeSpec)
    assert spec.merge.join_on


def test_phases_coverage():
    all_stream_keys = set(SILVER_STREAMS.keys())
    phased_streams = [stream for phase in PHASES for stream in phase]

    # Check that all streams in phases exist in definitions
    for stream in phased_streams:
        assert stream in all_stream_keys

    # Check that there are no duplicate definitions across phases
    assert len(phased_streams) == len(set(phased_streams))

    # Check that all defined streams are included in the execution phases
    assert set(phased_streams) == all_stream_keys
