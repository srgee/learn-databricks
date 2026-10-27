import pytest

from sbit.gold import GoldSpec
from sbit.gold_specs import GOLD_PHASES, GOLD_STREAMS
from sbit.upserters import MergeSpec


def test_gold_streams_are_defined():
    assert len(GOLD_STREAMS) > 0


@pytest.mark.parametrize('stream_name, spec', GOLD_STREAMS.items())
def test_stream_spec_structure(stream_name: str, spec: GoldSpec):
    assert spec.name == stream_name
    assert isinstance(spec.source, str) and spec.source
    assert isinstance(spec.merge, MergeSpec)
    assert spec.merge.join_on


def test_phases_coverage():
    all_stream_keys = set(GOLD_STREAMS.keys())
    phased_streams = [stream for phase in GOLD_PHASES for stream in phase]

    for stream in phased_streams:
        assert stream in all_stream_keys

    assert len(phased_streams) == len(set(phased_streams))
    assert set(phased_streams) == all_stream_keys
