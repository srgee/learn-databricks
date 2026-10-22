import pytest

from sbit.bronze_specs import FEEDS, SourceFeed


def test_feeds_are_defined():
    assert len(FEEDS) > 0


def test_feed_names_are_unique():
    names = [feed.name for feed in FEEDS]
    assert len(names) == len(set(names))


@pytest.mark.parametrize('feed', FEEDS, ids=[f.name for f in FEEDS])
def test_feed_structure(feed: SourceFeed):
    assert isinstance(feed.name, str) and feed.name
    assert feed.file_format in {'csv', 'json'}
    assert isinstance(feed.schema, str) and feed.schema
    assert isinstance(feed.options, dict)

    if feed.transform is not None:
        assert callable(feed.transform)
