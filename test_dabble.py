from itertools import zip_longest
import pytest
from dabble import InMemory, KVStore, SegmentedIndexedAppendLog
from dabble.segmented_indexed_append_log import Segment


@pytest.fixture(params=KVStore.__subclasses__())
def Store(request):
    yield request.param


def test_getting_and_setting(tmp_path, Store):
    store = Store(path=tmp_path / "db")
    store["foo"] = "bar"
    store["bar"] = "baz"
    assert store["foo"] == "bar"
    assert store["bar"] == "baz"
    store["foo"] = "baz"
    assert store["foo"] == "baz"


def test_not_found(tmp_path, Store):
    store = Store(path=tmp_path / "db")
    with pytest.raises(KeyError):
        store["whatisthis?"]


def test_recovery_from_failure(tmp_path, Store):
    if Store == InMemory:
        pytest.xfail("InMemory store cannot recover from failure")

    db_path = tmp_path / "db"
    store = Store(path=db_path)
    store["foo"] = "bar"
    assert store["foo"] == "bar"

    del store
    store = Store(path=db_path)
    assert store["foo"] == "bar"


class TestSegmentedIndexAppendedLog:
    def test_segment_creation(self, tmp_path):
        store = SegmentedIndexedAppendLog(path=tmp_path, max_segment_size=2)
        assert len(store._manifest.segments) == 0

        store["foo"] = "bar"
        assert len(store._manifest.segments) == 1
        store["toast"] = "toaster"
        assert len(store._manifest.segments) == 1
        first_segment = store._manifest.segments[0]
        assert first_segment._path.parent == tmp_path
        assert first_segment._path.read_text() == "foo:bar\ntoast:toaster\n"

        store["cat"] = "akiyo"
        assert len(store._manifest.segments) == 2
        store["cat"] = "chi"
        assert len(store._manifest.segments) == 2
        assert store._manifest.segments[-1]._path.read_text() == "cat:akiyo\ncat:chi\n"

    @pytest.mark.parametrize(
        ("ops", "expected_segments"),
        (
            pytest.param([], [], id="empty_store"),
            pytest.param([("foo", "bar")], ["foo:bar\n"], id="single_key_once"),
            pytest.param(
                [("foo", "val1"), ("foo", "val4"), ("foo", "val3"), ("foo", "val4")],
                ["foo:val4\n"],
                id="single_key_multiple_segments",
            ),
            pytest.param(
                [
                    ("key1", "val1"),
                    ("key2", "val2"),
                    ("key1", "val1*"),
                    ("key1", "val1**"),
                    ("key2", "val2*"),
                    ("key3", "val3"),
                ],
                ["key1:val1**\nkey2:val2*\n", "key3:val3\n"],
                id="more_keys_than_max_segment_size",
            ),
        ),
    )
    def test_consolidation(self, tmp_path, ops, expected_segments):
        store = SegmentedIndexedAppendLog(path=tmp_path, max_segment_size=2)
        for key, value in ops:
            store[key] = value
        store.maintain()
        for segment, expected_text in zip(store._manifest.segments, expected_segments):
            assert segment._path.read_text() == expected_text

    def test_segment_as_dict(self, tmp_path):
        segment = Segment(tmp_path / "segment")
        ops = (("foo", "bar"), ("biz", "baz"))
        for k, v in ops:
            segment[k] = v
        for expected, actual in zip_longest(ops, segment.items()):
            assert expected == actual
        assert len(segment) == 2
        assert list(segment.keys()) == ["foo", "biz"]
        assert list(segment) == ["foo", "biz"]
