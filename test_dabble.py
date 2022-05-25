from dabble_json import KVStore


def test_getting_and_setting(tmp_path):
    store = KVStore(path=tmp_path / "db")
    store["foo"] = "bar"
    store["bar"] = "baz"
    assert store["foo"] == "bar"
    assert store["bar"] == "baz"


def test_recovery_from_failure(tmp_path):
    db_path = tmp_path / "db"
    store = KVStore(path=db_path)
    store["foo"] = "bar"
    assert store["foo"] == "bar"

    del store
    store = KVStore(path=db_path)
    assert store["foo"] == "bar"
