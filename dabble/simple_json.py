import json
from dabble.base import K, V, KVStore


class SimpleJson(KVStore):
    def setup(self):
        self._store_path = self._path / "store"
        if not self._store_path.exists():
            self._dump({})

    def get(self, key: K) -> V:
        return self._load()[key]

    def set(self, key: K, value: V):
        d = self._load()
        d[key] = value
        self._dump(d)

    def _load(self) -> dict[K, V]:
        with self._store_path.open("rt") as f:
            return json.load(f)

    def _dump(self, d: dict[K, V]):
        with self._store_path.open("wt") as f:
            json.dump(d, f)
