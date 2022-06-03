from dabble.base import K, V, KVStore


class InMemory(KVStore):
    def setup(self):
        self._store = {}

    def get(self, key: K) -> V:
        return self._store[key]

    def set(self, key: K, value: V):
        self._store[key] = value
