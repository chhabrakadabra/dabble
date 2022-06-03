from dabble.base import K, V, KVStore


class SimpleAppendLog(KVStore):
    def setup(self):
        self._store_path = self._path / "store"
        if not self._store_path.exists():
            with self._store_path.open("wt") as f:
                f.write("")

    def get(self, key: K) -> V:
        found = None
        with self._store_path.open("rt") as f:
            for line in f.readlines():
                k, v = line.rstrip("\n").split(":")
                if k == key:
                    found = v
        if not found:
            raise KeyError
        else:
            return found

    def set(self, key: K, value: V):
        with self._store_path.open("at") as f:
            f.write(f"{key}:{value}\n")

    def maintain(self):
        ...
