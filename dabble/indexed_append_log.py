from dabble.base import K, V, KVStore


class IndexedAppendLog(KVStore):
    def setup(self):
        self._store_path = self._path / "store"
        self._index = {}
        if not self._store_path.exists():
            with self._store_path.open("wt") as f:
                f.write("")
        else:
            with self._store_path.open("rt") as f:
                pos = f.tell()
                for line in f.readlines():
                    k, v = line.rstrip("\n").split(":")
                    self._index[k] = pos
                    pos = f.tell()

    def get(self, key: K) -> V:
        with self._store_path.open("rt") as f:
            f.seek(self._index[key])
            line = f.readline()
            return line.rstrip("\n").split(":")[1]

    def set(self, key: K, value: V):
        with self._store_path.open("at") as f:
            self._index[key] = f.tell()
            f.write(f"{key}:{value}\n")
