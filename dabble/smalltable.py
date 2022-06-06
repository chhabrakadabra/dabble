import json
from uuid import uuid4
from pathlib import Path
from dabble.base import K, V, KVStore


class SSTable(dict[K, V]):
    MAX_INDEX_SIZE = 10

    def __init__(self, path: Path):
        self._path = path
        if path.exists():
            self._reconstruct()
        else:
            with self._path.open("wt") as f:
                f.write("")
            self._index = {}
            self._length = 0

    def __repr__(self) -> str:
        return f"({len(self._index)})<SSTable@{self._path}>"

    def __setitem__(self, key: K, val: V) -> None:
        raise NotImplemented("SSTables are immutable")

    def _reconstruct(self):
        index_path = self._path.parent / f"{self._path.name}-index.json"
        if not index_path.exists():
            raise NotImplemented("Too lazy to write out this part")
        self._index = json.loads(index_path.read_bytes())
        with self._path.open("rt") as f:
            self._length = len(f.readlines())

    def __getitem__(self, key: K) -> V:
        ...

    @classmethod
    def from_memtable(cls, memtable: "MemTable", path: Path):
        sorted_keys = sorted(memtable.keys())
        distance_to_marker = 0
        distance_between_markers = (len(sorted_keys) - 1) / (cls.MAX_INDEX_SIZE - 1)
        name = str(uuid4())
        table_path = path / name
        index_path = path / f"{name}-index.json"
        index = {}
        with table_path.open("wt") as f:
            for i, key in enumerate(sorted_keys):
                if distance_to_marker <= 0:
                    index[key] = f.tell()
                    distance_to_marker += distance_between_markers
                f.write(f"{key}:{memtable[key]}\n")
                distance_to_marker -= 1
        with index_path.open("wt") as f:
            json.dump(index, f)


class MemTable(dict[K, V]):
    """According to DDIA, this is supposed to be a balanced tree of some sort.

    I'm lazy and this is just a dict for now. For backup, this uses an append-only log.
    """

    def __init__(self, wal_path: Path):
        self._wal_path = wal_path
        if self._wal_path.exists():
            self._reconstruct()

    def __setitem__(self, key: K, val: V) -> None:
        self._append_to_log(key, val)
        return super().__setitem__(key, val)

    def _reconstruct(self):
        with open(self._wal_path, "rt") as f:
            for line in f.readlines():
                key, val = line.rstrip("\n").split(":")
                self[key] = val

    def flush(self) -> SSTable:
        sstable = SSTable.from_memtable(self)
        self.clear()
        self._wal_path.write_text("")
        return sstable


class SmallTable(KVStore):
    def __init__(self, *args, **kwargs):
        self._max_memtable_size = kwargs.pop("max_memtable_size", 100)
        super().__init__(*args, **kwargs)
        self._manifest_path = self._path / "MANIFEST"

    def setup(self):
        self._memtable = MemTable(self._path)
        if self._manifest_path.exists():
            self.load_sstables()
        else:
            self._sstables = []

    def load_sstables(self):
        with self._manifest_path.open("rt") as f:
            self._sstables = [
                SSTable(self._path / stripped_line)
                for line in f.readlines()
                if (stripped_line := line.rstrip("\n"))
            ]

    def _dump_manifest(self):
        # In a production-grade system, we might write to a temp file first and atomic rename in
        # case the process dies mid-way, leaving the manifest corrupted.
        with self._path.open("wt") as f:
            f.writelines([sstable._path.name + "\n" for sstable in self._sstables])

    def _add_sstable(self, sstable: SSTable):
        self._sstables.append(sstable)
        self._dump_manifest()

    def get(self, key: K) -> V:
        # We first try to look at the mem table.
        try:
            return self._manifest.memtable[key]
        except KeyError:
            pass
        # If the key was not in the memtable, we walk back through the sstables in reverse
        for sstable in reversed(self._manifest.sstables):
            try:
                return sstable[key]
            except KeyError:
                continue
        # If we got this far, we genuinely don't have the key
        raise KeyError

    def set(self, key: K, value: V):
        if len(self._memtable) > self._max_memtable_size:
            self._add_sstable(self._memtable.flush())
        self._memtable[key] = value

    def maintain(self):
        self._sstables = SSTable.consolidate(self._sstables)
        self._dump_manifest()
