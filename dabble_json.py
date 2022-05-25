import json

from pathlib import Path
from typing import TypeVar


K = TypeVar("K")
V = TypeVar("V")


class KVStore(dict[K, V]):
    def __init__(self, path: Path):
        path.mkdir(parents=True, exist_ok=True)
        self._path = path / "store"
        if not self._path.exists():
            self._dump({})

    def __getitem__(self, key: K) -> V:
        return self._load()[key]

    def __setitem__(self, key: K, value: V):
        d = self._load()
        d[key] = value
        self._dump(d)

    def _load(self) -> dict[K, V]:
        with self._path.open("rt") as f:
            return json.load(f)

    def _dump(self, d: dict[K, V]):
        with self._path.open("wt") as f:
            json.dump(d, f)
