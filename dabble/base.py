from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import TypeVar


K = TypeVar("K")
V = TypeVar("V")


class KVStore(dict[K, V], metaclass=ABCMeta):
    def __init__(self, path: Path):
        path.mkdir(parents=True, exist_ok=True)
        self._path = path
        self.setup()

    @abstractmethod
    def setup(self):
        ...

    @abstractmethod
    def get(self, key: K) -> V:
        ...

    @abstractmethod
    def set(self, key: K, value: V):
        ...

    def maintain(self):
        """Run maintenance tasks"""

    def __getitem__(self, key: K) -> V:
        return self.get(key)

    def __setitem__(self, key: K, value: V):
        return self.set(key, value)
