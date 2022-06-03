from functools import reduce
from pathlib import Path
import time
from typing import Iterator
from dabble.base import K, V, KVStore


class Segment(dict[K, V]):
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
        return f"({len(self._index)})<Segment@{self._path}>"

    @classmethod
    def from_basepath(cls, base_path: Path) -> "Segment":
        """Generates a new segment given a base path.

        The name of the path is based on the current time.
        """
        path = base_path / f"segment_{time.time_ns()}"
        return cls(path)

    @classmethod
    def consolidate(
        cls, segments: list["Segment"], max_segment_size: int, base_path: Path
    ) -> list["Segment"]:
        """Combine into smaller list of segments.

        Let's pretend that we can't just load all the data into memory and split it out into
        segments.

        But what if we assume that we can load all the keys into memory. In that case, we can split
        the key space into segments and build each segment up. That's what we'll do in this
        implementation.

        In other future implementations we'll also look at what happens when we can't load all the
        keys into memory.
        """
        all_keys = sorted(  # Sorted for consistency for test assertions
            list(
                reduce(
                    lambda key_set, segment: key_set.union(set(segment)),
                    segments,
                    set(),
                )
            )
        )
        consolidated_segments: list[Segment] = []
        while all_keys:
            segment_keys, all_keys = (
                all_keys[:max_segment_size],
                all_keys[max_segment_size:],
            )
            segment_data = {}
            for segment in segments:
                for key, value in segment.items():
                    if key in segment_keys:
                        segment_data[key] = value
            consolidated_segment = Segment.from_basepath(base_path)
            consolidated_segment.add_data(segment_data)
            consolidated_segments.append(consolidated_segment)
        return consolidated_segments

    def __iter__(self) -> Iterator[K]:
        return iter(self._index)

    # def __next__(self):
    #     for key, _ in self._iter_items():
    #         yield key
    #     raise StopIteration

    def _iter_lines(self):
        with self._path.open("rt") as f:
            yield from f.readlines()

    def _iter_items(self):
        for line in self._iter_lines():
            yield tuple(line.rstrip("\n").split(":"))

    def keys(self):
        return self._index.keys()

    def items(self):
        for key in self.keys():
            yield key, self[key]

    def _reconstruct(self):
        self._index = {}
        self._length = 0
        with self._path.open("rt") as f:
            pos = f.tell()
            for line in f.readlines():
                self._length += 1
                k, v = line.rstrip("\n").split(":")
                self._index[k] = pos
                pos = f.tell()

    def __getitem__(self, key: K) -> V:
        with self._path.open("rt") as f:
            f.seek(self._index[key])
            line = f.readline()
            return line.rstrip("\n").split(":")[1]

    def __setitem__(self, key: K, value: V):
        self.add_data({key: value})

    def __len__(self) -> int:
        return self._length

    def add_data(self, data: dict[K, V]):
        with self._path.open("at") as f:
            for key, value in data.items():
                self._index[key] = f.tell()
                f.write(f"{key}:{value}\n")
                self._length += 1


class Manifest:
    def __init__(self, path: Path):
        self._path = path
        if path.exists():
            self.segments = self.load()
        else:
            self.segments = []

    def load(self) -> list["Segment"]:
        with self._path.open("rt") as f:
            return [
                Segment(self._path.parent / stripped_line)
                for line in f.readlines()
                if (stripped_line := line.rstrip("\n"))
            ]

    def dump(self):
        # In a production-grade system, we might write to a temp file first and atomic rename in
        # case the process dies mid-way, leaving the manifest corrupted.
        with self._path.open("wt") as f:
            f.writelines([segment._path.name + "\n" for segment in self.segments])

    def add_segment(self, segment: Segment):
        self.segments.append(segment)
        self.dump()

    def consolidate(self, max_segment_size: int, base_path: Path):
        self.segments = Segment.consolidate(self.segments, max_segment_size, base_path)
        self.dump()


class SegmentedIndexedAppendLog(KVStore):
    def __init__(self, *args, **kwargs):
        self._max_segment_size = kwargs.pop("max_segment_size", 100)
        super().__init__(*args, **kwargs)

    def setup(self):
        # On restart, all segments get considered "completed". It doesn't really matter if some are
        # smaller than `self._max_segment_size`.
        self._manifest: Manifest = Manifest(self._path / "MANIFEST")

    def get(self, key: K) -> V:
        for segment in reversed(self._manifest.segments):
            try:
                return segment[key]
            except KeyError:
                continue
        # If we got this far, then none of the segments have the key
        raise KeyError

    def set(self, key: K, value: V):
        if (
            not len(self._manifest.segments)
            or len(self._manifest.segments[-1]) >= self._max_segment_size
        ):
            self._manifest.add_segment(Segment.from_basepath(self._path))
        self._manifest.segments[-1][key] = value

    def maintain(self):
        self._manifest.consolidate(self._max_segment_size, self._path)
