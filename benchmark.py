import argparse
from collections.abc import MutableMapping
from pathlib import Path
from random import randrange
from tempfile import TemporaryDirectory
from timeit import timeit

import dabble_json

benchmark_fns = []
implementations = {impl.__name__: getattr(impl, "KVStore") for impl in [dabble_json]}


def benchmark(fn):
    benchmark_fns.append(fn)
    return fn


@benchmark
def sequential_set_and_get(store: MutableMapping, iterations: int):
    """Repeatedly sets and gets a sequence of key-value pairs.

    Tests read-after-write performance.
    """
    numbers = iter(range(iterations))

    def workload():
        i = next(numbers)
        store[f"key_{i}"] = f"value_{i}"
        store[f"key_{i}"]

    return timeit(workload, number=iterations)


@benchmark
def sequential_sets(store: MutableMapping, iterations: int):
    """Repeatedly sets a sequence of key-value pairs.

    A decent indication of write-throughput.
    """
    numbers = iter(range(iterations))

    def workload():
        i = next(numbers)
        store[f"key_{i}"] = f"value_{i}"

    return timeit(workload, number=iterations)


@benchmark
def sequential_gets(store: MutableMapping, iterations: int):
    """Repeatedly gets a sequence of keys.

    The store is pre-filled with a bunch of data before we just read all keys sequentially.
    """
    for i in range(iterations):
        store[f"key_{i}"] = f"value_{i}"
    numbers = iter(range(iterations))

    def workload():
        i = next(numbers)
        store[f"key_{i}"]

    return timeit(workload, number=iterations)


@benchmark
def random_gets(store: MutableMapping, iterations: int):
    """Repeatedly gets random keys.

    The store is pre-filled with a bunch of data before we just do random gets.
    """
    for i in range(iterations):
        store[f"key_{i}"] = f"value_{i}"

    def workload():
        i = randrange(iterations)
        store[f"key_{i}"]

    return timeit(workload, number=iterations)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run benchmarks on dabble implementations."
    )
    parser.add_argument(
        "--iterations",
        dest="iterations",
        type=int,
        help="Number of iterations to run for the benchmarks",
    )
    parser.add_argument(
        "--implementation",
        dest="implementation",
        type=str,
        choices=implementations.keys(),
        help="The implementation of dabble to run benchmarks against",
    )
    args = parser.parse_args()
    line = "=============================="
    print(line)
    for benchmark_fn in benchmark_fns:
        print(f"Running: {benchmark_fn.__name__}")
        print(benchmark_fn.__doc__)
        with TemporaryDirectory() as tmpdir:
            time_taken = benchmark_fn(
                implementations[args.implementation](path=Path(tmpdir)), args.iterations
            )
        print(f"Completed in {time_taken:.4f} seconds")
        print(line)
