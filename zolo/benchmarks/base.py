import abc
from typing import Dict, Type


class BenchmarkRegistry:

    registry: Dict[str, Type["Benchmark"]] = dict()

    @classmethod
    def register(cls, alias: str, benchmark_class: Type["Benchmark"]):
        assert alias
        if alias in cls.registry:
            raise TypeError
        cls.registry[alias] = benchmark_class
    
    @classmethod
    def create_benchmark(cls, alias: str, *args, **kwargs):
        return cls.registry[alias](*args, **kwargs)
    

class Benchmark(abc.ABC):
    @classmethod
    def __init_subclass__(cls, alias: str = "", **kwargs):
        BenchmarkRegistry.register(alias, cls)

    def __init__(self, api_key: str):
        self._api_key = api_key

    @property
    def api_key(self) -> str:
        return self._api_key


create_benchmark = BenchmarkRegistry.create_benchmark
