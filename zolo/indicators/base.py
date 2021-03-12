import abc
from typing import Dict, Type, Union


class IndicatorRegistry:
    registry: Dict[str, Type["Indicator"]] = dict()

    @classmethod
    def register(cls, alias: str, indicator_class):
        assert alias
        if alias in cls.registry:
            raise ValueError
        cls.registry[alias] = indicator_class
        setattr(cls, "alias", alias)

    @classmethod
    def create_indicator(
        cls, alias_or_cls: Union[str, "Indicator"], *args, **kwargs
    ):
        if isinstance(alias_or_cls, str):
            return cls.registry[alias_or_cls](*args, **kwargs)
        else:
            return cls.registry[getattr(alias_or_cls, "alias")](*args, **kwargs)


class Indicator(abc.ABC):

    @classmethod
    def __init_subclass__(cls, alias: str = "", **kwargs):
        IndicatorRegistry.register(alias, cls)


create_indicator = IndicatorRegistry.create_indicator
