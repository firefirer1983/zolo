import abc
import json
from typing import List, Dict, Type, Optional
import importlib
import logging
from dataclasses import dataclass

from .dtypes import Credential
log = logging.getLogger(__name__)


class ConfigParser:
    tplreg: Dict[str, Type["Template"]] = dict()
    
    @classmethod
    def register_template(cls, entry: str, template: Type["Template"]):
        cls.tplreg[entry] = template
    
    @classmethod
    def get_template(cls, entry: str) -> Optional[Type["Template"]]:
        return cls.tplreg.get(entry, None)
    
    @classmethod
    def parse(cls, dat, tpl=None):
        if isinstance(dat, dict):
            res = {}
            for k, v in dat.items():
                sub_tpl = cls.get_template(k)
                if sub_tpl:
                    res[k] = cls.parse(v, sub_tpl)
                else:
                    res[k] = v
            if tpl:
                return tpl(**res)
            return res
        elif isinstance(dat, list):
            if tpl:
                return [tpl(**cls.parse(d)) for d in dat]
            return [cls.parse(d) for d in dat]
        else:
            return dat


class Template(abc.ABC):
    def __init_subclass__(cls, entry: str = "", **kwargs):
        assert entry
        ConfigParser.register_template(entry, cls)


@dataclass(frozen=True)
class IndicatorConfig(Template, entry="indicators"):
    alias: str
    type: str
    parameters: dict


@dataclass(frozen=True)
class SecurityConfig(Template, entry="securities"):
    api_key: str
    secret_key: str
    market: str
    instrument_id: str
    indicators: List[IndicatorConfig]
    benchmarks: List[str]
    initial_margin: float
    passphrase: str = ""

    @property
    def name(self):
        return f"{self.market}:{self.instrument_id}"

    @property
    def credential(self):
        return Credential(self.api_key, self.secret_key, self.passphrase)


@dataclass(frozen=True)
class AppConfig(Template, entry="portfolio"):
    name: str
    uid: str
    exchange: str
    securities: List[SecurityConfig]
    run: str = "product"
    script: str = ""


def load_cfg_from_yaml(yml_path: str) -> Dict[str, AppConfig]:
    with open(yml_path, "r") as f:
        yaml = importlib.import_module("yaml")
        res = ConfigParser.parse(yaml.safe_load(f))
        return {r.name: r for r in res["portfolio"]}


def load_cfg_from_json(json_path) -> Dict[str, AppConfig]:
    with open(json_path, "r") as f:
        res = ConfigParser.parse(json.loads(f.read()))
        return {r.name: r for r in res["portfolio"]}


def load_cfg_from_path(path: str) -> Dict[str, AppConfig]:
    if path.endswith(".yml") or path.endswith(".yaml"):
        return load_cfg_from_yaml(path)
    if path.endswith(".json"):
        return load_cfg_from_json(path)
    raise FileNotFoundError
