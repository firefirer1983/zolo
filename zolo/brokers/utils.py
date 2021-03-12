import json

from dataclasses import asdict
from typing import Tuple

from ..dtypes import Credential


def build_context_id(exchange: str, market: str, instrument_id: str) -> str:
    return f"{exchange}:{market}:{instrument_id}"


def parse_context_id(security_id: str) -> Tuple[str, str, str]:
    exchange, market, instrument_id = security_id.split(":")
    return exchange, market, instrument_id


def build_adapter_id(
    adapter_type: str, exchange: str, market: str, cred: Credential = None,
    **parameters
):
    res = f"{adapter_type}:{exchange}:{market}"
    if cred:
        parameters.update(asdict(cred))
    if parameters:
        parameters = json.dumps(parameters, sort_keys=True)
        res += f":{parameters}"
    return res
