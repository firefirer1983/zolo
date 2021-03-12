import os
from .dtypes import Credential


def load_credential_from_env() -> Credential:
    return Credential(
        os.environ["API_KEY"], os.environ["SECRET_KEY"],
        os.environ.get("passphrase")
    )
