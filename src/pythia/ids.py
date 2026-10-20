"""ID generation with a per-type prefix (ev_, en_, cl_, ...)."""

import hashlib
import secrets


def new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(6)}"


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
