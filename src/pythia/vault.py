"""Secrets in the macOS Keychain (via keyring)."""

from __future__ import annotations

import keyring
from keyring.errors import KeyringError

SERVICE = "pythia"
ANTHROPIC_KEY = "anthropic_api_key"
GITHUB_TOKEN = "github_token"
INCIDENT_IO_KEY = "incident_io_api_key"
SLACK_TOKEN = "slack_user_token"
CONFLUENCE_TOKEN = "confluence_api_token"


def get_secret(name: str) -> str | None:
    try:
        return keyring.get_password(SERVICE, name)
    except KeyringError:
        return None


def set_secret(name: str, value: str) -> None:
    keyring.set_password(SERVICE, name, value)


def delete_secret(name: str) -> None:
    try:
        keyring.delete_password(SERVICE, name)
    except KeyringError:
        pass
