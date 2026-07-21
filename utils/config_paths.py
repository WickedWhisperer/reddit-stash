from __future__ import annotations

import os
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _get_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def get_storage_provider() -> str:
    """Return the active storage provider name, if one was configured."""
    return (_get_env("STORAGE_PROVIDER") or _get_env("PROVIDER") or "none").lower()


def get_settings_file_path(default_name: str = "settings.ini") -> str:
    """Resolve the active settings file.

    Precedence:
    1. SETTINGS_FILE
    2. MEGA_SETTINGS_FILE when provider is mega
    3. mega_settings.ini when provider is mega and that file exists
    4. default_name in the repo root
    5. mega_settings.ini in the repo root if it exists and the default file does not
    """
    explicit = _get_env("SETTINGS_FILE")
    if explicit:
        return explicit

    provider = get_storage_provider()
    mega_explicit = _get_env("MEGA_SETTINGS_FILE")
    if provider == "mega" and mega_explicit:
        return mega_explicit

    repo_root = _repo_root()
    mega_default = repo_root / "mega_settings.ini"
    default_path = repo_root / default_name

    if provider == "mega" and mega_default.exists():
        return str(mega_default)

    if default_path.exists():
        return str(default_path)

    if mega_default.exists():
        return str(mega_default)

    return str(default_path)
