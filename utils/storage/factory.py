"""Storage provider factory and configuration loading."""
from __future__ import annotations

import configparser
import os
from dataclasses import dataclass
from typing import Optional

from utils.config_paths import get_settings_file_path
from utils.storage.base import StorageProvider


def _normalize_optional(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    if not value or value.lower() in {"none", "null", "undefined"}:
        return None
    return value


def _get_config_value(parser: configparser.ConfigParser, section: str, key: str) -> Optional[str]:
    if parser.has_option(section, key):
        return _normalize_optional(parser.get(section, key))
    return None


@dataclass(slots=True)
class StorageConfig:
    provider: StorageProvider = StorageProvider.NONE
    storage_root: str = "/reddit"
    s3_bucket: Optional[str] = None
    s3_region: Optional[str] = None
    s3_storage_class: str = "STANDARD_IA"
    s3_endpoint_url: Optional[str] = None

    @property
    def dropbox_directory(self) -> str:
        return self.storage_root


def load_storage_config() -> StorageConfig:
    """
    Load storage settings from environment variables and the active settings file.

    Precedence:
      1. Environment variables
      2. [Storage] section
      3. [Settings] section
      4. Defaults
    """
    parser = configparser.ConfigParser()
    settings_file = get_settings_file_path()
    parser.read(settings_file)

    provider_str = (
        os.getenv("STORAGE_PROVIDER")
        or _get_config_value(parser, "Storage", "provider")
        or _get_config_value(parser, "Settings", "storage_provider")
        or "none"
    )

    try:
        provider = StorageProvider(provider_str.lower())
    except ValueError as exc:
        valid = ", ".join(item.value for item in StorageProvider)
        raise ValueError(
            f"Invalid storage provider '{provider_str}'. "
            f"Must be one of: {valid}"
        ) from exc

    storage_root = (
        os.getenv("STORAGE_ROOT")
        or os.getenv("DROPBOX_DIRECTORY")
        or _get_config_value(parser, "Storage", "storage_root")
        or _get_config_value(parser, "Storage", "dropbox_directory")
        or _get_config_value(parser, "Settings", "storage_root")
        or _get_config_value(parser, "Settings", "dropbox_directory")
        or "/reddit"
    )

    s3_bucket = (
        os.getenv("AWS_S3_BUCKET")
        or _get_config_value(parser, "Storage", "s3_bucket")
        or _get_config_value(parser, "Settings", "s3_bucket")
    )
    s3_region = (
        os.getenv("AWS_DEFAULT_REGION")
        or os.getenv("AWS_REGION")
        or _get_config_value(parser, "Storage", "s3_region")
        or _get_config_value(parser, "Settings", "s3_region")
    )
    s3_storage_class = (
        os.getenv("S3_STORAGE_CLASS")
        or _get_config_value(parser, "Storage", "s3_storage_class")
        or _get_config_value(parser, "Settings", "s3_storage_class")
        or "STANDARD_IA"
    )
    s3_endpoint_url = (
        os.getenv("S3_ENDPOINT_URL")
        or _get_config_value(parser, "Storage", "s3_endpoint_url")
        or _get_config_value(parser, "Settings", "s3_endpoint_url")
    )

    return StorageConfig(
        provider=provider,
        storage_root=storage_root,
        s3_bucket=s3_bucket,
        s3_region=s3_region,
        s3_storage_class=s3_storage_class,
        s3_endpoint_url=s3_endpoint_url,
    )


def get_storage_provider(config: Optional[StorageConfig] = None):
    """Instantiate the configured storage provider."""
    if config is None:
        config = load_storage_config()

    if config.provider == StorageProvider.NONE:
        return None

    if config.provider == StorageProvider.DROPBOX:
        from utils.storage.dropbox_provider import DropboxStorageProvider

        return DropboxStorageProvider(dropbox_directory=config.storage_root)

    if config.provider == StorageProvider.S3:
        if not config.s3_bucket:
            raise ValueError(
                "S3 provider selected but no bucket is configured. "
                "Set AWS_S3_BUCKET or s3_bucket in the config."
            )

        from utils.storage.s3_provider import S3StorageProvider

        return S3StorageProvider(
            bucket=config.s3_bucket,
            region=config.s3_region,
            storage_class=config.s3_storage_class,
            endpoint_url=config.s3_endpoint_url,
        )

    if config.provider == StorageProvider.MEGA:
        from utils.storage.mega_provider import MegaStorageProvider

        return MegaStorageProvider()

    return None
