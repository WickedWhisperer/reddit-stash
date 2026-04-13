from __future__ import annotations

import argparse
import configparser
import sys

from utils.config_paths import get_settings_file_path
from utils.file_path_validate import validate_and_set_directory
from utils.storage.factory import get_storage_provider, load_storage_config


def _load_local_dir() -> str:
    parser = configparser.ConfigParser()
    parser.read(get_settings_file_path())
    local_dir = parser.get("Settings", "save_directory", fallback="reddit/")
    return validate_and_set_directory(local_dir)


def _load_check_type() -> str:
    parser = configparser.ConfigParser()
    parser.read(get_settings_file_path())
    return parser.get("Settings", "check_type", fallback="DIR").upper()


def _get_provider_for_name(name: str):
    config = load_storage_config()

    if name == "dropbox":
        from utils.storage.dropbox_provider import DropboxStorageProvider

        return DropboxStorageProvider(dropbox_directory=config.dropbox_directory)

    if name == "s3":
        if not config.s3_bucket:
            print("Error: S3 bucket not configured.")
            sys.exit(1)

        from utils.storage.s3_provider import S3StorageProvider

        return S3StorageProvider(
            bucket=config.s3_bucket,
            region=config.s3_region,
            storage_class=config.s3_storage_class,
            endpoint_url=config.s3_endpoint_url,
        )

    if name == "mega":
        from utils.storage.mega_provider import MegaStorageProvider

        return MegaStorageProvider()

    print(f"Error: Unknown provider '{name}'. Must be 'dropbox', 's3' or 'mega'.")
    sys.exit(1)


def _get_remote_directory(provider_name: str) -> str:
    config = load_storage_config()

    if provider_name in {"dropbox", "mega"}:
        return config.dropbox_directory

    return config.dropbox_directory.lstrip("/")


def cmd_download(args):
    """Download files from the configured storage provider."""
    config = load_storage_config()
    provider = get_storage_provider(config)

    if provider is None:
        print("No storage provider configured. Set provider in [Storage] section of settings.ini.")
        sys.exit(1)

    provider.connect()

    local_dir = _load_local_dir()
    check_type = _load_check_type()
    remote_dir = _get_remote_directory(config.provider.value)

    print(f"Downloading directory from {provider.get_provider_name()} using {check_type} mode...")
    result = provider.download_directory(remote_dir, local_dir, check_type=check_type)

    if result.errors:
        print(f"\n{len(result.errors)} error(s) occurred:")
        for err in result.errors[:5]:
            print(f" - {err}")
        if len(result.errors) > 5:
            print(f" - ... and {len(result.errors) - 5} more")
        sys.exit(1)

    print(f"Download complete: {result.summary()}")


def cmd_upload(args):
    """Upload files to the configured storage provider."""
    config = load_storage_config()
    provider = get_storage_provider(config)

    if provider is None:
        print("No storage provider configured. Set provider in [Storage] section of settings.ini.")
        sys.exit(1)

    provider.connect()

    local_dir = _load_local_dir()
    check_type = _load_check_type()
    remote_dir = _get_remote_directory(config.provider.value)

    print(f"Uploading directory to {provider.get_provider_name()} using {check_type} mode...")
    result = provider.upload_directory(local_dir, remote_dir, check_type=check_type)

    if result.errors:
        print(f"\n{len(result.errors)} error(s) occurred:")
        for err in result.errors[:5]:
            print(f" - {err}")
        if len(result.errors) > 5:
            print(f" - ... and {len(result.errors) - 5} more")
        sys.exit(1)

    print(f"Upload complete: {result.summary()}")


def main():
    parser = argparse.ArgumentParser(description="Reddit Stash storage helper")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--download", action="store_true", help="Download from storage")
    group.add_argument("--upload", action="store_true", help="Upload to storage")

    args = parser.parse_args()

    if args.download:
        cmd_download(args)
    elif args.upload:
        cmd_upload(args)


if __name__ == "__main__":
    main()
