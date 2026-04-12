"""Unified storage CLI for Reddit Stash.

Supports Dropbox, S3 and MEGA backends through a single interface.
"""

from __future__ import annotations

import argparse
import configparser
import os
import sys

from utils.file_path_validate import validate_and_set_directory
from utils.storage.base import StorageProvider
from utils.storage.factory import get_storage_provider, load_storage_config


def _load_local_dir() -> str:
    parser = configparser.ConfigParser()
    parser.read("settings.ini")
    local_dir = parser.get("Settings", "save_directory", fallback="reddit/")
    return validate_and_set_directory(local_dir)


def _load_check_type() -> str:
    parser = configparser.ConfigParser()
    parser.read("settings.ini")
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

    if check_type == "LOG":
        print(f"Downloading log file from {provider.get_provider_name()}...")
    else:
        print(f"Downloading directory from {provider.get_provider_name()}...")

    result = provider.download_directory(remote_dir, local_dir, check_type=check_type)
    if result.errors:
        print(f"\n{len(result.errors)} error(s) occurred:")
        for err in result.errors[:5]:
            print(f" - {err}")


def cmd_upload(args):
    """Upload files to the configured storage provider."""
    config = load_storage_config()
    provider = get_storage_provider(config)
    if provider is None:
        print("No storage provider configured. Set provider in [Storage] section of settings.ini.")
        sys.exit(1)

    provider.connect()
    local_dir = _load_local_dir()
    remote_dir = _get_remote_directory(config.provider.value)

    print(f"Uploading to {provider.get_provider_name()}...")
    result = provider.upload_directory(local_dir, remote_dir)
    if result.errors:
        print(f"\n{len(result.errors)} error(s) occurred:")
        for err in result.errors[:5]:
            print(f" - {err}")


def cmd_migrate(args):
    """Upload/download between storage providers using the migration helper."""
    from utils.storage.migration import StorageMigration

    source = _get_provider_for_name(args.source)
    target = _get_provider_for_name(args.target)
    if args.source == args.target:
        print("Error: Source and target providers must be different.")
        sys.exit(1)

    print(f"Connecting to {source.get_provider_name()}...")
    source.connect()
    print(f"Connecting to {target.get_provider_name()}...")
    target.connect()

    source_dir = _get_remote_directory(args.source)
    target_dir = _get_remote_directory(args.target)
    migration = StorageMigration(
        source=source,
        target=target,
        source_directory=source_dir,
        target_directory=target_dir,
    )

    if args.execute:
        print("\nExecuting migration...")
        result = migration.execute()
        if result.errors:
            print(f"\n{len(result.errors)} error(s):")
            for err in result.errors[:10]:
                print(f" - {err}")
    else:
        plan = migration.dry_run()
        if plan.file_count > 0:
            print("\nTo execute this migration, add --execute:")
            print(f" python storage_utils.py --migrate --source {args.source} --target {args.target} --execute")
        else:
            print("\nNo files to migrate.")


def main():
    parser = argparse.ArgumentParser(
        description="Reddit Stash storage management (Dropbox, S3, MEGA)",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--download", action="store_true", help="Download from storage provider")
    group.add_argument("--upload", action="store_true", help="Upload to storage provider")
    group.add_argument("--migrate", action="store_true", help="Migrate between providers")

    parser.add_argument("--source", choices=["dropbox", "s3", "mega"], help="Migration source provider")
    parser.add_argument("--target", choices=["dropbox", "s3", "mega"], help="Migration target provider")
    parser.add_argument("--execute", action="store_true", help="Execute migration (default is dry-run)")

    args = parser.parse_args()

    if args.migrate:
        if not args.source or not args.target:
            parser.error("--migrate requires --source and --target")
        cmd_migrate(args)
    elif args.download:
        cmd_download(args)
    elif args.upload:
        cmd_upload(args)


if __name__ == "__main__":
    main()
    
