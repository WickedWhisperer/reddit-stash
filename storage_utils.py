from __future__ import annotations

import argparse
import configparser
import os
import sys
import tempfile
from pathlib import Path
from typing import Optional

from utils.config_paths import get_settings_file_path
from utils.file_path_validate import validate_and_set_directory
from utils.storage.factory import get_storage_provider, load_storage_config


def _load_parser() -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    parser.read(get_settings_file_path())
    return parser


def _load_local_dir() -> str:
    parser = _load_parser()
    local_dir = (
        parser.get("Settings", "save_directory", fallback=None)
        or parser.get("Storage", "save_directory", fallback=None)
        or "reddit/"
    )
    return validate_and_set_directory(local_dir)


def _load_check_type() -> str:
    parser = _load_parser()
    return (
        parser.get("Settings", "check_type", fallback=None)
        or parser.get("Storage", "check_type", fallback=None)
        or "DIR"
    ).upper()


def _remote_root_for(provider_name: str) -> str:
    config = load_storage_config()
    if provider_name.lower() in {"dropbox", "mega"}:
        return config.storage_root
    return config.storage_root.lstrip("/")


def _provider_from_name(name: str):
    name = name.lower().strip()
    config = load_storage_config()

    if name == "dropbox":
        from utils.storage.dropbox_provider import DropboxStorageProvider

        return DropboxStorageProvider(dropbox_directory=config.storage_root)

    if name == "s3":
        if not config.s3_bucket:
            raise ValueError("S3 bucket is not configured.")
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

    raise ValueError(f"Unknown provider '{name}'.")


def _connect(provider) -> None:
    if hasattr(provider, "connect") and callable(provider.connect):
        provider.connect()


def _summary(result) -> str:
    if hasattr(result, "summary") and callable(result.summary):
        return result.summary()
    if isinstance(result, dict):
        return str(result)
    return "done"


def _errors(result) -> list[str]:
    errs = getattr(result, "errors", None)
    if errs is None and isinstance(result, dict):
        errs = result.get("errors", [])
    return list(errs or [])


def cmd_download(_: argparse.Namespace) -> int:
    config = load_storage_config()
    provider = get_storage_provider(config)
    if provider is None:
        print("No storage provider configured.")
        return 1

    _connect(provider)
    local_dir = _load_local_dir()
    check_type = _load_check_type()
    remote_dir = _remote_root_for(config.provider.value)

    print(f"Downloading from {provider.get_provider_name()} using {check_type} mode...")
    result = provider.download_directory(remote_dir, local_dir, check_type=check_type)

    errs = _errors(result)
    if errs:
        print(f"\n{len(errs)} error(s) occurred:")
        for err in errs[:5]:
            print(f" - {err}")
        if len(errs) > 5:
            print(f" - ... and {len(errs) - 5} more")
        return 1

    print(f"Download complete: {_summary(result)}")
    return 0


def cmd_upload(_: argparse.Namespace) -> int:
    config = load_storage_config()
    provider = get_storage_provider(config)
    if provider is None:
        print("No storage provider configured.")
        return 1

    _connect(provider)
    local_dir = _load_local_dir()
    check_type = _load_check_type()
    remote_dir = _remote_root_for(config.provider.value)

    print(f"Uploading to {provider.get_provider_name()} using {check_type} mode...")
    result = provider.upload_directory(local_dir, remote_dir, check_type=check_type)

    errs = _errors(result)
    if errs:
        print(f"\n{len(errs)} error(s) occurred:")
        for err in errs[:5]:
            print(f" - {err}")
        if len(errs) > 5:
            print(f" - ... and {len(errs) - 5} more")
        return 1

    print(f"Upload complete: {_summary(result)}")
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    source_name = args.source.lower().strip()
    target_name = args.target.lower().strip()

    if source_name == target_name:
        print("Error: source and target providers must be different.")
        return 1

    try:
        source = _provider_from_name(source_name)
        target = _provider_from_name(target_name)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    _connect(source)
    _connect(target)

    source_dir = _remote_root_for(source_name)
    target_dir = _remote_root_for(target_name)

    try:
        from utils.storage.migration import StorageMigration
    except Exception:
        StorageMigration = None  # type: ignore[assignment]

    if StorageMigration is not None:
        migration = StorageMigration(
            source=source,
            target=target,
            source_directory=source_dir,
            target_directory=target_dir,
        )

        if args.execute:
            print("Executing migration...")
            result = migration.execute()
            errs = _errors(result)
            if errs:
                print(f"\n{len(errs)} error(s):")
                for err in errs[:10]:
                    print(f" - {err}")
                return 1
            print(f"Migration complete: {_summary(result)}")
            return 0

        plan = migration.dry_run()
        count = getattr(plan, "file_count", None)
        if count is None and isinstance(plan, dict):
            count = plan.get("file_count", 0)
        print("Dry-run migration plan:")
        print(f" Source: {source_name} -> {source_dir}")
        print(f" Target: {target_name} -> {target_dir}")
        print(f" Files: {count or 0}")
        print(f"To execute this migration, rerun with: --migrate --source {source_name} --target {target_name} --execute")
        return 0

    # Fallback path if the repo does not expose StorageMigration
    if not args.execute:
        print("Dry-run migration plan:")
        print(f" Source: {source_name} -> {source_dir}")
        print(f" Target: {target_name} -> {target_dir}")
        print(f"To execute this migration, rerun with: --migrate --source {source_name} --target {target_name} --execute")
        return 0

    with tempfile.TemporaryDirectory(prefix="reddit-stash-migration-") as tmpdir:
        temp_dir = Path(tmpdir) / "payload"
        temp_dir.mkdir(parents=True, exist_ok=True)

        print(f"Downloading from {source.get_provider_name()}...")
        src_result = source.download_directory(source_dir, str(temp_dir), check_type="DIR")
        if _errors(src_result):
            for err in _errors(src_result)[:10]:
                print(f" - {err}")
            return 1

        print(f"Uploading to {target.get_provider_name()}...")
        tgt_result = target.upload_directory(str(temp_dir), target_dir, check_type="DIR")
        if _errors(tgt_result):
            for err in _errors(tgt_result)[:10]:
                print(f" - {err}")
            return 1

    print("Migration complete.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Reddit Stash storage management")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--download", action="store_true", help="Download from the configured storage provider")
    group.add_argument("--upload", action="store_true", help="Upload to the configured storage provider")
    group.add_argument("--migrate", action="store_true", help="Migrate between providers")

    parser.add_argument("--source", choices=["dropbox", "s3", "mega"], help="Source provider for migration")
    parser.add_argument("--target", choices=["dropbox", "s3", "mega"], help="Target provider for migration")
    parser.add_argument("--execute", action="store_true", help="Execute migration instead of a dry run")

    args = parser.parse_args()

    if args.download:
        return cmd_download(args)
    if args.upload:
        return cmd_upload(args)
    if args.migrate:
        if not args.source or not args.target:
            parser.error("--migrate requires --source and --target")
        return cmd_migrate(args)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
