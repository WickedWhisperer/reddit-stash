from __future__ import annotations

import configparser

import praw

from utils.config_paths import get_settings_file_path
from utils.config_validator import validate_configuration
from utils.env_config import load_config_and_env
from utils.feature_flags import get_feature_summary
from utils.file_operations import save_user_activity
from utils.file_path_validate import validate_and_set_directory
from utils.gdpr_processor import process_gdpr_export
from utils.log_utils import load_file_log, save_file_log


def _load_runtime_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    settings_path = get_settings_file_path()

    read_files = config.read(settings_path)
    if not read_files:
        raise FileNotFoundError(f"Configuration file not found: {settings_path}")

    return config


def _build_reddit_client() -> praw.Reddit:
    client_id, client_secret, username, password = load_config_and_env()
    return praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        username=username,
        password=password,
        user_agent=f"Reddit Saved Saver by /u/{username}",
    )


def main() -> None:
    print("Validating configuration...")

    try:
        validation_result = validate_configuration()

        warnings = validation_result.get("warnings", [])
        if warnings:
            print("\nConfiguration Warnings:")
            for warning in warnings:
                print(f"⚠ {warning}")

        print("✅ Configuration validated successfully")
        print(get_feature_summary())
    except Exception as exc:
        print(f"❌ Configuration validation failed: {exc}")
        print(f"\nFor detailed configuration information, check: {get_settings_file_path()}")
        return

    try:
        config_parser = _load_runtime_config()
    except Exception as exc:
        print(f"❌ Failed to load configuration: {exc}")
        return

    unsave_setting = config_parser.getboolean("Settings", "unsave_after_download", fallback=False)
    save_directory = config_parser.get("Settings", "save_directory", fallback="reddit/")
    process_api = config_parser.getboolean("Settings", "process_api", fallback=True)
    process_gdpr = config_parser.getboolean("Settings", "process_gdpr", fallback=False)

    save_directory = validate_and_set_directory(save_directory)

    reddit = None
    if process_api:
        try:
            reddit = _build_reddit_client()
        except Exception as exc:
            print(f"❌ Failed to initialize Reddit API client: {exc}")
            return
    elif process_gdpr:
        try:
            reddit = _build_reddit_client()
        except Exception:
            print("No API credentials available. GDPR export will run in CSV-only mode.")
            reddit = None

    if not process_api and not process_gdpr:
        print("Both process_api and process_gdpr are disabled. Nothing to do.")
        return

    file_log = load_file_log(save_directory)

    total_processed = 0
    total_skipped = 0
    total_size = 0
    total_media_size = 0

    if process_api:
        print("Processing items from Reddit API...")
        api_stats = save_user_activity(reddit, save_directory, file_log, unsave=unsave_setting)
        total_processed += api_stats[0]
        total_skipped += api_stats[1]
        total_size += api_stats[2]
        if len(api_stats) > 3:
            total_media_size += api_stats[3]

    if process_gdpr:
        print("\nProcessing GDPR export data...")
        gdpr_stats = process_gdpr_export(
            reddit,
            save_directory,
            set(file_log.keys()),
            set(),
            file_log,
        )
        total_processed += gdpr_stats[0]
        total_skipped += gdpr_stats[1]
        total_size += gdpr_stats[2]

    save_file_log(file_log, save_directory)

    print(
        f"\nProcessing completed. {total_processed} items processed, "
        f"{total_skipped} items skipped."
    )
    print(f"Markdown file storage: {total_size / (1024 * 1024):.2f} MB")
    print(f"Media file storage: {total_media_size / (1024 * 1024):.2f} MB")
    print(f"Total combined storage: {(total_size + total_media_size) / (1024 * 1024):.2f} MB")


if __name__ == "__main__":
    main()
