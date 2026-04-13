import configparser
import praw

from utils.config_validator import validate_configuration
from utils.env_config import load_config_and_env
from utils.feature_flags import get_feature_summary
from utils.file_operations import save_user_activity
from utils.file_path_validate import validate_and_set_directory
from utils.gdpr_processor import process_gdpr_export
from utils.log_utils import load_file_log, save_file_log


def main():
    print("Validating configuration...")

    try:
        validation_result = validate_configuration()

        if validation_result["warnings"]:
            print("\nConfiguration Warnings:")
            for warning in validation_result["warnings"]:
                print(f"⚠ {warning}")

        print("✅ Configuration validated successfully")
        print(get_feature_summary())

    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")
        print("\nFor detailed configuration information, check your settings.ini file.")
        return

    config_parser = configparser.ConfigParser()
    config_parser.read("settings.ini")

    unsave_setting = config_parser.getboolean("Settings", "unsave_after_download", fallback=False)
    save_directory = config_parser.get("Settings", "save_directory", fallback="reddit/")
    process_api = config_parser.getboolean("Settings", "process_api", fallback=True)
    process_gdpr = config_parser.getboolean("Settings", "process_gdpr", fallback=False)

    save_directory = validate_and_set_directory(save_directory)

    reddit = None

    if process_api:
        client_id, client_secret, username, password = load_config_and_env()
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
            user_agent=f"Reddit Saved Saver by /u/{username}",
        )
    elif process_gdpr:
        try:
            client_id, client_secret, username, password = load_config_and_env()
            reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                username=username,
                password=password,
                user_agent=f"Reddit Saved Saver by /u/{username}",
            )
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
        total_processed = api_stats[0]
        total_skipped = api_stats[1]
        total_size = api_stats[2]
        total_media_size = api_stats[3] if len(api_stats) > 3 else 0

    if process_gdpr:
        existing_files = set(file_log.keys())
        created_dirs_cache = set()

        print("\nProcessing GDPR export data...")
        gdpr_stats = process_gdpr_export(
            reddit,
            save_directory,
            existing_files,
            created_dirs_cache,
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
    print(
        f"Total combined storage: "
        f"{(total_size + total_media_size) / (1024 * 1024):.2f} MB"
    )


if __name__ == "__main__":
    main()
