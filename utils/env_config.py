import os
import configparser
from .config_paths import get_settings_file_path

invalid_config = (None, '', "None")

def load_config_and_env():
    """Load configuration from settings.ini (or SETTINGS_FILE) and fall back to environment variables if necessary."""
    config_parser = configparser.ConfigParser()

    config_file_path = get_settings_file_path()
    config_parser.read(config_file_path)

    # Load from config, but treat "None" or empty strings as invalid
    client_id = config_parser.get('Configuration', 'client_id', fallback=None)
    client_secret = config_parser.get('Configuration', 'client_secret', fallback=None)
    username = config_parser.get('Configuration', 'username', fallback=None)
    password = config_parser.get('Configuration', 'password', fallback=None)

    # Fall back to environment variables when config values are missing/invalid
    client_id = client_id if client_id and client_id not in invalid_config else os.getenv('REDDIT_CLIENT_ID')
    client_secret = client_secret if client_secret and client_secret not in invalid_config else os.getenv('REDDIT_CLIENT_SECRET')
    username = username if username and username not in invalid_config else os.getenv('REDDIT_USERNAME')
    password = password if password and password not in invalid_config else os.getenv('REDDIT_PASSWORD')

    if not all([client_id, client_secret, username, password]):
        missing = []
        if not client_id:
            missing.append("REDDIT_CLIENT_ID")
        if not client_secret:
            missing.append("REDDIT_CLIENT_SECRET")
        if not username:
            missing.append("REDDIT_USERNAME")
        if not password:
            missing.append("REDDIT_PASSWORD")

        msg = (
            f"Missing Reddit API credentials: {', '.join(missing)}\n\n"
            "Since November 2025, Reddit requires pre-approval to create new API apps.\n"
            "If you need new credentials, apply here (2-4 week wait):\n"
            " https://support.reddithelp.com/hc/en-us/requests/new?ticket_form_id=14868593862164\n\n"
            "Existing credentials (created before Nov 2025) still work normally.\n"
            "Set credentials via environment variables (REDDIT_CLIENT_ID, etc.) or in settings.ini.\n\n"
            "Alternative: Set process_api=false and process_gdpr=true in settings.ini\n"
            "to process your Reddit GDPR export data without API credentials."
        )
        raise Exception(msg)

    return client_id, client_secret, username, password

def get_ignore_tls_errors():
    """Load the ignore_tls_errors setting from settings.ini (or SETTINGS_FILE)."""
    config_parser = configparser.ConfigParser()
    config_file_path = get_settings_file_path()
    config_parser.read(config_file_path)
    return config_parser.getboolean('Settings', 'ignore_tls_errors', fallback=False)
    
