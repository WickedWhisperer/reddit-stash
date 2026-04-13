import os
from pathlib import Path


def get_settings_file_path(default_name: str = "settings.ini") -> str:
    """
    Resolve the settings file path for the current run.

    Priority:
    1) SETTINGS_FILE environment variable
    2) default_name in the repo root
    """
    base_dir = Path(__file__).resolve().parents[1]
    settings_name = os.getenv("SETTINGS_FILE", default_name)
    settings_path = Path(settings_name)

    if settings_path.is_absolute():
        return str(settings_path)

    return str(base_dir / settings_path)
