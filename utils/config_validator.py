from __future__ import annotations

import configparser
import os
from typing import Any, Dict, List, Optional

from .feature_flags import get_media_config, get_settings_file_path, validate_media_config


class ConfigValidationError(Exception):
    def __init__(self, message: str, suggestions: Optional[List[str]] = None):
        super().__init__(message)
        self.message = message
        self.suggestions = suggestions or []

    def __str__(self) -> str:
        result = f"Configuration Error: {self.message}"
        if self.suggestions:
            result += "\n\nSuggestions:"
            for suggestion in self.suggestions:
                result += f"\n • {suggestion}"
        return result


class ConfigValidator:
    def __init__(self):
        self.config_parser = configparser.ConfigParser()
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self._load_config()

    def _load_config(self) -> None:
        config_file_path = get_settings_file_path()

        if not os.path.exists(config_file_path):
            raise ConfigValidationError(
                f"Configuration file not found: {config_file_path}",
                [
                    "Create the settings file in the repository root",
                    "Or set SETTINGS_FILE to the desired config filename",
                ],
            )

        try:
            read_files = self.config_parser.read(config_file_path)
            if not read_files:
                raise configparser.Error("No configuration data could be read")
        except configparser.Error as exc:
            raise ConfigValidationError(
                f"Failed to parse configuration file: {exc}",
                [
                    "Check for syntax errors in the INI file",
                    "Ensure section headers like [Settings] are present",
                    "Verify key=value formatting",
                ],
            )

    def validate_required_sections(self) -> None:
        required_sections = ["Settings", "Configuration"]
        missing = [section for section in required_sections if not self.config_parser.has_section(section)]
        if missing:
            self.errors.append(f"Missing required sections: {', '.join(missing)}")

    def validate_settings_section(self) -> None:
        if not self.config_parser.has_section("Settings"):
            return

        save_dir = self.config_parser.get("Settings", "save_directory", fallback="reddit/")
        if not save_dir.strip():
            self.errors.append("save_directory cannot be empty")

        save_type = self.config_parser.get("Settings", "save_type", fallback="ALL").upper()
        valid_save_types = ["ALL", "SAVED", "ACTIVITY", "UPVOTED"]
        if save_type not in valid_save_types:
            self.errors.append(f"Invalid save_type '{save_type}'. Must be one of: {', '.join(valid_save_types)}")

        check_type = self.config_parser.get("Settings", "check_type", fallback="LOG").upper()
        valid_check_types = ["LOG", "DIR"]
        if check_type not in valid_check_types:
            self.errors.append(f"Invalid check_type '{check_type}'. Must be one of: {', '.join(valid_check_types)}")

        for key in ("unsave_after_download", "process_gdpr", "process_api", "ignore_tls_errors"):
            try:
                self.config_parser.getboolean("Settings", key, fallback=False)
            except ValueError:
                self.errors.append(f"Invalid boolean value for {key}. Must be true or false")

        if self.config_parser.getboolean("Settings", "ignore_tls_errors", fallback=False):
            self.warnings.append(
                "ignore_tls_errors is enabled - this reduces security and should only be used for testing"
            )

    def validate_configuration_section(self) -> None:
        if not self.config_parser.has_section("Configuration"):
            return

        for key in ("client_id", "client_secret", "username", "password"):
            value = self.config_parser.get("Configuration", key, fallback=None)
            if value and value.strip() == "":
                self.warnings.append(f"{key} contains only whitespace - will fall back to environment variables")

    def validate_media_configuration(self) -> None:
        media_error = validate_media_config()
        if media_error:
            self.errors.append(f"Media configuration error: {media_error}")

        media_config = get_media_config()
        if media_config.is_media_enabled():
            if media_config.max_concurrent_downloads() <= 0:
                self.errors.append("Media max_concurrent_downloads must be greater than 0")
            if media_config.download_timeout() <= 0:
                self.errors.append("Media download_timeout must be greater than 0")

    def validate_all(self) -> Dict[str, Any]:
        self.errors = []
        self.warnings = []

        self.validate_required_sections()
        self.validate_settings_section()
        self.validate_configuration_section()
        self.validate_media_configuration()

        return {
            "valid": len(self.errors) == 0,
            "errors": self.errors,
            "warnings": self.warnings,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
        }

    def get_configuration_summary(self) -> str:
        summary = []
        summary.append(f"Config file: {get_settings_file_path()}")

        save_type = self.config_parser.get("Settings", "save_type", fallback="ALL")
        process_api = self.config_parser.getboolean("Settings", "process_api", fallback=True)
        process_gdpr = self.config_parser.getboolean("Settings", "process_gdpr", fallback=False)

        summary.append(f"Save Type: {save_type}")
        summary.append(f"API Processing: {'Enabled' if process_api else 'Disabled'}")
        summary.append(f"GDPR Processing: {'Enabled' if process_gdpr else 'Disabled'}")

        from .feature_flags import get_feature_summary

        summary.append(get_feature_summary())
        return "\n".join(summary)


def validate_configuration() -> Dict[str, Any]:
    validator = ConfigValidator()
    result = validator.validate_all()

    if not result["valid"]:
        error_msg = f"Found {result['error_count']} configuration error(s):\n"
        error_msg += "\n".join(f" • {error}" for error in result["errors"])
        suggestions = [
            "Check the active config file selected by SETTINGS_FILE",
            "Verify all required sections and settings are present",
            "Ensure boolean values are true or false",
            "Check file and directory permissions",
        ]
        raise ConfigValidationError(error_msg, suggestions)

    return result


def print_configuration_summary() -> None:
    try:
        validator = ConfigValidator()
        print("Configuration Summary:")
        print("=" * 50)
        print(validator.get_configuration_summary())

        result = validator.validate_all()
        if result["warnings"]:
            print("\nWarnings:")
            for warning in result["warnings"]:
                print(f" ⚠ {warning}")

        if result["valid"]:
            print("\n✅ Configuration is valid")
        else:
            print(f"\n❌ Configuration has {result['error_count']} error(s)")
    except Exception as exc:
        print(f"Failed to validate configuration: {exc}")
