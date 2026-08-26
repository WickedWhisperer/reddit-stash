from __future__ import annotations

import configparser
import logging
import os
import threading
from dataclasses import dataclass
from typing import Optional

from .config_paths import get_settings_file_path

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool = False) -> bool:
    """Read a boolean environment variable without raising on bad input."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class MediaConfig:
    """Typed media settings shared by the downloader and configuration validator.

    The public API intentionally keeps the callable accessors used elsewhere in
    the repository (for example ``max_concurrent_downloads()`` and
    ``download_timeout()``).  These were part of the application's expected
    interface and must not be replaced with plain dataclass attributes.
    """

    enabled: bool = True
    images: bool = True
    gifs: bool = False
    videos: bool = True
    audio: bool = True
    albums: bool = True
    video_quality_value: str = "high"
    max_image_size_value: int = 50 * 1024 * 1024
    max_video_size_value: int = 2 * 1024 * 1024 * 1024
    max_album_images_value: int = 50
    max_concurrent_downloads_value: int = 4
    download_timeout_value: int = 120
    max_daily_storage_mb_value: int = 1024
    thumbnail_size_value: int = 800
    create_thumbnails: bool = True

    def is_media_enabled(self) -> bool:
        return self.enabled

    def is_images_enabled(self) -> bool:
        return self.enabled and self.images

    def is_gifs_enabled(self) -> bool:
        return self.enabled and self.gifs

    def is_videos_enabled(self) -> bool:
        return self.enabled and self.videos

    def is_audio_enabled(self) -> bool:
        return self.enabled and self.audio

    def is_albums_enabled(self) -> bool:
        return self.enabled and self.albums

    def video_quality(self) -> str:
        return self.video_quality_value

    def max_image_size(self) -> int:
        return self.max_image_size_value

    def max_video_size(self) -> int:
        return self.max_video_size_value

    def max_album_images(self) -> int:
        return self.max_album_images_value

    def max_concurrent_downloads(self) -> int:
        return self.max_concurrent_downloads_value

    def download_timeout(self) -> int:
        return self.download_timeout_value

    def max_daily_storage_mb(self) -> int:
        return self.max_daily_storage_mb_value

    def thumbnail_size(self) -> int:
        return self.thumbnail_size_value

    def thumbnails_enabled(self) -> bool:
        return self.create_thumbnails


class FeatureFlags:
    """Load and expose settings-backed feature flags."""

    def __init__(self, settings_path: Optional[str] = None):
        self.settings_path = settings_path or str(get_settings_file_path())
        self._config = self._load_config(self.settings_path)

    @staticmethod
    def _load_config(path: str) -> configparser.ConfigParser:
        parser = configparser.ConfigParser()
        parser.read(path)
        return parser

    def get_media_config(self) -> MediaConfig:
        section = self._config["Media"] if self._config.has_section("Media") else {}

        def get_bool(key: str, default: bool) -> bool:
            value = section.get(key, str(default))
            return value.strip().lower() in {"1", "true", "yes", "on"}

        def get_int(key: str, default: int) -> int:
            try:
                return int(section.get(key, str(default)))
            except (TypeError, ValueError):
                return default

        return MediaConfig(
            enabled=get_bool("download_enabled", True),
            images=get_bool("download_images", True),
            gifs=get_bool("download_gifs", False),
            videos=get_bool("download_videos", True),
            audio=get_bool("download_audio", True),
            albums=get_bool("download_albums", True),
            video_quality_value=section.get("video_quality", "high").strip().lower(),
            max_image_size_value=get_int("max_image_size", 50 * 1024 * 1024),
            max_video_size_value=get_int("max_video_size", 2 * 1024 * 1024 * 1024),
            max_album_images_value=get_int("max_album_images", 50),
            max_concurrent_downloads_value=get_int("max_concurrent_downloads", 4),
            download_timeout_value=get_int("download_timeout", 120),
            max_daily_storage_mb_value=get_int("max_daily_storage_mb", 1024),
            thumbnail_size_value=get_int("thumbnail_size", 800),
            create_thumbnails=get_bool("create_thumbnails", True),
        )

    def is_feature_enabled(self, feature_name: str, default: bool = False) -> bool:
        section = self._config["Features"] if self._config.has_section("Features") else {}
        value = section.get(feature_name, str(default))
        return value.strip().lower() in {"1", "true", "yes", "on"}

    def reload(self) -> None:
        self.settings_path = str(get_settings_file_path())
        self._config = self._load_config(self.settings_path)


_instance: Optional[FeatureFlags] = None
_lock = threading.Lock()


def get_feature_flags() -> FeatureFlags:
    global _instance
    with _lock:
        current_path = str(get_settings_file_path())
        if _instance is None or _instance.settings_path != current_path:
            _instance = FeatureFlags(current_path)
    return _instance


def get_media_config() -> MediaConfig:
    return get_feature_flags().get_media_config()


def validate_media_config() -> Optional[str]:
    """Validate the active media settings.

    Returns ``None`` when valid, otherwise a concise error string consumed by
    ``utils.config_validator.ConfigValidator``.
    """
    config = get_media_config()

    if not config.is_media_enabled():
        return None

    if config.max_image_size() <= 0:
        return "max_image_size must be greater than 0"

    if config.max_video_size() <= 0:
        return "max_video_size must be greater than 0"

    if config.max_album_images() < 0:
        return "max_album_images must be 0 or greater"

    if config.max_concurrent_downloads() <= 0:
        return "max_concurrent_downloads must be greater than 0"

    if config.download_timeout() <= 0:
        return "download_timeout must be greater than 0"

    if config.max_daily_storage_mb() <= 0:
        return "max_daily_storage_mb must be greater than 0"

    if config.thumbnail_size() <= 0:
        return "thumbnail_size must be greater than 0"

    if config.video_quality() not in {"high", "low"}:
        return "video_quality must be 'high' or 'low'"

    return None


def get_feature_summary() -> str:
    """Return the media feature summary printed during application startup."""
    config = get_media_config()

    if not config.is_media_enabled():
        return "Media downloads: DISABLED"

    enabled = []
    if config.is_images_enabled():
        enabled.append("images")
    if config.is_gifs_enabled():
        enabled.append("gifs")
    if config.is_videos_enabled():
        enabled.append("videos")
    if config.is_audio_enabled():
        enabled.append("audio")
    if config.is_albums_enabled():
        enabled.append("albums")

    features = ", ".join(enabled) if enabled else "none"
    return f"Media downloads: ENABLED ({features})"


def is_feature_enabled(feature_name: str, default: bool = False) -> bool:
    return get_feature_flags().is_feature_enabled(feature_name, default)


def reload_features() -> None:
    get_feature_flags().reload()


__all__ = [
    "FeatureFlags",
    "MediaConfig",
    "get_feature_flags",
    "get_media_config",
    "validate_media_config",
    "get_feature_summary",
    "is_feature_enabled",
    "get_settings_file_path",
    "reload_features",
        ]
