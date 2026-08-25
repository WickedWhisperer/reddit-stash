from __future__ import annotations

import configparser
import logging
import os
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .config_paths import get_settings_file_path

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class MediaConfig:
    enabled: bool = True
    images: bool = True
    gifs: bool = False
    videos: bool = True
    audio: bool = True
    albums: bool = True
    video_quality: str = "high"
    max_image_size: int = 50 * 1024 * 1024
    max_video_size: int = 2 * 1024 * 1024 * 1024
    max_album_images: int = 50
    max_concurrent_downloads: int = 4
    download_timeout: int = 120

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


class FeatureFlags:
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
            video_quality=section.get("video_quality", "high").strip().lower(),
            max_image_size=get_int("max_image_size", 50 * 1024 * 1024),
            max_video_size=get_int("max_video_size", 2 * 1024 * 1024 * 1024),
            max_album_images=get_int("max_album_images", 50),
            max_concurrent_downloads=get_int("max_concurrent_downloads", 4),
            download_timeout=get_int("download_timeout", 120),
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


def is_feature_enabled(feature_name: str, default: bool = False) -> bool:
    return get_feature_flags().is_feature_enabled(feature_name, default)


def reload_features() -> None:
    get_feature_flags().reload()


__all__ = [
    "FeatureFlags",
    "MediaConfig",
    "get_feature_flags",
    "get_media_config",
    "is_feature_enabled",
    "reload_features",
]
