from __future__ import annotations

import configparser
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


def get_settings_file_path() -> str:
    """
    Return the active settings file.

    The workflow can set SETTINGS_FILE to switch configs, but plain
    local runs still fall back to settings.ini in the repository root.
    """
    settings_file = os.getenv("SETTINGS_FILE", "settings.ini")

    if os.path.isabs(settings_file):
        return settings_file

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, settings_file)


def _load_config() -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    parser.read(get_settings_file_path())
    return parser


@dataclass
class MediaConfig:
    parser: configparser.ConfigParser

    def is_media_enabled(self) -> bool:
        return self.parser.getboolean("Media", "download_enabled", fallback=False)

    def is_images_enabled(self) -> bool:
        return self.is_media_enabled() and self.parser.getboolean(
            "Media", "download_images", fallback=True
        )

    def is_gifs_enabled(self) -> bool:
        return self.is_media_enabled() and self.parser.getboolean(
            "Media", "download_gifs", fallback=True
        )

    def is_videos_enabled(self) -> bool:
        return self.is_media_enabled() and self.parser.getboolean(
            "Media", "download_videos", fallback=True
        )

    def is_audio_enabled(self) -> bool:
        return self.is_media_enabled() and self.parser.getboolean(
            "Media", "download_audio", fallback=True
        )

    def is_albums_enabled(self) -> bool:
        return self.is_media_enabled() and self.parser.getboolean(
            "Media", "download_albums", fallback=True
        )

    def is_thumbnails_enabled(self) -> bool:
        return self.is_media_enabled() and self.parser.getboolean(
            "Media", "create_thumbnails", fallback=True
        )

    def max_image_size(self) -> int:
        return self.parser.getint("Media", "max_image_size", fallback=5 * 1024 * 1024)

    def max_video_size(self) -> int:
        return self.parser.getint("Media", "max_video_size", fallback=200 * 1024 * 1024)

    def max_album_images(self) -> int:
        return self.parser.getint("Media", "max_album_images", fallback=50)

    def max_concurrent_downloads(self) -> int:
        return self.parser.getint("Media", "max_concurrent_downloads", fallback=3)

    def download_timeout(self) -> int:
        return self.parser.getint("Media", "download_timeout", fallback=30)

    def max_daily_storage_mb(self) -> int:
        return self.parser.getint("Media", "max_daily_storage_mb", fallback=1024)

    def get_summary(self) -> str:
        if not self.is_media_enabled():
            return "Media downloads: DISABLED"

        features = []
        if self.is_images_enabled():
            features.append("images")
        if self.is_gifs_enabled():
            features.append("gifs")
        if self.is_videos_enabled():
            features.append("videos")
        if self.is_audio_enabled():
            features.append("audio")
        if self.is_albums_enabled():
            features.append("albums")
        if self.is_thumbnails_enabled():
            features.append("thumbnails")

        return f"Media downloads: ENABLED ({', '.join(features)})"


@dataclass
class StorageConfig:
    provider: str = "none"
    dropbox_directory: str = "/reddit"
    s3_bucket: Optional[str] = None
    s3_region: Optional[str] = None
    s3_storage_class: str = "STANDARD_IA"
    s3_endpoint_url: Optional[str] = None


def get_media_config() -> MediaConfig:
    return MediaConfig(_load_config())


def get_storage_config() -> StorageConfig:
    parser = _load_config()

    provider = parser.get("Storage", "provider", fallback="none").strip().lower()
    dropbox_directory = parser.get("Settings", "dropbox_directory", fallback="/reddit")
    s3_bucket = parser.get("Storage", "s3_bucket", fallback=None)
    s3_region = parser.get("Storage", "s3_region", fallback=None)
    s3_storage_class = parser.get("Storage", "s3_storage_class", fallback="STANDARD_IA")
    s3_endpoint_url = parser.get("Storage", "s3_endpoint_url", fallback=None)

    return StorageConfig(
        provider=provider,
        dropbox_directory=dropbox_directory,
        s3_bucket=s3_bucket if s3_bucket and s3_bucket.lower() != "none" else None,
        s3_region=s3_region if s3_region and s3_region.lower() != "none" else None,
        s3_storage_class=s3_storage_class,
        s3_endpoint_url=s3_endpoint_url if s3_endpoint_url and s3_endpoint_url.lower() != "none" else None,
    )


def validate_media_config() -> Optional[str]:
    """
    Return an error string if the media section is invalid, otherwise None.
    """
    parser = _load_config()

    if not parser.has_section("Media"):
        return "Missing [Media] section"

    try:
        enabled = parser.getboolean("Media", "download_enabled", fallback=False)
        images = parser.getboolean("Media", "download_images", fallback=False)
        gifs = parser.getboolean("Media", "download_gifs", fallback=False)
        videos = parser.getboolean("Media", "download_videos", fallback=False)
        audio = parser.getboolean("Media", "download_audio", fallback=False)
        albums = parser.getboolean("Media", "download_albums", fallback=False)
        thumbnails = parser.getboolean("Media", "create_thumbnails", fallback=False)

        if enabled and not any([images, gifs, videos, audio, albums, thumbnails]):
            return "Media is enabled but all media features are disabled"

        for key in ("max_image_size", "max_video_size", "max_album_images", "max_concurrent_downloads", "download_timeout", "max_daily_storage_mb"):
            value = parser.getint("Media", key, fallback=1)
            if value <= 0:
                return f"{key} must be a positive integer"

    except ValueError as exc:
        return f"Invalid media configuration value: {exc}"

    return None


def get_storage_summary() -> str:
    config = get_storage_config()

    if config.provider == "dropbox":
        return f"Cloud storage: Dropbox ({config.dropbox_directory})"
    if config.provider == "s3":
        bucket = config.s3_bucket or "unset"
        return f"Cloud storage: S3 (s3://{bucket}, class={config.s3_storage_class})"

    return "Cloud storage: disabled"


def get_feature_summary() -> str:
    media = get_media_config()
    parts = [media.get_summary(), get_storage_summary()]
    return "\n".join(parts)
