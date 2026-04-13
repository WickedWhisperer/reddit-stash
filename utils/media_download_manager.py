from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests

from .media_services.reddit_media import RedditMediaDownloader

logger = logging.getLogger(__name__)


def _looks_like_reddit_media(url: str) -> bool:
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        return domain.endswith(
            (
                "i.redd.it",
                "v.redd.it",
                "preview.redd.it",
                "external-preview.redd.it",
            )
        )
    except Exception:
        return False


def _infer_media_extension(url: str) -> str:
    """
    Infer a sane extension for extensionless Reddit URLs.
    """
    try:
        parsed = urlparse(url or "")
        domain = parsed.netloc.lower()
        path = parsed.path.lower()
        filename = os.path.basename(path)
        _, ext = os.path.splitext(filename)

        if ext == ".gifv":
            return ".mp4"

        if ext in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".mp4", ".webm", ".mov"}:
            return ext

        if "v.redd.it" in domain or path.startswith("/dash_"):
            return ".mp4"

        if domain.endswith(("i.redd.it", "preview.redd.it", "external-preview.redd.it")):
            return ".jpg"

        if domain.endswith(("redgifs.com", "gfycat.com")):
            return ".mp4"

        return ".jpg"
    except Exception:
        return ".jpg"


class MediaDownloadManager:
    """
    Minimal, reliable media manager.

    The important fix is that downloads are no longer blocked just because
    images are disabled. Video/GIF-style Reddit media now gets a chance to run.
    """

    def __init__(self):
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._url_lock = threading.Lock()
        self._downloaded_urls: Dict[str, str] = {}
        self._failed_urls: Dict[str, int] = {}
        self._reddit_downloader: Optional[RedditMediaDownloader] = None

        try:
            self._reddit_downloader = RedditMediaDownloader()
        except Exception as exc:
            self._logger.warning(f"Reddit media downloader unavailable: {exc}")
            self._reddit_downloader = None

    def _should_skip_url(self, url: str) -> bool:
        return self._failed_urls.get(url, 0) >= 2

    def _record_failure(self, url: str) -> None:
        self._failed_urls[url] = self._failed_urls.get(url, 0) + 1

    def _download_with_requests(self, url: str, save_path: str) -> Optional[str]:
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)

            with open(save_path, "wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        handle.write(chunk)

            if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                return save_path

            return None
        except Exception as exc:
            self._logger.debug(f"Generic download failed for {url}: {exc}")
            return None

    def download_media(self, url: str, save_path: str) -> Optional[str]:
        """
        Download media and return the saved file path, or None on failure.
        """
        if not url or not save_path:
            return None

        with self._url_lock:
            if self._should_skip_url(url):
                self._logger.debug(f"Skipping URL after prior failures: {url}")
                return None

            if url in self._downloaded_urls:
                existing = self._downloaded_urls[url]
                if os.path.exists(existing) and os.path.getsize(existing) > 0:
                    return existing
                self._downloaded_urls.pop(url, None)

        try:
            if _looks_like_reddit_media(url) and self._reddit_downloader is not None:
                result = self._reddit_downloader.download(url, save_path)
                if getattr(result, "is_success", False) and getattr(result, "local_path", None):
                    local_path = result.local_path
                    with self._url_lock:
                        self._downloaded_urls[url] = local_path
                    return local_path

                self._logger.debug(f"Reddit downloader failed for {url}: {getattr(result, 'error_message', None)}")
                self._record_failure(url)
                return None

            local_path = self._download_with_requests(url, save_path)
            if local_path:
                with self._url_lock:
                    self._downloaded_urls[url] = local_path
                return local_path

            self._record_failure(url)
            return None

        except Exception as exc:
            self._logger.error(f"Exception during media download from {url}: {exc}")
            self._record_failure(url)
            return None

    def get_service_health(self) -> Dict[str, Any]:
        return {
            "reddit_downloader": self._reddit_downloader is not None,
            "cached_urls": len(self._downloaded_urls),
            "failed_urls": len(self._failed_urls),
        }

    def is_service_available(self, service_name: str) -> bool:
        if service_name == "reddit":
            return self._reddit_downloader is not None
        return True

    def reset_service(self, service_name: str) -> None:
        if service_name == "reddit":
            try:
                self._reddit_downloader = RedditMediaDownloader()
            except Exception:
                self._reddit_downloader = None

    def process_pending_retries(self, max_retries: int = 50) -> Dict[str, int]:
        return {"processed": 0, "successful": 0, "failed": 0, "skipped": 0}


_media_manager: Optional[MediaDownloadManager] = None


def get_media_manager() -> MediaDownloadManager:
    global _media_manager
    if _media_manager is None:
        _media_manager = MediaDownloadManager()
    return _media_manager


def download_media_file(url: str, save_directory: str, file_id: str) -> Optional[str]:
    """
    Convenience wrapper used by save_utils.py.
    """
    if not url or not save_directory or not file_id:
        return None

    try:
        os.makedirs(save_directory, exist_ok=True)

        extension = _infer_media_extension(url)
        filename = f"{file_id}{extension}"
        save_path = os.path.join(save_directory, filename)

        manager = get_media_manager()
        return manager.download_media(url, save_path)
    except Exception as exc:
        logging.error(f"Error in download_media_file for {url}: {exc}")
        return None
