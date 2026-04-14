from __future__ import annotations

import glob
import logging
import os
import re
import subprocess
import tempfile
import threading
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests

from .media_services.reddit_media import RedditMediaDownloader

logger = logging.getLogger(__name__)

try:
    import yt_dlp  # type: ignore

    YT_DLP_AVAILABLE = True
except Exception:
    yt_dlp = None
    YT_DLP_AVAILABLE = False

try:
    from redgifs import API as RedgifsAPI  # type: ignore

    REDGIFS_AVAILABLE = True
except Exception:
    RedgifsAPI = None
    REDGIFS_AVAILABLE = False


REDDIT_MEDIA_HOSTS = (
    "i.redd.it",
    "v.redd.it",
    "preview.redd.it",
    "external-preview.redd.it",
)

REDGIFS_HOSTS = (
    "redgifs.com",
)

VIDEO_PAGE_HOSTS = (
    "gfycat.com",
    "giphy.com",
    "streamable.com",
    "imgur.com",
)


def _host(url: str) -> str:
    try:
        return urlparse(url or "").netloc.lower()
    except Exception:
        return ""


def _path(url: str) -> str:
    try:
        return urlparse(url or "").path.lower()
    except Exception:
        return ""


def _is_reddit_media(url: str) -> bool:
    domain = _host(url)
    return any(domain.endswith(host) for host in REDDIT_MEDIA_HOSTS)


def _is_redgifs(url: str) -> bool:
    domain = _host(url)
    return any(domain.endswith(host) for host in REDGIFS_HOSTS)


def _is_video_page(url: str) -> bool:
    domain = _host(url)
    path = _path(url)

    if not domain:
        return False

    if any(domain.endswith(host) for host in VIDEO_PAGE_HOSTS):
        return True

    if path.endswith(".gifv"):
        return True

    return False


def _is_direct_image(url: str) -> bool:
    domain = _host(url)
    path = _path(url)

    if path.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff")):
        return True

    if any(
        domain.endswith(host)
        for host in ("i.redd.it", "i.imgur.com", "preview.redd.it", "external-preview.redd.it")
    ):
        return True

    return False


def _infer_media_extension(url: str) -> str:
    """
    Pick a sensible extension for the initial save path.
    yt-dlp or the RedGifs API may still write a final mp4.
    """
    try:
        parsed = urlparse(url or "")
        domain = parsed.netloc.lower()
        path = parsed.path.lower()
        filename = os.path.basename(path)
        _, ext = os.path.splitext(filename)

        if ext == ".gifv":
            return ".mp4"

        if ext in {
            ".jpg",
            ".jpeg",
            ".png",
            ".gif",
            ".webp",
            ".bmp",
            ".tiff",
            ".mp4",
            ".webm",
            ".mov",
            ".mkv",
        }:
            return ext

        if "v.redd.it" in domain:
            return ".mp4"

        if domain.endswith(("redgifs.com", "gfycat.com", "giphy.com", "streamable.com")):
            return ".mp4"

        if domain.endswith(("i.redd.it", "preview.redd.it", "external-preview.redd.it", "i.imgur.com")):
            return ".jpg"

        return ".jpg"
    except Exception:
        return ".jpg"


def _extract_redgifs_id(url: str) -> Optional[str]:
    """
    Extract the RedGifs id from common watch / iframe URLs.
    """
    if not url:
        return None

    cleaned = url.split("?")[0].split("#")[0].strip()
    match = re.search(r"/(?:watch|ifr)/([A-Za-z0-9]+)", cleaned, re.IGNORECASE)
    if match:
        return match.group(1)

    path = urlparse(cleaned).path.strip("/")
    if path:
        last = path.split("/")[-1]
        if last and last.lower() not in {"watch", "ifr"}:
            return last

    return None


class MediaDownloadManager:
    """
    Central coordinator for media downloads.

    Reddit-hosted media goes through RedditMediaDownloader.
    RedGifs gets a dedicated API-based path first, because the API exposes
    whether audio exists and provides the media URLs that are meant to be used
    for download.
    External video/GIF hosts go through yt-dlp when available.
    """

    def __init__(self):
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._url_lock = threading.Lock()
        self._downloaded_urls: Dict[str, str] = {}
        self._failed_urls: Dict[str, int] = {}
        self._reddit_downloader = RedditMediaDownloader()
        self._redgifs_api = None
        self._redgifs_lock = threading.Lock()

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

    def _download_with_ytdlp(self, url: str, save_path: str) -> Optional[str]:
        if not YT_DLP_AVAILABLE:
            return None

        try:
            output_dir = os.path.dirname(save_path) or "."
            base_name = os.path.splitext(os.path.basename(save_path))[0]
            outtmpl = os.path.join(output_dir, f"{base_name}.%(ext)s")

            options = {
                "outtmpl": outtmpl,
                "format": "bv*+ba/b",
                "merge_output_format": "mp4",
                "noplaylist": True,
                "quiet": True,
                "no_warnings": True,
                "retries": 3,
                "socket_timeout": 30,
                "paths": {"home": output_dir},
            }

            os.makedirs(output_dir, exist_ok=True)

            with yt_dlp.YoutubeDL(options) as ydl:  # type: ignore[attr-defined]
                ydl.extract_info(url, download=True)

            candidates = []
            for pattern in (
                os.path.join(output_dir, f"{base_name}.*"),
                os.path.join(output_dir, f"{base_name}*.mp4"),
                os.path.join(output_dir, f"{base_name}*.webm"),
                os.path.join(output_dir, f"{base_name}*.mkv"),
                os.path.join(output_dir, f"{base_name}*.mov"),
            ):
                candidates.extend(glob.glob(pattern))

            candidates = [
                path
                for path in candidates
                if os.path.isfile(path) and not path.endswith(".part") and not path.endswith(".ytdl")
            ]

            if not candidates:
                if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                    return save_path
                return None

            candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            final_path = candidates[0]

            if final_path != save_path:
                try:
                    if os.path.exists(save_path):
                        os.remove(save_path)
                    os.replace(final_path, save_path)
                    final_path = save_path
                except Exception:
                    pass

            if os.path.exists(final_path) and os.path.getsize(final_path) > 0:
                return final_path
            return None
        except Exception as exc:
            self._logger.debug(f"yt-dlp download failed for {url}: {exc}")
            return None

    def _get_redgifs_api(self):
        if not REDGIFS_AVAILABLE:
            return None

        with self._redgifs_lock:
            if self._redgifs_api is not None:
                return self._redgifs_api

            try:
                api = RedgifsAPI()
                api.login()
                self._redgifs_api = api
                return self._redgifs_api
            except Exception as exc:
                self._logger.debug(f"RedGifs API login failed: {exc}")
                self._redgifs_api = None
                return None

    def _has_audio_stream(self, file_path: str) -> bool:
        """
        Best-effort check for an audio stream in a downloaded media file.
        """
        try:
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "a:0",
                    "-show_entries",
                    "stream=index",
                    "-of",
                    "csv=p=0",
                    file_path,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                check=False,
            )
            return bool((result.stdout or "").strip())
        except Exception:
            return False

    def _download_redgifs_with_api(self, url: str, save_path: str) -> Optional[str]:
        """
        Download RedGifs through the official API client when available.

        This is the audio-preserving path. The API exposes the GIF metadata,
        including whether the clip has audio, and provides the URLs that should
        be downloaded.
        """
        api = self._get_redgifs_api()
        if api is None:
            return None

        gif_id = _extract_redgifs_id(url)
        if not gif_id:
            return None

        try:
            gif = api.get_gif(gif_id)
        except Exception as exc:
            self._logger.debug(f"RedGifs metadata fetch failed for {gif_id}: {exc}")
            return None

        has_audio = bool(getattr(gif, "has_audio", False))
        self._logger.info(f"RedGifs clip {gif_id}: has_audio={has_audio}")

        urls_obj = getattr(gif, "urls", None)
        candidate_urls = []
        if urls_obj is not None:
            for attr in ("hd", "file_url", "sd", "embed_url"):
                candidate = getattr(urls_obj, attr, None)
                if candidate and candidate not in candidate_urls:
                    candidate_urls.append(candidate)

        if url not in candidate_urls:
            candidate_urls.append(url)

        output_dir = os.path.dirname(save_path) or "."
        os.makedirs(output_dir, exist_ok=True)

        for candidate in candidate_urls:
            tmp_path = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4", dir=output_dir) as tmp:
                    tmp_path = tmp.name

                api.download(candidate, tmp_path)

                if not os.path.exists(tmp_path) or os.path.getsize(tmp_path) <= 0:
                    continue

                if has_audio and not self._has_audio_stream(tmp_path):
                    self._logger.warning(
                        f"RedGifs clip {gif_id} expected audio, but ffprobe found none for {candidate}"
                    )
                    continue

                if os.path.exists(save_path):
                    os.remove(save_path)
                os.replace(tmp_path, save_path)
                return save_path
            except Exception as exc:
                self._logger.debug(f"RedGifs download attempt failed for {gif_id} via {candidate}: {exc}")
            finally:
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass

        return None

    def download_media(self, url: str, save_path: str) -> Optional[str]:
        """
        Download media and return the saved file path, or None on failure.
        """
        if not url or not save_path:
            return None

        with self._url_lock:
            if self._should_skip_url(url):
                return None

            if url in self._downloaded_urls:
                cached = self._downloaded_urls[url]
                if os.path.exists(cached) and os.path.getsize(cached) > 0:
                    return cached
                self._downloaded_urls.pop(url, None)

        try:
            if _is_reddit_media(url):
                result = self._reddit_downloader.download(url, save_path)
                if getattr(result, "is_success", False) and getattr(result, "local_path", None):
                    local_path = result.local_path
                    with self._url_lock:
                        self._downloaded_urls[url] = local_path
                    return local_path

                self._record_failure(url)
                return None

            if _is_redgifs(url):
                local_path = self._download_redgifs_with_api(url, save_path)
                if not local_path:
                    local_path = self._download_with_ytdlp(url, save_path)

                if local_path:
                    with self._url_lock:
                        self._downloaded_urls[url] = local_path
                    return local_path

                self._record_failure(url)
                return None

            if _is_video_page(url):
                local_path = self._download_with_ytdlp(url, save_path)
                if local_path:
                    with self._url_lock:
                        self._downloaded_urls[url] = local_path
                    return local_path

                local_path = self._download_with_requests(url, save_path)
                if local_path:
                    with self._url_lock:
                        self._downloaded_urls[url] = local_path
                    return local_path

                self._record_failure(url)
                return None

            if _is_direct_image(url):
                local_path = self._download_with_requests(url, save_path)
                if local_path:
                    with self._url_lock:
                        self._downloaded_urls[url] = local_path
                    return local_path

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
            "redgifs_api": REDGIFS_AVAILABLE,
            "yt_dlp_available": YT_DLP_AVAILABLE,
            "cached_urls": len(self._downloaded_urls),
            "failed_urls": len(self._failed_urls),
        }

    def is_service_available(self, service_name: str) -> bool:
        if service_name == "reddit":
            return self._reddit_downloader is not None
        if service_name == "redgifs":
            return REDGIFS_AVAILABLE or YT_DLP_AVAILABLE
        if service_name == "yt_dlp":
            return YT_DLP_AVAILABLE
        return True

    def reset_service(self, service_name: str) -> None:
        if service_name == "reddit":
            try:
                self._reddit_downloader = RedditMediaDownloader()
            except Exception:
                self._reddit_downloader = None
        elif service_name == "redgifs":
            with self._redgifs_lock:
                self._redgifs_api = None

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
