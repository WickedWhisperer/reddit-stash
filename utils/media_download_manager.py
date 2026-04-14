from __future__ import annotations

import glob
import html
import logging
import os
import re
import subprocess
import threading
from typing import Any, Dict, List, Optional
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

REDGIFS_TRACE = os.getenv("REDGIFS_TRACE", "1").lower() not in {"0", "false", "no", "off"}


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

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

_URL_RE = re.compile(r'https?://[^\s"\'<>]+', re.IGNORECASE)
_OG_VIDEO_RE = re.compile(
    r'<meta[^>]+(?:property|name)=["\']og:video(?::url)?["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_TWITTER_STREAM_RE = re.compile(
    r'<meta[^>]+name=["\']twitter:player:stream["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_REDGIFS_MEDIA_RE = re.compile(
    r'https?://(?:media|thumbs\d*|thumbs)[^"\']*redgifs\.com[^"\']+',
    re.IGNORECASE,
)
_REDGIFS_DIRECT_MEDIA_RE = re.compile(
    r'https?://(?:media\.)?redgifs\.com/[^"\']+\.(?:mp4|m3u8|mpd|m4s)(?:\?[^"\']*)?',
    re.IGNORECASE,
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
    Extract the RedGifs ID from common watch / iframe URLs.
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


def _obj_get(obj: Any, key: str, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _dedupe_urls(urls: List[Optional[str]]) -> List[str]:
    seen = set()
    out: List[str] = []
    for url in urls:
        if not url:
            continue
        cleaned = html.unescape(str(url)).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def _looks_like_manifest(url: str) -> bool:
    lowered = url.lower()
    return lowered.endswith((".m3u8", ".mpd")) or ".m3u8?" in lowered or ".mpd?" in lowered


def _looks_like_direct_media(url: str) -> bool:
    lowered = url.lower()
    return lowered.endswith((".mp4", ".webm", ".mov", ".mkv")) or "-silent.mp4" in lowered


def _strip_redgifs_silent_suffix(url: str) -> str:
    """
    RedGifs page source often exposes ...-silent.mp4 for the silent variant.
    If the non-silent variant exists, it is usually the same URL without
    '-silent'.
    """
    cleaned = html.unescape(url or "").strip()
    return cleaned.replace("-silent.mp4", ".mp4")


class MediaDownloadManager:
    """
    Central coordinator for media downloads.

    Reddit-hosted media goes through RedditMediaDownloader.
    RedGifs is resolved via page-style URLs first, then verified with ffprobe so
    we only accept a file as audio-bearing when it really contains an audio stream.
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

    def _download_with_requests(self, url: str, save_path: str, referer: Optional[str] = None) -> Optional[str]:
        try:
            headers = {
                "User-Agent": USER_AGENT,
                "Accept": "*/*",
            }
            if referer:
                headers["Referer"] = referer
                headers["Origin"] = "https://www.redgifs.com"

            response = requests.get(
                url,
                stream=True,
                timeout=30,
                headers=headers,
                allow_redirects=True,
            )
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

    def _download_with_ytdlp(self, url: str, save_path: str, referer: Optional[str] = None) -> Optional[str]:
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
                "http_headers": {
                    "User-Agent": USER_AGENT,
                },
            }
            if referer:
                options["http_headers"]["Referer"] = referer

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

    def _trace_redgifs(self, message: str) -> None:
        text = f"[RedGifs] {message}"
        if REDGIFS_TRACE:
            print(text, flush=True)
        self._logger.info(text)

    def _fetch_html(self, url: str) -> Optional[str]:
        try:
            response = requests.get(
                url,
                timeout=30,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
                allow_redirects=True,
            )
            response.raise_for_status()
            return response.text
        except Exception as exc:
            self._logger.debug(f"Failed to fetch RedGifs HTML for {url}: {exc}")
            return None

    def _extract_redgifs_candidates_from_html(self, html_text: str) -> Dict[str, List[str]]:
        """
        Return candidate URLs separated into:
        - manifest URLs (.m3u8 / .mpd)
        - audio-suspected URLs (non-silent mp4, stripped silent mp4)
        - fallback URLs (silent mp4, other media)
        """
        manifests: List[str] = []
        audio_candidates: List[str] = []
        fallback_candidates: List[str] = []

        raw_urls = []
        raw_urls.extend(_OG_VIDEO_RE.findall(html_text))
        raw_urls.extend(_TWITTER_STREAM_RE.findall(html_text))
        raw_urls.extend(_REDGIFS_MEDIA_RE.findall(html_text))
        raw_urls.extend(_REDGIFS_DIRECT_MEDIA_RE.findall(html_text))
        raw_urls.extend([m.group(0) for m in _URL_RE.finditer(html_text) if "redgifs.com" in m.group(0).lower()])

        for raw in _dedupe_urls(raw_urls):
            candidate = html.unescape(raw).replace("\\/", "/").strip()

            if not candidate.startswith(("http://", "https://")):
                continue

            lowered = candidate.lower()

            if _looks_like_manifest(candidate):
                manifests.append(candidate)
                continue

            if _looks_like_direct_media(candidate):
                if "-silent.mp4" in lowered:
                    audio_candidates.append(_strip_redgifs_silent_suffix(candidate))
                    fallback_candidates.append(candidate)
                else:
                    audio_candidates.append(candidate)
                continue

        return {
            "manifests": _dedupe_urls(manifests),
            "audio_candidates": _dedupe_urls(audio_candidates),
            "fallback_candidates": _dedupe_urls(fallback_candidates),
        }

    def _build_redgifs_page_urls(self, url: str, gif_id: Optional[str], api_obj: Any) -> List[str]:
        candidates: List[Optional[str]] = [url]

        if gif_id and api_obj is not None:
            try:
                gif = api_obj.get_gif(gif_id)
            except Exception as exc:
                self._logger.debug(f"RedGifs metadata fetch failed for {gif_id}: {exc}")
                gif = None

            if gif:
                urls_obj = getattr(gif, "urls", None)
                candidates.extend(
                    [
                        _obj_get(urls_obj, "web_url"),
                        _obj_get(urls_obj, "embed_url"),
                    ]
                )

        return _dedupe_urls(candidates)

    def _download_redgifs_candidates(
        self,
        candidates: List[str],
        save_path: str,
        require_audio: bool,
        referer: Optional[str] = None,
        label: str = "candidate",
    ) -> Optional[str]:
        """
        Try candidate URLs in order. If require_audio is True, only accept a file
        that ffprobe confirms contains an audio stream.
        """
        first_silent: Optional[str] = None

        for candidate in candidates:
            self._trace_redgifs(
                f"trying {label}: {candidate} | require_audio={require_audio} | referer={referer or 'none'}"
            )

            downloaded = None

            if _looks_like_manifest(candidate):
                downloaded = self._download_with_ytdlp(candidate, save_path, referer=referer)
            else:
                downloaded = self._download_with_requests(candidate, save_path, referer=referer)

            if not downloaded:
                self._trace_redgifs(f"failed {label}: {candidate}")
                continue

            has_audio_stream = self._has_audio_stream(downloaded)
            self._trace_redgifs(
                f"downloaded {label}: {candidate} -> {downloaded} | audio_stream={has_audio_stream}"
            )

            if require_audio:
                if has_audio_stream:
                    self._trace_redgifs(f"accepted audio {label}: {candidate}")
                    return downloaded

                if first_silent is None:
                    first_silent = downloaded
                continue

            self._trace_redgifs(f"accepted {label}: {candidate}")
            return downloaded

        return first_silent if not require_audio else None

    def _download_redgifs(self, url: str, save_path: str) -> Optional[str]:
        """
        Resolve RedGifs from the page source first.

        The repo previously accepted silent direct media URLs too early. Now the
        flow is:

        1) Get page URLs (watch/web/embed)
        2) Fetch HTML
        3) Prefer stripped '-silent.mp4' candidates and manifest URLs
        4) Use ffprobe to verify audio when the GIF says it has audio
        5) Only then fall back to other candidates
        """
        gif_id = _extract_redgifs_id(url)
        api = self._get_redgifs_api()

        has_audio = False
        page_urls = self._build_redgifs_page_urls(url, gif_id, api)

        if gif_id and api is not None:
            try:
                gif = api.get_gif(gif_id)
            except Exception as exc:
                self._logger.debug(f"RedGifs metadata fetch failed for {gif_id}: {exc}")
                gif = None

            if gif:
                has_audio = bool(getattr(gif, "has_audio", False))
                self._trace_redgifs(f"clip {gif_id}: has_audio={has_audio}")
                urls_obj = getattr(gif, "urls", None)
                page_urls = _dedupe_urls(
                    page_urls
                    + [
                        _obj_get(urls_obj, "web_url"),
                        _obj_get(urls_obj, "embed_url"),
                    ]
                )

        self._trace_redgifs(f"page urls for {gif_id or url}: {page_urls}")

        manifest_candidates: List[str] = []
        audio_candidates: List[str] = []
        fallback_candidates: List[str] = []

        for page_url in page_urls:
            self._trace_redgifs(f"fetching page html: {page_url}")
            html_text = self._fetch_html(page_url)
            if not html_text:
                continue

            extracted = self._extract_redgifs_candidates_from_html(html_text)
            manifest_candidates.extend(extracted["manifests"])
            audio_candidates.extend(extracted["audio_candidates"])
            fallback_candidates.extend(extracted["fallback_candidates"])

        manifest_candidates = _dedupe_urls(manifest_candidates)
        audio_candidates = _dedupe_urls(audio_candidates)
        fallback_candidates = _dedupe_urls(fallback_candidates)

        self._trace_redgifs(f"manifests: {manifest_candidates}")
        self._trace_redgifs(f"audio candidates: {audio_candidates}")
        self._trace_redgifs(f"fallback candidates: {fallback_candidates}")

        referer = page_urls[0] if page_urls else None

        if has_audio:
            audio_first = self._download_redgifs_candidates(
                audio_candidates,
                save_path,
                require_audio=True,
                referer=referer,
                label="audio-candidate",
            )
            if audio_first:
                self._trace_redgifs(f"final selected audio file for {gif_id}: {audio_first}")
                return audio_first

            manifest_first = self._download_redgifs_candidates(
                manifest_candidates,
                save_path,
                require_audio=True,
                referer=referer,
                label="manifest",
            )
            if manifest_first:
                self._trace_redgifs(f"final selected manifest file for {gif_id}: {manifest_first}")
                return manifest_first

            direct_media_candidates = []
            for candidate in audio_candidates + fallback_candidates:
                if candidate not in direct_media_candidates:
                    direct_media_candidates.append(candidate)

            for candidate in direct_media_candidates:
                self._trace_redgifs(f"trying direct-media fallback: {candidate}")
                downloaded = self._download_with_requests(
                    candidate,
                    save_path,
                    referer=referer,
                )
                if downloaded:
                    has_audio_stream = self._has_audio_stream(downloaded)
                    self._trace_redgifs(
                        f"direct-media fallback downloaded {candidate} -> {downloaded} | audio_stream={has_audio_stream}"
                    )
                    if has_audio_stream:
                        self._trace_redgifs(f"final selected direct-media file for {gif_id}: {downloaded}")
                        return downloaded

            self._trace_redgifs(
                f"clip {gif_id} reported audio, but no extracted candidate contained an audio stream"
            )
            return None

        no_audio_first = self._download_redgifs_candidates(
            audio_candidates,
            save_path,
            require_audio=False,
            referer=referer,
            label="audio-candidate",
        )
        if no_audio_first:
            self._trace_redgifs(f"final selected file for {gif_id}: {no_audio_first}")
            return no_audio_first

        no_audio_first = self._download_redgifs_candidates(
            manifest_candidates,
            save_path,
            require_audio=False,
            referer=referer,
            label="manifest",
        )
        if no_audio_first:
            self._trace_redgifs(f"final selected manifest file for {gif_id}: {no_audio_first}")
            return no_audio_first

        no_audio_first = self._download_redgifs_candidates(
            fallback_candidates,
            save_path,
            require_audio=False,
            referer=referer,
            label="fallback",
        )
        if no_audio_first:
            self._trace_redgifs(f"final selected fallback file for {gif_id}: {no_audio_first}")
            return no_audio_first

        for candidate in page_urls:
            self._trace_redgifs(f"yt-dlp on page url: {candidate}")
            downloaded = self._download_with_ytdlp(candidate, save_path, referer=candidate)
            if downloaded:
                self._trace_redgifs(f"final selected page-url file for {gif_id}: {downloaded}")
                return downloaded

        self._trace_redgifs(f"no RedGifs source succeeded for {gif_id or url}")
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
                local_path = self._download_redgifs(url, save_path)
                if local_path:
                    with self._url_lock:
                        self._downloaded_urls[url] = local_path
                    return local_path

                self._record_failure(url)
                return None

            if _is_video_page(url):
                local_path = self._download_with_ytdlp(url, save_path, referer=url)
                if local_path:
                    with self._url_lock:
                        self._downloaded_urls[url] = local_path
                    return local_path

                local_path = self._download_with_requests(url, save_path, referer=url)
                if local_path:
                    with self._url_lock:
                        self._downloaded_urls[url] = local_path
                    return local_path

                self._record_failure(url)
                return None

            if _is_direct_image(url):
                local_path = self._download_with_requests(url, save_path, referer=url)
                if local_path:
                    with self._url_lock:
                        self._downloaded_urls[url] = local_path
                    return local_path

                self._record_failure(url)
                return None

            local_path = self._download_with_requests(url, save_path, referer=url)
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
            
