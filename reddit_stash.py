"""
Reddit media downloader service.

This module provides comprehensive support for downloading Reddit-hosted media
including i.redd.it images, v.redd.it videos (with audio merging), and gallery
posts. It now prefers yt-dlp for v.redd.it because Reddit's stream formats and
audio URLs change over time, and yt-dlp's extractor + ffmpeg muxing is more
robust than bespoke URL guessing alone.
"""

from __future__ import annotations

import glob
import logging
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import replace
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from ..constants import FFMPEG_TIMEOUT_SECONDS
from ..feature_flags import get_media_config as get_media_feature_config
from ..service_abstractions import (
    DownloadResult,
    DownloadStatus,
    MediaMetadata,
    MediaType,
    ServiceConfig,
)
from ..temp_file_utils import temp_files_cleanup
from .base_downloader import BaseHTTPDownloader

try:
    import yt_dlp  # type: ignore

    YTDLP_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    yt_dlp = None
    YTDLP_AVAILABLE = False

_logger = logging.getLogger(__name__)


class RedditMediaDownloader(BaseHTTPDownloader):
    """
    Reddit media downloader supporting i.redd.it, v.redd.it, and galleries.

    The downloader now tries yt-dlp first for Reddit-hosted videos because
    it can follow Reddit's current manifest format more robustly than
    handcrafted audio-URL guessing. If yt-dlp cannot handle a URL, the legacy
    direct downloader is used as a fallback.
    """

    def __init__(self, config: Optional[ServiceConfig] = None):
        if config is None:
            config = ServiceConfig(
                name="Reddit",
                rate_limit_per_minute=100,
                timeout_seconds=30,
                max_file_size=209715200,
                user_agent="Reddit Stash Media Downloader/1.0",
                max_redirects=5,
                connect_timeout=5.0,
                read_timeout=30.0,
                allowed_content_types=["image/*", "video/*", "audio/*"],
                verify_ssl=True,
            )
        super().__init__(config)
        self._media_config = get_media_feature_config()

    def _get_media_size_limit(self, media_kind: str) -> int:
        """Return the configured size limit for the requested media kind."""
        default_limit = self.config.max_file_size
        try:
            media_cfg = self._media_config.get_media_config()
        except Exception:
            media_cfg = {}

        if not media_cfg or not media_cfg.get("media_enabled", False):
            return default_limit

        if media_kind in ("image", "preview"):
            return int(media_cfg.get("max_image_size", default_limit) or default_limit)
        if media_kind == "video":
            return int(media_cfg.get("max_video_size", default_limit) or default_limit)
        if media_kind == "audio":
            return int(media_cfg.get("max_video_size", default_limit) or default_limit)
        return default_limit

    def can_handle(self, url: str) -> bool:
        """Check if this service can handle the given URL."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            reddit_domains = [
                "i.redd.it",
                "v.redd.it",
                "preview.redd.it",
                "external-preview.redd.it",
            ]
            return any(domain.endswith(reddit_domain) for reddit_domain in reddit_domains)
        except Exception:
            return False

    def get_metadata(self, url: str) -> Optional[MediaMetadata]:
        """Get metadata for Reddit media without downloading."""
        if not self.can_handle(url):
            return None

        try:
            self._respect_rate_limit()
            response = self._session.head(
                url,
                timeout=(5.0, 10.0),
                allow_redirects=True,
            )
            response.raise_for_status()
            media_type = self._determine_reddit_media_type(url, response.headers)
            file_size = int(response.headers.get("content-length", 0))
            return MediaMetadata(
                url=url,
                media_type=media_type,
                file_size=file_size,
                format=self._get_file_extension_from_headers(response.headers),
            )
        except Exception:
            return MediaMetadata(
                url=url,
                media_type=self._determine_reddit_media_type(url, {}),
                file_size=None,
            )

    def download(self, url: str, save_path: str) -> DownloadResult:
        """Download Reddit media with appropriate handling for different types."""
        if not self.can_handle(url):
            return DownloadResult(
                status=DownloadStatus.INVALID_URL,
                error_message=f"Cannot handle URL: {url}",
            )

        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()

            if "v.redd.it" in domain:
                return self._download_reddit_video(url, save_path)
            if "i.redd.it" in domain:
                return self._download_reddit_image(url, save_path)
            if "preview.redd.it" in domain or "external-preview.redd.it" in domain:
                return self._download_reddit_preview(url, save_path)

            return self.download_file(url, save_path)
        except Exception as e:
            return DownloadResult(
                status=DownloadStatus.FAILED,
                error_message=f"Reddit media download failed: {str(e)}",
            )

    def _determine_reddit_media_type(self, url: str, headers: Dict[str, str]) -> MediaType:
        """Determine media type from Reddit URL patterns."""
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if "v.redd.it" in domain:
            return MediaType.VIDEO
        if "i.redd.it" in domain:
            return MediaType.IMAGE
        return self._detect_media_type(headers)

    def _download_reddit_image(self, url: str, save_path: str) -> DownloadResult:
        """Download Reddit hosted image (i.redd.it) with optimized headers."""
        return self._download_with_reddit_headers(
            url,
            save_path,
            max_file_size=self._get_media_size_limit("image"),
        )

    def _download_reddit_preview(self, url: str, save_path: str) -> DownloadResult:
        """Download Reddit preview image with URL decoding and optimized headers."""
        try:
            cleaned_url = url.replace("amp;", "")
            return self._download_with_reddit_headers(
                cleaned_url,
                save_path,
                max_file_size=self._get_media_size_limit("preview"),
            )
        except Exception as e:
            return DownloadResult(
                status=DownloadStatus.FAILED,
                error_message=f"Preview download failed: {str(e)}",
            )

    def _download_reddit_video(self, url: str, save_path: str) -> DownloadResult:
        """
        Download Reddit hosted video (v.redd.it).

        Strategy:
        1) Try yt-dlp first on the Reddit DASH playlist because it is the most
           resilient way to fetch and mux Reddit-hosted streams.
        2) Fall back to the older direct download / audio-discovery path.
        """
        _logger.debug(f"Starting Reddit video download: {url}")
        video_limit = self._get_media_size_limit("video")

        if self._is_ffmpeg_available() and YTDLP_AVAILABLE:
            ytdlp_result = self._download_reddit_video_with_ytdlp(url, save_path, video_limit)
            if ytdlp_result.is_success:
                return ytdlp_result
            _logger.debug(
                "yt-dlp Reddit download did not succeed, falling back to legacy path: %s",
                ytdlp_result.error_message,
            )

        try:
            parsed = urlparse(url)
            is_short_url = "DASH_" not in parsed.path

            if is_short_url:
                _logger.info(f"Short v.redd.it URL detected (no DASH_ segment): {url}")
                video_result = self.download_file(url, save_path, max_file_size=video_limit)
                if not video_result.is_success:
                    error = video_result.error_message or ""
                    if "403" in error or "Forbidden" in error:
                        _logger.warning(f"Short v.redd.it URL returned 403, failing fast: {url}")
                    return video_result
                return video_result

            has_ffmpeg = self._is_ffmpeg_available()
            audio_url = self._get_audio_url_from_video_url(url) if has_ffmpeg else None

            if audio_url:
                with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_audio:
                    temp_audio_path = temp_audio.name
                with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_video:
                    temp_video_path = temp_video.name

                try:
                    with temp_files_cleanup(temp_audio_path), temp_files_cleanup(temp_video_path):
                        from concurrent.futures import ThreadPoolExecutor

                        with ThreadPoolExecutor(max_workers=2) as pool:
                            video_future = pool.submit(
                                self.download_file,
                                url,
                                temp_video_path,
                                None,
                                None,
                                video_limit,
                            )
                            audio_future = pool.submit(
                                self.download_file,
                                audio_url,
                                temp_audio_path,
                                None,
                                None,
                                video_limit,
                            )

                            video_result = video_future.result()
                            audio_result = audio_future.result()

                        if not video_result.is_success:
                            if "DASH_" in url:
                                error = video_result.error_message or ""
                                if "404" in error or "Not Found" in error:
                                    _logger.info("Video DASH quality failed with 404, trying alternatives")
                                    video_result = self._try_dash_qualities(
                                        url, temp_video_path, max_file_size=video_limit
                                    )
                            if not video_result.is_success:
                                return video_result

                        if audio_result.is_success:
                            merged_result = self._merge_video_audio(
                                video_result.local_path or temp_video_path,
                                audio_result.local_path or temp_audio_path,
                                save_path,
                            )
                            if merged_result.is_success:
                                return replace(
                                    video_result,
                                    local_path=merged_result.local_path,
                                    bytes_downloaded=(
                                        (video_result.bytes_downloaded or 0)
                                        + (audio_result.bytes_downloaded or 0)
                                    ),
                                )

                            _logger.warning(
                                f"ffmpeg merge failed for Reddit video; returning video-only file: {merged_result.error_message}"
                            )
                        else:
                            _logger.warning(
                                f"Reddit audio download failed; returning video-only file: {audio_result.error_message}"
                            )

                        return video_result
                finally:
                    pass
            else:
                video_result = self.download_file(url, save_path, max_file_size=video_limit)
                if not video_result.is_success and "DASH_" in url:
                    error = video_result.error_message or ""
                    if "404" in error or "Not Found" in error:
                        _logger.info("Initial DASH quality failed with 404, trying alternatives")
                        video_result = self._try_dash_qualities(
                            url, save_path, max_file_size=video_limit
                        )

                if video_result.is_success and not has_ffmpeg:
                    return replace(video_result, error_message="Audio track not merged (ffmpeg not available)")

                return video_result

        except Exception as e:
            return DownloadResult(
                status=DownloadStatus.FAILED,
                error_message=f"Reddit video download failed: {str(e)}",
            )

    def _download_reddit_video_with_ytdlp(
        self,
        url: str,
        save_path: str,
        video_limit: int,
    ) -> DownloadResult:
        """
        Use yt-dlp as the first-choice Reddit video downloader.

        yt-dlp can follow Reddit's current manifest format more robustly than
        handcrafted audio-URL guessing, and ffmpeg handles muxing.
        """
        if not YTDLP_AVAILABLE:
            return DownloadResult(
                status=DownloadStatus.FAILED,
                error_message="yt-dlp is not installed",
            )

        playlist_url = self._build_reddit_playlist_url(url)
        candidate_urls = [u for u in [playlist_url, url] if u]

        for candidate in candidate_urls:
            try:
                with tempfile.TemporaryDirectory(prefix="reddit_ytdlp_") as tmpdir:
                    base_name = os.path.splitext(os.path.basename(save_path))[0]
                    outtmpl = os.path.join(tmpdir, base_name)

                    ydl_opts: Dict[str, Any] = {
                        "format": "bv*+ba/b",
                        "merge_output_format": "mp4",
                        "outtmpl": outtmpl,
                        "noplaylist": True,
                        "quiet": True,
                        "no_warnings": True,
                        "retries": 3,
                        "fragment_retries": 3,
                        "socket_timeout": self.config.read_timeout,
                        "http_headers": {
                            "User-Agent": self.config.user_agent,
                            "Referer": "https://www.reddit.com/",
                            "Origin": "https://www.reddit.com",
                            "Accept-Language": "en-US,en;q=0.9",
                        },
                    }

                    _logger.info(f"Attempting yt-dlp Reddit download: {candidate}")
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:  # type: ignore[attr-defined]
                        ydl.download([candidate])

                    candidates = sorted(
                        [
                            p
                            for p in glob.glob(os.path.join(tmpdir, f"{base_name}*"))
                            if os.path.isfile(p) and not p.endswith(".part")
                        ],
                        key=os.path.getmtime,
                        reverse=True,
                    )

                    if not candidates:
                        continue

                    downloaded_path = None
                    for p in candidates:
                        if p.lower().endswith(".mp4"):
                            downloaded_path = p
                            break
                    if downloaded_path is None:
                        downloaded_path = candidates[0]

                    os.makedirs(os.path.dirname(save_path), exist_ok=True)
                    if os.path.abspath(downloaded_path) != os.path.abspath(save_path):
                        shutil.move(downloaded_path, save_path)

                    if not os.path.exists(save_path):
                        continue

                    file_size = os.path.getsize(save_path)
                    if video_limit and file_size > video_limit:
                        try:
                            os.remove(save_path)
                        except OSError:
                            pass
                        return DownloadResult(
                            status=DownloadStatus.FAILED,
                            error_message=(
                                f"yt-dlp output exceeded configured video size limit "
                                f"({file_size:,} > {video_limit:,} bytes)"
                            ),
                        )

                    if self._has_audio_stream(save_path):
                        _logger.info(f"yt-dlp produced video with audio: {save_path}")
                    else:
                        _logger.warning(
                            "yt-dlp produced a video file but no audio stream was detected. "
                            "This can happen on genuinely silent or GIF-converted Reddit posts."
                        )

                    return DownloadResult(
                        status=DownloadStatus.SUCCESS,
                        local_path=save_path,
                        bytes_downloaded=file_size,
                        metadata=MediaMetadata(
                            url=candidate,
                            media_type=MediaType.VIDEO,
                            file_size=file_size,
                            format="mp4",
                        ),
                    )
            except Exception as e:
                _logger.debug(f"yt-dlp candidate failed for {candidate}: {e}")
                continue

        return DownloadResult(
            status=DownloadStatus.FAILED,
            error_message="yt-dlp could not download a playable Reddit video",
        )

    @staticmethod
    def _build_reddit_playlist_url(video_url: str) -> Optional[str]:
        """Build a DASHPlaylist.mpd URL from a Reddit video URL."""
        try:
            parsed = urlparse(video_url)
            parts = [p for p in parsed.path.split("/") if p]
            if not parts:
                return None
            video_id = parts[0]
            return f"{parsed.scheme}://{parsed.netloc}/{video_id}/DASHPlaylist.mpd"
        except Exception:
            return None

    def _get_audio_url_from_video_url(self, video_url: str) -> Optional[str]:
        """
        Generate the best audio URL from a Reddit video URL.

        First tries the DASHPlaylist.mpd manifest because Reddit often exposes
        the exact audio URL there. Falls back to common historical audio
        filenames if the manifest cannot be read.
        """
        import xml.etree.ElementTree as ET

        try:
            parsed = urlparse(video_url)
            path_parts = [p for p in parsed.path.split("/") if p]

            if not path_parts:
                return None

            playlist_url = self._build_reddit_playlist_url(video_url)
            if not playlist_url:
                return None

            def _normalize_candidate(candidate: str, parsed_url) -> str:
                candidate = candidate.strip()
                if candidate.startswith("/"):
                    candidate = f"{parsed_url.scheme}://{parsed_url.netloc}{candidate}"
                elif candidate.startswith("//"):
                    candidate = f"{parsed_url.scheme}:{candidate}"
                if parsed_url.query and "?" not in candidate:
                    candidate = f"{candidate}?{parsed_url.query}"
                return candidate

            def _candidate_works(candidate_url: str) -> bool:
                try:
                    resp = self._session.get(
                        candidate_url,
                        timeout=(5.0, 10.0),
                        allow_redirects=True,
                        stream=False,
                    )
                    if resp.status_code != 200:
                        return False
                    content_type = (resp.headers.get("content-type") or "").lower()
                    if not content_type:
                        return True
                    return (
                        content_type.startswith("audio/")
                        or content_type.startswith("video/")
                        or "application/octet-stream" in content_type
                    )
                except Exception:
                    return False

            try:
                resp = self._session.get(
                    playlist_url,
                    timeout=(3.0, 10.0),
                    allow_redirects=True,
                    headers={
                        "Accept": "application/dash+xml,application/xml;q=0.9,*/*;q=0.8"
                    },
                )
                if resp.status_code == 200 and resp.text:
                    playlist_text = resp.text
                    playlist_text = re.sub(
                        r'\sxmlns="[^"]+"',
                        "",
                        playlist_text,
                        count=1,
                    )

                    try:
                        root = ET.fromstring(playlist_text)
                        for adaptation in root.iter():
                            tag = adaptation.tag.split("}")[-1].lower()
                            mime_type = (
                                adaptation.attrib.get("mimeType", "")
                                or adaptation.attrib.get("contentType", "")
                            ).lower()
                            if tag != "adaptationset":
                                continue
                            if "audio" not in mime_type:
                                continue

                            for base_url in adaptation.iter():
                                base_tag = base_url.tag.split("}")[-1].lower()
                                if base_tag != "baseurl":
                                    continue
                                candidate = (base_url.text or "").strip()
                                if not candidate:
                                    continue
                                candidate = _normalize_candidate(candidate, parsed)
                                if _candidate_works(candidate):
                                    _logger.debug(
                                        f"Found audio track from DASH playlist XML: {candidate}"
                                    )
                                    return candidate
                    except ET.ParseError:
                        _logger.debug(
                            f"DASH playlist XML parse failed for {video_url}; falling back to regex parsing"
                        )

                    manifest_patterns = [
                        r'(?is)<AdaptationSet[^>]*(?:mimeType|contentType)="[^"]*audio[^"]*"[^>]*>(.*?)</AdaptationSet>',
                        r'(?is)(?:mimeType|contentType)="[^"]*audio[^"]*".*?(<BaseURL>.*?</BaseURL>)',
                    ]
                    for pattern in manifest_patterns:
                        blocks = re.findall(pattern, playlist_text)
                        for block in blocks:
                            candidates = re.findall(
                                r"(?is)<BaseURL>(.*?)</BaseURL>|([^<\"']+\.mp4[^<\"']*)",
                                block,
                            )
                            for c1, c2 in candidates:
                                candidate = c1 or c2
                                if not candidate:
                                    continue
                                candidate = _normalize_candidate(candidate, parsed)
                                if _candidate_works(candidate):
                                    _logger.debug(
                                        f"Found audio track from DASH playlist regex: {candidate}"
                                    )
                                    return candidate
            except Exception as e:
                _logger.debug(f"DASH playlist lookup failed for {video_url}: {e}")

            if "DASH_" not in parsed.path:
                return None

            dash_index = None
            for i, part in enumerate(path_parts):
                if "DASH_" in part and ".mp4" in part:
                    dash_index = i
                    break

            if dash_index is None:
                return None

            audio_filenames = [
                "DASH_audio.mp4",
                "DASH_AUDIO_128.mp4",
                "DASH_AUDIO_64.mp4",
            ]

            for audio_filename in audio_filenames:
                candidate_parts = path_parts.copy()
                candidate_parts[dash_index] = audio_filename
                audio_path = "/" + "/".join(candidate_parts)
                audio_url = _normalize_candidate(audio_path, parsed)

                if _candidate_works(audio_url):
                    _logger.debug(f"Found audio track via fallback filename: {audio_filename}")
                    return audio_url

            _logger.warning(f"No audio track found for video: {video_url}")
            return None

        except Exception:
            return None

    def _try_dash_qualities(
        self, url: str, save_path: str, max_file_size: Optional[int] = None
    ) -> DownloadResult:
        """
        Try multiple DASH quality tiers when the initial quality returns 404.

        Replaces DASH_NNN in the URL with progressively lower quality tiers.
        """
        qualities = [720, 480, 360, 240]
        last_result = None

        for quality in qualities:
            candidate = re.sub(r"DASH_\d+", f"DASH_{quality}", url)
            if candidate == url:
                continue
            _logger.debug(f"Trying DASH quality fallback: DASH_{quality}")
            result = self.download_file(candidate, save_path, max_file_size=max_file_size)
            last_result = result
            if result.is_success:
                _logger.info(f"DASH quality fallback succeeded at {quality}p")
                return result

            error = result.error_message or ""
            if "404" not in error and "Not Found" not in error:
                break

        return last_result or DownloadResult(
            status=DownloadStatus.FAILED,
            error_message="All DASH quality tiers failed",
        )

    def _download_with_reddit_headers(
        self,
        url: str,
        save_path: str,
        max_file_size: Optional[int] = None,
    ) -> DownloadResult:
        """Download Reddit images with optimal headers to prevent HTML wrapper pages."""
        reddit_headers = {
            "Accept": "image/*,*/*;q=0.8",
            "User-Agent": self.config.user_agent,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
        }
        return self.download_file(
            url,
            save_path,
            extra_headers=reddit_headers,
            max_file_size=max_file_size,
        )

    def _is_ffmpeg_available(self) -> bool:
        """Check if ffmpeg is available in system PATH."""
        try:
            subprocess.run(
                ["ffmpeg", "-version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    def _has_audio_stream(self, file_path: str) -> bool:
        """Best-effort check for an audio stream in a media file."""
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

    def _merge_video_audio(
        self, video_path: str, audio_path: str, output_path: str
    ) -> DownloadResult:
        """Merge video and audio files using ffmpeg."""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            cmd = [
                "ffmpeg",
                "-i",
                video_path,
                "-i",
                audio_path,
                "-c:v",
                "copy",
                "-c:a",
                "aac",
                "-shortest",
                "-y",
                output_path,
            ]

            result = subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=FFMPEG_TIMEOUT_SECONDS,
                text=True,
            )

            if result.returncode == 0:
                file_size = os.path.getsize(output_path)
                return DownloadResult(
                    status=DownloadStatus.SUCCESS,
                    local_path=output_path,
                    bytes_downloaded=file_size,
                )

            return DownloadResult(
                status=DownloadStatus.FAILED,
                error_message=f"ffmpeg failed: {result.stderr}",
            )
        except subprocess.TimeoutExpired:
            return DownloadResult(
                status=DownloadStatus.FAILED,
                error_message="Video merge timed out",
            )
        except Exception as e:
            return DownloadResult(
                status=DownloadStatus.FAILED,
                error_message=f"Video merge failed: {str(e)}",
            )

    def get_service_name(self) -> str:
        return "Reddit"

    def is_rate_limited(self) -> bool:
        return False

    def get_rate_limit_reset_time(self) -> Optional[float]:
        return None

    @staticmethod
    def _get_best_reddit_video_url(submission) -> Optional[str]:
        """
        Return the most useful Reddit video URL for a submission.

        Reddit often exposes a richer `fallback_url` or `dash_url` in
        `submission.media['reddit_video']` or `submission.secure_media['reddit_video']`.
        """
        try:
            for attr in ("media", "secure_media"):
                data = getattr(submission, attr, None)
                if not isinstance(data, dict):
                    continue
                reddit_video = data.get("reddit_video")
                if not isinstance(reddit_video, dict):
                    continue
                for key in ("fallback_url", "dash_url"):
                    candidate = reddit_video.get(key)
                    if candidate:
                        return candidate.replace("&amp;", "&")
        except Exception:
            pass
        return getattr(submission, "url", None)

    @classmethod
    def extract_media_urls_from_submission(cls, submission) -> List[Dict[str, Any]]:
        """Extract all media URLs from a PRAW submission."""
        media_urls: List[Dict[str, Any]] = []
        try:
            if hasattr(submission, "is_reddit_media_domain") and submission.is_reddit_media_domain:
                if hasattr(submission, "domain"):
                    if submission.domain == "i.redd.it":
                        media_urls.append(
                            {"url": submission.url, "type": "image", "source": "reddit_direct"}
                        )
                    elif submission.domain == "v.redd.it":
                        video_url = cls._get_best_reddit_video_url(submission)
                        media_urls.append(
                            {
                                "url": video_url or submission.url,
                                "type": "video",
                                "source": "reddit_direct",
                                "fallback_used": bool(video_url and video_url != submission.url),
                            }
                        )

            if hasattr(submission, "is_gallery") and submission.is_gallery:
                if hasattr(submission, "media_metadata") and submission.media_metadata:
                    for item_id, metadata in submission.media_metadata.items():
                        if "s" in metadata and "u" in metadata["s"]:
                            gallery_url = metadata["s"]["u"].replace("&amp;", "&")
                            media_urls.append(
                                {
                                    "url": gallery_url,
                                    "type": "image",
                                    "source": "reddit_gallery",
                                    "gallery_id": item_id,
                                }
                            )

            if hasattr(submission, "preview") and submission.preview and "images" in submission.preview:
                for image in submission.preview["images"]:
                    if "source" in image:
                        preview_url = image["source"]["url"].replace("&amp;", "&")
                        media_urls.append(
                            {
                                "url": preview_url,
                                "type": "image",
                                "source": "reddit_preview",
                                "width": image["source"].get("width"),
                                "height": image["source"].get("height"),
                            }
                        )
        except Exception as e:
            logging.getLogger(__name__).warning(f"Error extracting media URLs from submission: {e}")

        return media_urls

            
