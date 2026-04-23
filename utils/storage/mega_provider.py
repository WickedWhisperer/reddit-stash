"""MEGA storage backend implemented via rclone."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from typing import List, Optional

from .base import StorageFileInfo, StorageProviderProtocol, SyncResult


class MegaStorageProvider(StorageProviderProtocol):
    """MEGA implementation backed by rclone."""

    LEGACY_LOG_NAME = "log.json"
    CANONICAL_LOG_NAME = "file_log.json"

    def __init__(self, mega_remote: str = "mega"):
        self._mega_remote = mega_remote
        self._connected = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _require_rclone(self) -> None:
        if shutil.which("rclone") is None:
            raise RuntimeError(
                "rclone is not installed. Install it before using the MEGA storage provider."
            )

    def _remote_prefix(self, remote_directory: str = "") -> str:
        directory = (remote_directory or "").strip().lstrip("/")
        if directory:
            return f"{self._mega_remote}:{directory}"
        return f"{self._mega_remote}:"

    def _remote_join(self, remote_directory: str, filename: str) -> str:
        remote_spec = self._remote_prefix(remote_directory)
        if remote_spec.endswith(":"):
            return f"{remote_spec}{filename}"
        return f"{remote_spec}/{filename}"

    def _run(self, args: list[str], *, check: bool = False) -> subprocess.CompletedProcess[str]:
        self._require_rclone()
        proc = subprocess.run(
            ["rclone", *args],
            text=True,
            capture_output=True,
        )
        if check and proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "rclone command failed")
        return proc

    def _ensure_remote_configured(self) -> None:
        result = self._run(["listremotes"])
        if f"{self._mega_remote}:" in result.stdout:
            return

        email = os.getenv("MEGA_EMAIL")
        password = os.getenv("MEGA_PASSWORD")
        if not email or not password:
            raise RuntimeError(
                "MEGA remote is not configured. Set MEGA_EMAIL and MEGA_PASSWORD, "
                "or create the rclone remote manually."
            )

        self._run(
            [
                "config",
                "create",
                self._mega_remote,
                "mega",
                "user",
                email,
                "pass",
                password,
                "--non-interactive",
            ],
            check=True,
        )

    def _sync_result(
        self,
        *,
        uploaded=0,
        downloaded=0,
        skipped=0,
        failed=0,
        bytes_transferred=0,
        start=0.0,
        errors=None,
    ):
        return SyncResult(
            uploaded=uploaded,
            downloaded=downloaded,
            skipped=skipped,
            failed=failed,
            bytes_transferred=bytes_transferred,
            elapsed_seconds=time.time() - start if start else 0.0,
            errors=errors or [],
        )

    def _delete_remote_file(self, remote_path: str) -> None:
        proc = self._run(["deletefile", remote_path])
        if proc.returncode != 0:
            stderr = (proc.stderr or proc.stdout or "").lower()
            if "not found" in stderr or "does not exist" in stderr:
                return
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "MEGA delete failed")

    def _cleanup_legacy_log_files(self, remote_directory: str) -> None:
        """
        Remove any old log.json files from the remote archive before upload.

        This prevents MEGA from accumulating both:
          - file_log.json (canonical)
          - log.json (legacy)
        """
        for item in self.list_files(remote_directory):
            basename = os.path.basename(item.remote_path)
            if basename != self.LEGACY_LOG_NAME:
                continue
            self._delete_remote_file(self._remote_join(os.path.dirname(item.remote_path), basename))

    # ------------------------------------------------------------------
    # Protocol methods
    # ------------------------------------------------------------------
    def connect(self) -> None:
        self._ensure_remote_configured()
        self._connected = True

    def get_provider_name(self) -> str:
        return "MEGA"

    def upload_file(self, local_path: str, remote_path: str) -> StorageFileInfo:
        remote_spec = self._remote_prefix(os.path.dirname(remote_path))
        remote_file_name = os.path.basename(remote_path)
        target = (
            f"{remote_spec}/{remote_file_name}"
            if not remote_spec.endswith(":")
            else f"{remote_spec}{remote_file_name}"
        )

        proc = self._run(["copyto", local_path, target, "--progress"])
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "MEGA upload failed")

        size_bytes = os.path.getsize(local_path)
        return StorageFileInfo(
            remote_path=remote_path,
            content_hash=None,
            size_bytes=size_bytes,
            last_modified=None,
        )

    def download_file(self, remote_path: str, local_path: str) -> StorageFileInfo:
        remote_spec = self._remote_prefix(os.path.dirname(remote_path))
        remote_file_name = os.path.basename(remote_path)
        source = (
            f"{remote_spec}/{remote_file_name}"
            if not remote_spec.endswith(":")
            else f"{remote_spec}{remote_file_name}"
        )

        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        proc = self._run(["copyto", source, local_path, "--progress"])
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "MEGA download failed")

        size_bytes = os.path.getsize(local_path)
        return StorageFileInfo(
            remote_path=remote_path,
            content_hash=None,
            size_bytes=size_bytes,
            last_modified=None,
        )

    def list_files(self, remote_directory: str) -> List[StorageFileInfo]:
        remote_spec = self._remote_prefix(remote_directory)
        proc = self._run(["lsjson", remote_spec, "--recursive"])
        if proc.returncode != 0:
            return []

        try:
            entries = json.loads(proc.stdout or "[]")
        except json.JSONDecodeError:
            return []

        files: List[StorageFileInfo] = []
        for entry in entries:
            if entry.get("IsDir"):
                continue

            rel_path = entry.get("Path") or entry.get("Name") or ""
            if not rel_path:
                continue

            if remote_directory and remote_directory.strip("/"):
                remote_path = f"{remote_directory.strip('/')}/{rel_path}".lstrip("/")
            else:
                remote_path = rel_path.lstrip("/")

            files.append(
                StorageFileInfo(
                    remote_path=remote_path,
                    content_hash=None,
                    size_bytes=int(entry.get("Size") or 0),
                    last_modified=entry.get("ModTime"),
                )
            )

        return files

    def get_file_info(self, remote_path: str) -> Optional[StorageFileInfo]:
        remote_path = remote_path.strip("/")
        if not remote_path:
            return None

        directory = os.path.dirname(remote_path)
        basename = os.path.basename(remote_path)
        for item in self.list_files(directory):
            if os.path.basename(item.remote_path) == basename:
                return item
        return None

    def file_exists(self, remote_path: str) -> bool:
        return self.get_file_info(remote_path) is not None

    def upload_directory(self, local_directory: str, remote_directory: str, check_type: str = "DIR") -> SyncResult:
        self.connect()
        start = time.time()
        remote_spec = self._remote_prefix(remote_directory)

        # Remove stale legacy logs before syncing so only file_log.json remains.
        self._cleanup_legacy_log_files(remote_directory)

        proc = self._run(
            [
                "copy",
                local_directory,
                remote_spec,
                "--size-only",
                "--transfers",
                "2",
                "--checkers",
                "4",
                "--low-level-retries",
                "20",
                "--retries",
                "10",
                "--progress",
            ]
        )

        if proc.returncode != 0:
            stderr = proc.stderr.strip() or proc.stdout.strip() or "MEGA upload failed"
            return self._sync_result(failed=1, start=start, errors=[stderr])

        return self._sync_result(uploaded=1, start=start, bytes_transferred=0)

    def download_directory(self, remote_directory: str, local_directory: str, check_type: str = "DIR") -> SyncResult:
        self.connect()
        start = time.time()
        os.makedirs(local_directory, exist_ok=True)
        remote_spec = self._remote_prefix(remote_directory)

        if check_type.upper() == "LOG":
            local_file = os.path.join(local_directory, self.CANONICAL_LOG_NAME)

            for candidate_name in (self.CANONICAL_LOG_NAME, self.LEGACY_LOG_NAME):
                remote_file = self._remote_join(remote_directory, candidate_name)
                proc = self._run(["copyto", remote_file, local_file, "--progress"])

                if proc.returncode == 0:
                    size_bytes = os.path.getsize(local_file) if os.path.exists(local_file) else 0
                    return self._sync_result(
                        downloaded=1,
                        start=start,
                        bytes_transferred=size_bytes,
                    )

                stderr = (proc.stderr or proc.stdout or "").lower()
                if "not found" in stderr or "does not exist" in stderr:
                    continue

                return self._sync_result(
                    failed=1,
                    start=start,
                    errors=[proc.stderr.strip() or proc.stdout.strip() or "MEGA log download failed"],
                )

            return self._sync_result(start=start)

        proc = self._run(
            [
                "copy",
                remote_spec,
                local_directory,
                "--size-only",
                "--transfers",
                "2",
                "--checkers",
                "4",
                "--low-level-retries",
                "20",
                "--retries",
                "10",
                "--progress",
            ]
        )
        if proc.returncode != 0:
            stderr = proc.stderr.strip() or proc.stdout.strip() or "MEGA download failed"
            if "not found" in stderr.lower() or "does not exist" in stderr.lower():
                return self._sync_result(start=start)
            return self._sync_result(failed=1, start=start, errors=[stderr])

        return self._sync_result(downloaded=1, start=start)

    # ------------------------------------------------------------------
    # Convenience methods for compatibility with migration utilities
    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        return f"MegaStorageProvider(remote={self._mega_remote!r})"
