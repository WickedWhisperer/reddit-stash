from __future__ import annotations

import configparser
import hashlib
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import dropbox
import requests
from dropbox.exceptions import ApiError
from dropbox.files import FileMetadata
from tqdm import tqdm

from utils.file_path_validate import validate_and_set_directory


class DropboxContentHasher:
    """Implements Dropbox content hashing as per Dropbox's reference algorithm."""

    BLOCK_SIZE = 4 * 1024 * 1024

    def __init__(self):
        self._overall_hasher = hashlib.sha256()
        self._block_hasher = hashlib.sha256()
        self._block_pos = 0
        self.digest_size = self._overall_hasher.digest_size

    def update(self, new_data):
        if self._overall_hasher is None:
            raise AssertionError("can't use this object anymore; you already called digest()")
        assert isinstance(new_data, bytes), f"Expecting a byte string, got {type(new_data)!r}"

        new_data_pos = 0
        while new_data_pos < len(new_data):
            if self._block_pos == self.BLOCK_SIZE:
                self._overall_hasher.update(self._block_hasher.digest())
                self._block_hasher = hashlib.sha256()
                self._block_pos = 0

            space_in_block = self.BLOCK_SIZE - self._block_pos
            part = new_data[new_data_pos : new_data_pos + space_in_block]
            self._block_hasher.update(part)
            self._block_pos += len(part)
            new_data_pos += len(part)

    def _finish(self):
        if self._overall_hasher is None:
            raise AssertionError(
                "can't use this object anymore; you already called digest() or hexdigest()"
            )
        if self._block_pos > 0:
            self._overall_hasher.update(self._block_hasher.digest())
            self._block_hasher = None

        h = self._overall_hasher
        self._overall_hasher = None
        return h

    def digest(self):
        return self._finish().digest()

    def hexdigest(self):
        return self._finish().hexdigest()


def refresh_dropbox_token():
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
    client_id = os.getenv("DROPBOX_APP_KEY")
    client_secret = os.getenv("DROPBOX_APP_SECRET")

    response = requests.post(
        "https://api.dropboxapi.com/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )

    if response.status_code == 200:
        new_access_token = response.json().get("access_token")
        os.environ["DROPBOX_TOKEN"] = new_access_token
        print(" -- Access Token Refreshed -- ")
        return new_access_token

    raise Exception("Failed to refresh Dropbox token")


config_parser = configparser.ConfigParser()
config_parser.read("settings.ini")

local_dir = config_parser.get("Settings", "save_directory", fallback="reddit/")
local_dir = validate_and_set_directory(local_dir)

dropbox_folder = config_parser.get("Settings", "dropbox_directory", fallback="/reddit")
check_type = config_parser.get("Settings", "check_type", fallback="DIR").upper()
process_gdpr = config_parser.getboolean("Settings", "process_gdpr", fallback=False)


def sanitize_filename(filename: str) -> str:
    """Sanitize a filename to be Dropbox-compatible."""
    sanitized_name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", filename)
    sanitized_name = sanitized_name.strip()

    reserved_names = {
        "CON", "PRN", "AUX", "NUL",
        "COM1", "LPT1", "COM2", "LPT2", "COM3", "LPT3",
        "COM4", "LPT4", "COM5", "LPT5", "COM6", "LPT6",
        "COM7", "LPT7", "COM8", "LPT8", "COM9", "LPT9",
    }
    if sanitized_name.upper() in reserved_names:
        sanitized_name = "_" + sanitized_name
    return sanitized_name


def calculate_local_content_hash(file_path: str) -> str:
    """Calculate the Dropbox content hash for a local file."""
    hasher = DropboxContentHasher()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if len(chunk) == 0:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def list_dropbox_files_with_hashes(dbx, dropbox_folder_path: str):
    """List all files in the specified Dropbox folder along with content hashes."""
    file_metadata = {}
    try:
        result = dbx.files_list_folder(dropbox_folder_path, recursive=True)
        while True:
            for entry in result.entries:
                if isinstance(entry, FileMetadata):
                    file_metadata[entry.path_lower] = entry.content_hash
            if not result.has_more:
                break
            result = dbx.files_list_folder_continue(result.cursor)
    except ApiError as err:
        print(f"Failed to list files in Dropbox folder {dropbox_folder_path}: {err}")
    return file_metadata


DROPBOX_SINGLE_UPLOAD_LIMIT = 150 * 1024 * 1024  # 150MB
DROPBOX_UPLOAD_CHUNK_SIZE = 4 * 1024 * 1024      # 4MB


def _upload_file_to_dropbox(dbx, file_path: str, dropbox_path: str) -> int:
    """Upload a single file to Dropbox, using chunked upload for large files."""
    file_size = os.path.getsize(file_path)

    if file_size <= DROPBOX_SINGLE_UPLOAD_LIMIT:
        with open(file_path, "rb") as f:
            dbx.files_upload(
                f.read(),
                dropbox_path,
                mode=dropbox.files.WriteMode.overwrite,
            )
        return file_size

    with open(file_path, "rb") as f:
        chunk = f.read(DROPBOX_UPLOAD_CHUNK_SIZE)
        session = dbx.files_upload_session_start(chunk)
        cursor = dropbox.files.UploadSessionCursor(
            session_id=session.session_id,
            offset=len(chunk),
        )
        commit = dropbox.files.CommitInfo(
            path=dropbox_path,
            mode=dropbox.files.WriteMode.overwrite,
        )

        while True:
            chunk = f.read(DROPBOX_UPLOAD_CHUNK_SIZE)
            if f.tell() >= file_size:
                dbx.files_upload_session_finish(chunk, cursor, commit)
                break
            dbx.files_upload_session_append_v2(chunk, cursor)
            cursor.offset += len(chunk)

    return file_size


def upload_directory_to_dropbox(local_directory: str, dropbox_folder_path: str = "/"):
    """Uploads all files in a local directory to Dropbox, replacing only changed files."""
    dbx = dropbox.Dropbox(os.getenv("DROPBOX_TOKEN"))

    dropbox_files = list_dropbox_files_with_hashes(dbx, dropbox_folder_path)
    uploaded_count = 0
    uploaded_size = 0
    skipped_count = 0
    lock = threading.Lock()

    files_to_upload = [
        (root, file_name)
        for root, dirs, files in os.walk(local_directory)
        for file_name in files
        if not file_name.startswith(".")
    ]

    def _process_file(root_and_name):
        root, file_name = root_and_name
        sanitized_name = sanitize_filename(file_name)
        file_path = os.path.join(root, file_name)

        dropbox_path = f"{dropbox_folder_path}/{os.path.relpath(file_path, local_directory).replace(os.path.sep, '/')}"
        dropbox_path = dropbox_path.replace(file_name, sanitized_name)

        if dropbox_path.lower() in dropbox_files:
            local_content_hash = calculate_local_content_hash(file_path)
            if dropbox_files[dropbox_path.lower()] == local_content_hash:
                return "skipped", 0

        try:
            size = _upload_file_to_dropbox(dbx, file_path, dropbox_path)
            return "uploaded", size
        except ApiError as e:
            print(f"Failed to upload {file_path} to Dropbox: {e}")
            return "failed", 0

    with tqdm(total=len(files_to_upload), desc="Uploading files to Dropbox") as pbar:
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(_process_file, f): f for f in files_to_upload}
            for future in as_completed(futures):
                status, size = future.result()
                with lock:
                    if status == "uploaded":
                        uploaded_count += 1
                        uploaded_size += size
                    elif status == "skipped":
                        skipped_count += 1
                pbar.update(1)

    print(f"Upload completed.\n{uploaded_count} files uploaded ({uploaded_size / (1024 * 1024):.2f} MB).")
    print(f"{skipped_count} files were skipped (already existed or unchanged).")


def download_directory_from_dropbox(dbx, dropbox_folder_path: str, local_directory: str):
    """Downloads all files from a Dropbox folder to a local directory."""
    downloaded_count = 0
    downloaded_size = 0
    skipped_count = 0

    dropbox_files = list_dropbox_files_with_hashes(dbx, dropbox_folder_path)

    with tqdm(total=len(dropbox_files), desc="Downloading files from Dropbox") as pbar:
        try:
            for dropbox_path, dropbox_hash in dropbox_files.items():
                local_path = os.path.join(
                    local_directory,
                    dropbox_path[len(dropbox_folder_path):].lstrip("/"),
                )
                if os.path.exists(local_path):
                    local_content_hash = calculate_local_content_hash(local_path)
                    if local_content_hash == dropbox_hash:
                        skipped_count += 1
                        pbar.update(1)
                        continue

                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                with open(local_path, "wb") as f:
                    metadata, res = dbx.files_download(dropbox_path)
                    f.write(res.content)

                downloaded_count += 1
                downloaded_size += metadata.size
                pbar.update(1)
        except ApiError as err:
            print(f"Failed to download files from Dropbox folder {dropbox_folder_path}: {err}")

    print(f"Download completed.\n{downloaded_count} files downloaded ({downloaded_size / (1024 * 1024):.2f} MB).")
    print(f"{skipped_count} files were skipped (already existed or unchanged).")


def download_log_file_from_dropbox(dbx, dropbox_folder_path: str, local_directory: str):
    """Download only the log file from Dropbox."""
    log_file_path = os.path.join(local_directory, "file_log.json")
    try:
        metadata, res = dbx.files_download(f"{dropbox_folder_path}/file_log.json")
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        with open(log_file_path, "wb") as f:
            f.write(res.content)
        print(f"Log file downloaded successfully to {log_file_path}.")
    except ApiError as err:
        print(f"Failed to download the log file from Dropbox: {err}")


def download_gdpr_data_from_dropbox(dbx, dropbox_folder_path: str, local_directory: str):
    """
    Download GDPR export data from Dropbox into a local gdpr_data folder.

    This fixes LOG mode runs that also need process_gdpr=true: the workflow used
    to only fetch file_log.json, which meant the runner never saw gdpr_data/.
    """
    gdpr_dropbox_folder = f"{dropbox_folder_path}/gdpr_data"
    gdpr_local_directory = os.path.join(local_directory, "gdpr_data")
    print(f"Downloading GDPR data from {gdpr_dropbox_folder} to {gdpr_local_directory}...")
    download_directory_from_dropbox(dbx, gdpr_dropbox_folder, gdpr_local_directory)


if __name__ == "__main__":
    refresh_dropbox_token()
    dbx = dropbox.Dropbox(os.getenv("DROPBOX_TOKEN"))

    if "--download" in sys.argv:
        if check_type == "LOG":
            print("Downloading only the log file as check_type is LOG.")
            download_log_file_from_dropbox(dbx, dropbox_folder, local_dir)

            if process_gdpr:
                download_gdpr_data_from_dropbox(dbx, dropbox_folder, local_dir)

        elif check_type == "DIR":
            print("Downloading the entire directory as check_type is DIR.")
            download_directory_from_dropbox(dbx, dropbox_folder, local_dir)
        else:
            raise ValueError(f"Unknown check_type: {check_type}")

    elif "--upload" in sys.argv:
        upload_directory_to_dropbox(local_dir, dropbox_folder)
