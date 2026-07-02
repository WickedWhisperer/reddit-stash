import os
import json
import tempfile
import threading

from utils.constants import FILE_LOG_CHECKPOINT_INTERVAL

# Lock protecting concurrent access to file_log dict and checkpoint writes
_log_lock = threading.Lock()


def get_log_file_path(save_directory):
    """Return the path to the log file inside the save_directory."""
    return os.path.join(save_directory, 'file_log.json')


def load_file_log(save_directory):
    """Load the file log from a JSON file in the specified directory."""
    log_file_path = get_log_file_path(save_directory)
    if os.path.exists(log_file_path):
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, OSError):
            broken_path = f"{log_file_path}.broken"
            try:
                os.replace(log_file_path, broken_path)
                print(f"⚠️ Corrupted log moved to {broken_path}; starting with an empty log.")
            except OSError:
                print("⚠️ Corrupted log detected; starting with an empty log.")
            return {}
    return {}


def save_file_log(log_data, save_directory):
    """Save the file log to a JSON file in the specified directory."""
    log_file_path = get_log_file_path(save_directory)
    os.makedirs(save_directory, exist_ok=True)

    with _log_lock:
        fd, tmp_path = tempfile.mkstemp(prefix='file_log.', suffix='.tmp', dir=save_directory)
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, log_file_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise


def is_file_logged(log_data, unique_key):
    """Check if a unique key is already logged."""
    return unique_key in log_data


def log_file(log_data, unique_key, file_info, save_directory):
    """Add a file information to the log with the provided unique key.

    Thread-safe: uses _log_lock to serialize dict mutation and checkpoint writes.
    """
    with _log_lock:
        # Convert the absolute file path to a relative one
        relative_file_path = os.path.relpath(file_info['file_path'], start=save_directory)

        # Update the file info with the relative path
        file_info['file_path'] = relative_file_path

        # Add the file info to the log with the unique key
        log_data[unique_key] = file_info

        # Periodic checkpoint: save every N items to limit data loss on crash
        # Final save happens in file_operations.py:save_user_activity()
        if len(log_data) % FILE_LOG_CHECKPOINT_INTERVAL == 0:
            save_file_log(log_data, save_directory)


def convert_to_absolute_path(relative_path, save_directory):
    """Convert a relative path from the log back to an absolute path."""
    return os.path.join(save_directory, relative_path)
