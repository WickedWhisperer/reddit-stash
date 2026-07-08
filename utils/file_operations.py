from __future__ import annotations

import configparser
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor

import prawcore
from praw.models import Comment, Submission
from tqdm import tqdm

from utils.env_config import get_ignore_tls_errors
from utils.log_utils import log_file, save_file_log
from utils.path_security import create_safe_path, create_reddit_file_path
from utils.praw_helpers import safe_fetch_items_one_by_one
from utils.save_utils import (
    _get_media_size,
    _reset_media_tracker,
    save_comment_and_context,
    save_submission,
)
from utils.time_utilities import dynamic_sleep

logger = logging.getLogger(__name__)

# Lock protecting concurrent access to created_dirs_cache (check-then-create pattern)
_dir_cache_lock = threading.Lock()

# Dynamically determine the path to the root directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Construct the full path to the settings.ini file
config_path = os.path.join(BASE_DIR, "settings.ini")

# Load settings from the settings.ini file
config = configparser.ConfigParser()
config.read(config_path)

save_type = config.get("Settings", "save_type", fallback="ALL").upper()
check_type = config.get("Settings", "check_type", fallback="DIR").upper()


def create_directory(subreddit_name, save_directory, created_dirs_cache):
    """Create the directory for saving data if it does not exist."""
    path_result = create_safe_path(save_directory, subreddit_name)
    if not path_result.is_safe:
        logger.error(f"Unsafe subreddit name '{subreddit_name}': {path_result.issues}")
        fallback_name = "sanitized_subreddit"
        path_result = create_safe_path(save_directory, fallback_name)
        if not path_result.is_safe:
            raise ValueError(f"Cannot create safe directory path: {path_result.issues}")

    sub_dir = path_result.safe_path
    with _dir_cache_lock:
        if sub_dir not in created_dirs_cache:
            os.makedirs(sub_dir, exist_ok=True)
            created_dirs_cache.add(sub_dir)
            logger.info(f"Created directory: {sub_dir}")
    return sub_dir


def get_existing_files_from_log(file_log):
    """Return a set of unique keys based on the JSON log."""
    return set(file_log.keys())


def get_existing_files_from_dir(save_directory):
    """Build a set of all existing files in the save directory using os.walk."""
    existing_files = set()

    for root, _, files in os.walk(save_directory):
        subreddit_name = os.path.basename(root)
        for filename in files:
            stem = os.path.splitext(filename)[0]

            if stem.startswith("POST_"):
                file_id = stem.split("POST_", 1)[1]
                content_type = "Submission"
            elif stem.startswith("COMMENT_"):
                file_id = stem.split("COMMENT_", 1)[1]
                content_type = "Comment"
            elif stem.startswith("SAVED_POST_"):
                file_id = stem.split("SAVED_POST_", 1)[1]
                content_type = "Submission"
            elif stem.startswith("SAVED_COMMENT_"):
                file_id = stem.split("SAVED_COMMENT_", 1)[1]
                content_type = "Comment"
            elif stem.startswith("UPVOTE_POST_"):
                file_id = stem.split("UPVOTE_POST_", 1)[1]
                content_type = "Submission"
            elif stem.startswith("UPVOTE_COMMENT_"):
                file_id = stem.split("UPVOTE_COMMENT_", 1)[1]
                content_type = "Comment"
            elif stem.startswith("GDPR_POST_"):
                file_id = stem.split("GDPR_POST_", 1)[1]
                content_type = "Submission"
            elif stem.startswith("GDPR_COMMENT_"):
                file_id = stem.split("GDPR_COMMENT_", 1)[1]
                content_type = "Comment"
            else:
                continue

            unique_key = f"{file_id}-{subreddit_name}-{content_type}"
            existing_files.add(unique_key)

    return existing_files


def save_to_file(
    content,
    file_path,
    save_function,
    existing_files,
    file_log,
    save_directory,
    created_dirs_cache,
    category="POST",
    unsave=False,
    ignore_tls_errors=None,
):
    """Save content to a file using the specified save function."""
    from utils.praw_helpers import RecoveredItem

    is_recovered = isinstance(content, RecoveredItem)
    file_id = content.id

    if is_recovered:
        recovered_data = content.recovered_data if hasattr(content, "recovered_data") else {}
        subreddit_name = recovered_data.get("subreddit", "unknown")
    else:
        subreddit_name = content.subreddit.display_name

    unique_key = f"{file_id}-{subreddit_name}-{type(content).__name__}-{category}"

    if unique_key in existing_files:
        return True, 0

    path_result = create_safe_path(save_directory, subreddit_name)
    if not path_result.is_safe:
        logger.error(f"Unsafe subreddit name '{subreddit_name}': {path_result.issues}")
        fallback_name = "sanitized_subreddit"
        path_result = create_safe_path(save_directory, fallback_name)
        if not path_result.is_safe:
            raise ValueError(f"Cannot create safe directory path: {path_result.issues}")

    sub_dir = path_result.safe_path

    with _dir_cache_lock:
        if sub_dir not in created_dirs_cache:
            os.makedirs(sub_dir, exist_ok=True)
            created_dirs_cache.add(sub_dir)

    try:
        _reset_media_tracker()

        with open(file_path, "w", encoding="utf-8") as f:
            save_function(content, f, unsave=unsave, ignore_tls_errors=ignore_tls_errors)

        media_size = _get_media_size()

        file_info = {
            "subreddit": subreddit_name,
            "type": type(content).__name__,
            "file_path": file_path,
        }

        if is_recovered and hasattr(content, "recovery_result"):
            recovery_result = content.recovery_result
            if recovery_result and recovery_result.metadata:
                file_info["recovered"] = True
                file_info["recovery_source"] = recovery_result.metadata.source.value
                file_info["recovery_timestamp"] = recovery_result.metadata.recovery_date
                file_info["recovery_quality"] = recovery_result.metadata.content_quality.value
                if recovery_result.recovered_url:
                    file_info["recovery_url"] = recovery_result.recovered_url

        log_file(file_log, unique_key, file_info, save_directory)
        return False, media_size

    except Exception as e:
        logger.error(f"Failed to save {file_path}: {e}")
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except OSError:
            pass
        return False, 0


def handle_dynamic_sleep(item):
    """Handle dynamic sleep based on the type of Reddit item."""
    if isinstance(item, Submission) and item.is_self and item.selftext:
        dynamic_sleep(len(item.selftext))
    elif isinstance(item, Comment) and item.body:
        dynamic_sleep(len(item.body))
    else:
        dynamic_sleep(0)


def _clone_reddit(reddit):
    """Create a new PRAW Reddit instance with the same credentials for thread-safe parallel use."""
    try:
        import praw
    except Exception:
        return reddit

    kwargs = {}
    for attr in (
        "client_id",
        "client_secret",
        "username",
        "password",
        "refresh_token",
        "user_agent",
        "ratelimit_seconds",
        "timeout",
    ):
        value = getattr(reddit, attr, None)
        if value not in (None, "", "None"):
            kwargs[attr] = value

    try:
        if kwargs:
            return praw.Reddit(**kwargs)
    except Exception:
        pass

    return reddit


def _fetch_items(reddit, method, limit, label):
    """Fetch Reddit items using the shared helper."""
    try:
        return list(safe_fetch_items_one_by_one(reddit, method, limit, label))
    except TypeError:
        return list(safe_fetch_items_one_by_one(reddit, method, limit))
    except prawcore.exceptions.PrawcoreException:
        raise
    except Exception:
        return []


def _merge_results(*results):
    processed_count = 0
    skipped_count = 0
    total_size = 0
    total_media_size = 0

    for result in results:
        if not result:
            continue
        p, s, ts, tms = result
        processed_count += p
        skipped_count += s
        total_size += ts
        total_media_size += tms

    return processed_count, skipped_count, total_size, total_media_size


def _process_submissions_batch(
    submissions,
    save_directory,
    existing_files,
    created_dirs_cache,
    file_log,
    ignore_tls_errors,
    category="POST",
    unsave=False,
    tqdm_desc="Processing Submissions",
    tqdm_position=0,
):
    """Process a batch of submissions in a single thread."""
    processed_count = 0
    skipped_count = 0
    total_size = 0
    total_media_size = 0

    for submission in tqdm(submissions, desc=tqdm_desc, position=tqdm_position, leave=True):
        path_result = create_reddit_file_path(
            save_directory,
            submission.subreddit.display_name,
            category,
            submission.id,
        )

        if not path_result.is_safe:
            logger.error(f"Unsafe path for submission {submission.id}: {path_result.issues}")
            continue

        file_path = path_result.safe_path
        save_result, media_size = save_to_file(
            submission,
            file_path,
            save_submission,
            existing_files,
            file_log,
            save_directory,
            created_dirs_cache,
            category=category,
            unsave=unsave,
            ignore_tls_errors=ignore_tls_errors,
        )

        if save_result:
            skipped_count += 1
            continue

        processed_count += 1
        total_media_size += media_size

        try:
            if os.path.exists(file_path):
                total_size += os.path.getsize(file_path)
        except OSError:
            pass

        handle_dynamic_sleep(submission)

    return processed_count, skipped_count, total_size, total_media_size


def _process_comments_batch(
    comments,
    save_directory,
    existing_files,
    created_dirs_cache,
    file_log,
    ignore_tls_errors,
    category="COMMENT",
    unsave=False,
    tqdm_desc="Processing Comments",
    tqdm_position=1,
):
    """Process a batch of comments in a single thread."""
    processed_count = 0
    skipped_count = 0
    total_size = 0
    total_media_size = 0

    for comment in tqdm(comments, desc=tqdm_desc, position=tqdm_position, leave=True):
        path_result = create_reddit_file_path(
            save_directory,
            comment.subreddit.display_name,
            category,
            comment.id,
        )

        if not path_result.is_safe:
            logger.error(f"Unsafe path for comment {comment.id}: {path_result.issues}")
            continue

        file_path = path_result.safe_path
        save_result, media_size = save_to_file(
            comment,
            file_path,
            save_comment_and_context,
            existing_files,
            file_log,
            save_directory,
            created_dirs_cache,
            category=category,
            unsave=unsave,
            ignore_tls_errors=ignore_tls_errors,
        )

        if save_result:
            skipped_count += 1
            continue

        processed_count += 1
        total_media_size += media_size

        try:
            if os.path.exists(file_path):
                total_size += os.path.getsize(file_path)
        except OSError:
            pass

        handle_dynamic_sleep(comment)

    return processed_count, skipped_count, total_size, total_media_size


def _process_mixed_items(
    items,
    save_directory,
    existing_files,
    created_dirs_cache,
    file_log,
    ignore_tls_errors,
    sub_category="SAVED_POST",
    comment_category="SAVED_COMMENT",
    unsave=False,
    tqdm_desc="Processing Items",
    tqdm_position=0,
):
    """Process mixed submissions and comments in a single thread."""
    processed_count = 0
    skipped_count = 0
    total_size = 0
    total_media_size = 0

    for item in tqdm(items, desc=tqdm_desc, position=tqdm_position, leave=True):
        if isinstance(item, Submission):
            category = sub_category
            save_fn = save_submission
        elif isinstance(item, Comment):
            category = comment_category
            save_fn = save_comment_and_context
        else:
            continue

        path_result = create_reddit_file_path(
            save_directory,
            item.subreddit.display_name,
            category,
            item.id,
        )

        if not path_result.is_safe:
            logger.error(f"Unsafe path for item {item.id}: {path_result.issues}")
            continue

        file_path = path_result.safe_path
        save_result, media_size = save_to_file(
            item,
            file_path,
            save_fn,
            existing_files,
            file_log,
            save_directory,
            created_dirs_cache,
            category=category,
            unsave=unsave,
            ignore_tls_errors=ignore_tls_errors,
        )

        if save_result:
            skipped_count += 1
            continue

        processed_count += 1
        total_media_size += media_size

        try:
            if os.path.exists(file_path):
                total_size += os.path.getsize(file_path)
        except OSError:
            pass

        handle_dynamic_sleep(item)

    return processed_count, skipped_count, total_size, total_media_size


def save_user_activity(reddit, save_directory, file_log, unsave=False):
    """Save user's posts, comments, saved items, and upvoted content."""
    ignore_tls_errors = get_ignore_tls_errors()

    if check_type == "LOG":
        print("Check type is LOG. Using JSON log to find existing files.")
        existing_files = get_existing_files_from_log(file_log)
    elif check_type == "DIR":
        print("Check type is DIR. Using directory scan to find existing files.")
        existing_files = get_existing_files_from_dir(save_directory)
    else:
        raise ValueError(f"Unknown check_type: {check_type}")

    created_dirs_cache = set()
    shared_args = dict(
        save_directory=save_directory,
        existing_files=existing_files,
        created_dirs_cache=created_dirs_cache,
        file_log=file_log,
        ignore_tls_errors=ignore_tls_errors,
    )

    if save_type == "ALL":
        endpoints = [
            ("submissions", "submission"),
            ("comments", "comment"),
            ("saved", "saved"),
            ("upvoted", "upvoted"),
        ]

        fetched = {}
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {}
            for method, label in endpoints:
                r = _clone_reddit(reddit)
                futures[label] = pool.submit(_fetch_items, r, method, 1000, label)

            for label, future in futures.items():
                fetched[label] = future.result()

        submissions = fetched["submission"]
        comments = fetched["comment"]
        saved_items = fetched["saved"]
        upvoted_items = fetched["upvoted"]

        with ThreadPoolExecutor(max_workers=4) as pool:
            f1 = pool.submit(
                _process_submissions_batch,
                submissions,
                category="POST",
                tqdm_desc="Submissions",
                tqdm_position=0,
                **shared_args,
            )
            f2 = pool.submit(
                _process_comments_batch,
                comments,
                category="COMMENT",
                tqdm_desc="Comments",
                tqdm_position=1,
                **shared_args,
            )
            f3 = pool.submit(
                _process_mixed_items,
                saved_items,
                sub_category="SAVED_POST",
                comment_category="SAVED_COMMENT",
                unsave=unsave,
                tqdm_desc="Saved Items",
                tqdm_position=2,
                **shared_args,
            )
            f4 = pool.submit(
                _process_mixed_items,
                upvoted_items,
                sub_category="UPVOTE_POST",
                comment_category="UPVOTE_COMMENT",
                tqdm_desc="Upvoted Items",
                tqdm_position=3,
                **shared_args,
            )

            results = [f.result() for f in [f1, f2, f3, f4]]

        processed_count, skipped_count, total_size, total_media_size = _merge_results(*results)

    elif save_type == "ACTIVITY":
        with ThreadPoolExecutor(max_workers=2) as pool:
            r1 = _clone_reddit(reddit)
            r2 = _clone_reddit(reddit)
            f_sub = pool.submit(_fetch_items, r1, "submissions", 1000, "submission")
            f_com = pool.submit(_fetch_items, r2, "comments", 1000, "comment")
            submissions = f_sub.result()
            comments = f_com.result()

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(
                _process_submissions_batch,
                submissions,
                category="POST",
                tqdm_desc="Submissions",
                tqdm_position=0,
                **shared_args,
            )
            f2 = pool.submit(
                _process_comments_batch,
                comments,
                category="COMMENT",
                tqdm_desc="Comments",
                tqdm_position=1,
                **shared_args,
            )
            processed_count, skipped_count, total_size, total_media_size = _merge_results(
                f1.result(),
                f2.result(),
            )

    elif save_type == "SAVED":
        saved_items = _fetch_items(reddit, "saved", 1000, "saved")
        processed_count, skipped_count, total_size, total_media_size = _process_mixed_items(
            saved_items,
            sub_category="SAVED_POST",
            comment_category="SAVED_COMMENT",
            unsave=unsave,
            tqdm_desc="Saved Items",
            **shared_args,
        )

    elif save_type == "UPVOTED":
        upvoted_items = _fetch_items(reddit, "upvoted", 1000, "upvoted")
        processed_count, skipped_count, total_size, total_media_size = _process_mixed_items(
            upvoted_items,
            sub_category="UPVOTE_POST",
            comment_category="UPVOTE_COMMENT",
            tqdm_desc="Upvoted Items",
            **shared_args,
        )

    else:
        raise ValueError(f"Unknown save_type: {save_type}")

    save_file_log(file_log, save_directory)
    return processed_count, skipped_count, total_size, total_media_size
