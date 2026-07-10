from __future__ import annotations

"""Process Reddit GDPR export data.

The GDPR export is used in addition to the normal API crawl. The processor looks
for CSV files inside {save_directory}/gdpr_data and writes markdown records for
saved posts, saved comments, and vote history.
"""

import logging
import os
from typing import Any, Iterable, Tuple

import pandas as pd
from tqdm import tqdm

from utils.env_config import get_ignore_tls_errors
from utils.file_operations import save_to_file
from utils.log_utils import is_file_logged, log_file
from utils.path_security import create_reddit_file_path
from utils.praw_helpers import RecoveredItem
from utils.save_utils import create_recovery_metadata_markdown, save_comment_and_context, save_submission
from utils.time_utilities import dynamic_sleep

logger = logging.getLogger(__name__)


def get_gdpr_directory(save_directory: str) -> str:
    """Return the path to the GDPR data directory."""
    gdpr_dir = os.path.join(save_directory, "gdpr_data")
    if not os.path.exists(gdpr_dir):
        print(f"GDPR data directory not found at: {gdpr_dir}")
    return gdpr_dir


def _read_csv(path: str) -> pd.DataFrame:
    """Read a GDPR CSV file as strings so IDs stay stable."""
    return pd.read_csv(path, dtype=str).fillna("")


def _extract_subreddit_from_permalink(permalink: str) -> str:
    """Extract subreddit name from a Reddit permalink, or return a fallback."""
    if permalink:
        parts = permalink.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "r":
            return f"r_{parts[1]}"
    return "r_unknown"


def _normalize_direction(direction: str) -> str:
    """Normalize vote direction from Reddit's CSV export."""
    raw = str(direction).strip().lower()
    if raw in {"1", "up", "upvote", "upvoted", "true"}:
        return "upvote"
    if raw in {"-1", "down", "downvote", "downvoted", "false"}:
        return "downvote"
    if not raw:
        return "unknown"
    return raw


def _write_csv_only_record(
    *,
    record_type: str,
    record_id: str,
    permalink: str,
    body_lines: Iterable[str],
    save_directory: str,
    existing_files: set[str],
    file_log: dict[str, Any],
    created_dirs_cache: set[str],
) -> Tuple[int, int]:
    """Write a minimal GDPR markdown record when API enrichment is unavailable."""
    unique_key = f"GDPR_{record_type}_{record_id}"
    if unique_key in existing_files or is_file_logged(file_log, unique_key):
        return 0, 0

    subreddit = _extract_subreddit_from_permalink(permalink)
    path_result = create_reddit_file_path(save_directory, subreddit, f"GDPR_{record_type}", record_id)
    if not path_result.is_safe:
        logger.error(f"Unsafe path for GDPR {record_type.lower()} {record_id}: {path_result.issues}")
        return 0, 0

    file_path = path_result.safe_path
    dir_path = os.path.dirname(file_path)
    if dir_path not in created_dirs_cache:
        os.makedirs(dir_path, exist_ok=True)
        created_dirs_cache.add(dir_path)

    reddit_url = f"https://www.reddit.com{permalink}" if permalink else f"https://www.reddit.com/"
    content = "\n".join(body_lines).rstrip() + "\n"

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

    file_size = os.path.getsize(file_path)
    log_file(
        file_log,
        unique_key,
        {
            "file_path": file_path,
            "type": f"GDPR_{record_type}",
            "id": record_id,
            "permalink": reddit_url,
        },
        save_directory,
    )
    existing_files.add(unique_key)
    return 1, file_size


def _save_csv_only_post(
    row: pd.Series,
    save_directory: str,
    existing_files: set[str],
    file_log: dict[str, Any],
    created_dirs_cache: set[str],
) -> Tuple[int, int]:
    """Save a minimal markdown file for a GDPR post using only CSV metadata."""
    post_id = str(row.get("id", "")).strip()
    if not post_id:
        return 0, 0

    permalink = str(row.get("permalink", "")).strip()
    reddit_url = f"https://www.reddit.com{permalink}" if permalink else f"https://www.reddit.com/comments/{post_id}"

    return _write_csv_only_record(
        record_type="POST",
        record_id=post_id,
        permalink=permalink,
        body_lines=[
            f"# GDPR Export Post: {post_id}",
            "",
            f"**Reddit Link:** [{reddit_url}]({reddit_url})",
            "",
            f"**Post ID:** {post_id}",
            "",
            "---",
            "",
            "*This is a CSV-only export (no API enrichment required for this record).",
            "Content was not fetched from Reddit.",
            "Visit the link above to view the full post.*",
            "",
        ],
        save_directory=save_directory,
        existing_files=existing_files,
        file_log=file_log,
        created_dirs_cache=created_dirs_cache,
    )


def _save_csv_only_comment(
    row: pd.Series,
    save_directory: str,
    existing_files: set[str],
    file_log: dict[str, Any],
    created_dirs_cache: set[str],
) -> Tuple[int, int]:
    """Save a minimal markdown file for a GDPR comment using only CSV metadata."""
    comment_id = str(row.get("id", "")).strip()
    if not comment_id:
        return 0, 0

    permalink = str(row.get("permalink", "")).strip()
    reddit_url = f"https://www.reddit.com{permalink}" if permalink else f"https://www.reddit.com/comments/{comment_id}"

    return _write_csv_only_record(
        record_type="COMMENT",
        record_id=comment_id,
        permalink=permalink,
        body_lines=[
            f"# GDPR Export Comment: {comment_id}",
            "",
            f"**Reddit Link:** [{reddit_url}]({reddit_url})",
            "",
            f"**Comment ID:** {comment_id}",
            "",
            "---",
            "",
            "*This is a CSV-only export (no API enrichment required for this record).",
            "Content was not fetched from Reddit.",
            "Visit the link above to view the full comment.*",
            "",
        ],
        save_directory=save_directory,
        existing_files=existing_files,
        file_log=file_log,
        created_dirs_cache=created_dirs_cache,
    )


def _save_csv_only_vote(
    row: pd.Series,
    save_directory: str,
    existing_files: set[str],
    file_log: dict[str, Any],
    created_dirs_cache: set[str],
) -> Tuple[int, int]:
    """Save a minimal markdown file for a GDPR vote record."""
    vote_id = str(row.get("id", "")).strip()
    if not vote_id:
        return 0, 0

    permalink = str(row.get("permalink", "")).strip()
    direction = _normalize_direction(row.get("direction", ""))
    reddit_url = f"https://www.reddit.com{permalink}" if permalink else "https://www.reddit.com/"

    return _write_csv_only_record(
        record_type="VOTE",
        record_id=vote_id,
        permalink=permalink,
        body_lines=[
            f"# GDPR Export Vote: {vote_id}",
            "",
            f"**Reddit Link:** [{reddit_url}]({reddit_url})",
            "",
            f"**Vote ID:** {vote_id}",
            f"**Direction:** {direction}",
            "",
            "---",
            "",
            "*This record comes from Reddit's GDPR vote export.",
            "It stores the vote metadata and the linked Reddit item, not the content itself.*",
            "",
        ],
        save_directory=save_directory,
        existing_files=existing_files,
        file_log=file_log,
        created_dirs_cache=created_dirs_cache,
    )


def _process_csv(
    *,
    csv_path: str,
    csv_label: str,
    record_type: str,
    save_directory: str,
    existing_files: set[str],
    file_log: dict[str, Any],
    created_dirs_cache: set[str],
    csv_only_mode: bool,
    reddit,
) -> Tuple[int, int, int]:
    """
    Shared handler for GDPR CSVs.

    Returns:
        processed_count, skipped_count, total_size
    """
    if not os.path.exists(csv_path):
        return 0, 0, 0

    print(f"\nProcessing {csv_label} from GDPR export...")
    df = _read_csv(csv_path)

    processed_count = 0
    skipped_count = 0
    total_size = 0

    for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Processing GDPR {csv_label}"):
        try:
            if record_type == "POST":
                if csv_only_mode:
                    count, size = _save_csv_only_post(row, save_directory, existing_files, file_log, created_dirs_cache)
                    if count == 0:
                        skipped_count += 1
                    else:
                        processed_count += count
                        total_size += size
                    continue

                post_id = str(row.get("id", "")).strip()
                if not post_id:
                    skipped_count += 1
                    continue

                submission = reddit.submission(id=post_id)
                path_result = create_reddit_file_path(
                    save_directory,
                    submission.subreddit.display_name,
                    "GDPR_POST",
                    submission.id,
                )
                if not path_result.is_safe:
                    logger.error(f"Unsafe path for GDPR submission {submission.id}: {path_result.issues}")
                    skipped_count += 1
                    continue

                file_path = path_result.safe_path
                if save_to_file(
                    submission,
                    file_path,
                    save_submission,
                    existing_files,
                    file_log,
                    save_directory,
                    created_dirs_cache,
                    ignore_tls_errors=get_ignore_tls_errors(),
                ):
                    skipped_count += 1
                    continue

                processed_count += 1
                total_size += os.path.getsize(file_path)
                dynamic_sleep(len(submission.selftext) if submission.is_self and submission.selftext else 0)

            elif record_type == "COMMENT":
                if csv_only_mode:
                    count, size = _save_csv_only_comment(row, save_directory, existing_files, file_log, created_dirs_cache)
                    if count == 0:
                        skipped_count += 1
                    else:
                        processed_count += count
                        total_size += size
                    continue

                comment_id = str(row.get("id", "")).strip()
                if not comment_id:
                    skipped_count += 1
                    continue

                comment = reddit.comment(id=comment_id)
                path_result = create_reddit_file_path(
                    save_directory,
                    comment.subreddit.display_name,
                    "GDPR_COMMENT",
                    comment.id,
                )
                if not path_result.is_safe:
                    logger.error(f"Unsafe path for GDPR comment {comment.id}: {path_result.issues}")
                    skipped_count += 1
                    continue

                file_path = path_result.safe_path
                if save_to_file(
                    comment,
                    file_path,
                    save_comment_and_context,
                    existing_files,
                    file_log,
                    save_directory,
                    created_dirs_cache,
                    ignore_tls_errors=get_ignore_tls_errors(),
                ):
                    skipped_count += 1
                    continue

                processed_count += 1
                total_size += os.path.getsize(file_path)
                dynamic_sleep(len(comment.body) if comment.body else 0)

            elif record_type == "VOTE":
                # Votes do not have an API enrichment path in this repo, so we always
                # archive them as CSV-only records.
                count, size = _save_csv_only_vote(row, save_directory, existing_files, file_log, created_dirs_cache)
                if count == 0:
                    skipped_count += 1
                else:
                    processed_count += count
                    total_size += size

        except Exception as e:
            print(f"Error processing GDPR {csv_label.lower()} {row.get('id', '[unknown]')}: {e}")
            skipped_count += 1

    return processed_count, skipped_count, total_size


def process_gdpr_export(reddit, save_directory, existing_files, created_dirs_cache, file_log):
    """Process saved posts/comments/votes from GDPR export CSVs."""
    processed_count = 0
    skipped_count = 0
    total_size = 0
    csv_only_mode = reddit is None

    if csv_only_mode:
        print("Running in CSV-only mode (no API credentials).")
        print("Saving post/comment/vote links from GDPR export without fetching full content.")
    else:
        ignore_tls_errors = get_ignore_tls_errors()  # retained for parity with API mode

    gdpr_dir = get_gdpr_directory(save_directory)

    posts_file = os.path.join(gdpr_dir, "saved_posts.csv")
    comments_file = os.path.join(gdpr_dir, "saved_comments.csv")
    votes_file = os.path.join(gdpr_dir, "post_votes.csv")

    p_count, p_skip, p_size = _process_csv(
        csv_path=posts_file,
        csv_label="saved posts",
        record_type="POST",
        save_directory=save_directory,
        existing_files=existing_files,
        file_log=file_log,
        created_dirs_cache=created_dirs_cache,
        csv_only_mode=csv_only_mode,
        reddit=reddit,
    )
    processed_count += p_count
    skipped_count += p_skip
    total_size += p_size

    c_count, c_skip, c_size = _process_csv(
        csv_path=comments_file,
        csv_label="saved comments",
        record_type="COMMENT",
        save_directory=save_directory,
        existing_files=existing_files,
        file_log=file_log,
        created_dirs_cache=created_dirs_cache,
        csv_only_mode=csv_only_mode,
        reddit=reddit,
    )
    processed_count += c_count
    skipped_count += c_skip
    total_size += c_size

    v_count, v_skip, v_size = _process_csv(
        csv_path=votes_file,
        csv_label="post votes",
        record_type="VOTE",
        save_directory=save_directory,
        existing_files=existing_files,
        file_log=file_log,
        created_dirs_cache=created_dirs_cache,
        csv_only_mode=True,  # votes are archived as CSV-only records
        reddit=None,
    )
    processed_count += v_count
    skipped_count += v_skip
    total_size += v_size

    return processed_count, skipped_count, total_size
