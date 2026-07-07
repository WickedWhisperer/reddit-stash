import os
import unittest

from utils.path_security import create_ordered_reddit_item_path


class ArchiveLayoutTests(unittest.TestCase):
    def test_ordered_item_path_is_nested_and_timestamped(self):
        result = create_ordered_reddit_item_path(
            "/tmp/reddit",
            "pics",
            "POST",
            "abc123",
            created_utc=1700000000,
        )
        self.assertTrue(result.is_safe)
        self.assertIsNotNone(result.safe_path)

        parts = result.safe_path.split(os.sep)
        self.assertEqual(parts[-4], "pics")
        self.assertEqual(parts[-3], "POST")
        self.assertTrue(parts[-2].endswith("_POST_abc123"))
        self.assertEqual(parts[-1], "POST_abc123.md")

    def test_ordered_item_path_falls_back_when_timestamp_missing(self):
        result = create_ordered_reddit_item_path(
            "/tmp/reddit",
            "pics",
            "COMMENT",
            "def456",
        )
        self.assertTrue(result.is_safe)
        self.assertIn("COMMENT_def456.md", result.safe_path)


if __name__ == "__main__":
    unittest.main()
