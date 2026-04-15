import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import utils.media_download_manager as mdm
import utils.save_utils as save_utils


class FakeSubmission(SimpleNamespace):
    pass


class MediaSourceSelectionTests(unittest.TestCase):
    def setUp(self):
        self._old_media_trace = getattr(save_utils, "_MEDIA_TRACE", False)
        self._old_redgifs_trace = getattr(mdm, "REDGIFS_TRACE", False)
        save_utils._MEDIA_TRACE = False
        mdm.REDGIFS_TRACE = False

    def tearDown(self):
        save_utils._MEDIA_TRACE = self._old_media_trace
        mdm.REDGIFS_TRACE = self._old_redgifs_trace

    def test_redgifs_url_is_video_like(self):
        sub = FakeSubmission(url="https://www.redgifs.com/watch/exampleclip", is_video=False, preview=None, media=None, secure_media=None)
        self.assertTrue(save_utils._is_video_like_submission(sub))

    def test_extract_reddit_video_url_preserves_redgifs_url(self):
        sub = FakeSubmission(url="https://www.redgifs.com/watch/exampleclip", media=None, secure_media=None, preview=None)
        self.assertEqual(
            save_utils._extract_reddit_video_url(sub),
            "https://www.redgifs.com/watch/exampleclip",
        )

    def test_redgifs_page_candidate_builder_finds_pages_and_rewrites_silent_mp4(self):
        html = """
        <html>
          <head>
            <meta property="og:video" content="https://media.redgifs.com/Foo-silent.mp4">
            <meta name="twitter:player:stream" content="https://media.redgifs.com/Foo.mp4">
          </head>
          <body>
            <script>
              window.__DATA__ = {"has_audio": true};
            </script>
          </body>
        </html>
        """

        with patch.object(mdm, "_fetch_html", return_value=html):
            info = mdm._redgifs_page_candidates("https://www.redgifs.com/watch/foo")

        self.assertTrue(info["has_audio"])
        self.assertIn("https://www.redgifs.com/watch/foo", info["pages"])
        self.assertIn("https://www.redgifs.com/ifr/foo", info["pages"])
        self.assertIn("https://media.redgifs.com/Foo-silent.mp4", info["directs"])
        self.assertIn("https://media.redgifs.com/Foo.mp4", info["directs"])

    def test_download_image_does_not_fall_back_for_video_like_url(self):
        with patch("utils.media_download_manager.download_media_file", return_value=None), patch(
            "utils.save_utils._download_image_fallback"
        ) as fallback:
            path, size = save_utils.download_image(
                "https://www.redgifs.com/watch/exampleclip",
                "/tmp",
                "abc123",
            )

        self.assertIsNone(path)
        self.assertEqual(size, 0)
        fallback.assert_not_called()


if __name__ == "__main__":
    unittest.main()
