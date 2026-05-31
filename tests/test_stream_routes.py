import unittest

from Backend.fastapi.routes.stream_routes import resolve_video_mime_type


class StreamMimeTypeTests(unittest.TestCase):
    def test_known_video_extension_overrides_generic_telegram_mime(self):
        self.assertEqual(
            resolve_video_mime_type("movie.mkv", "application/octet-stream"),
            "video/x-matroska",
        )

    def test_known_video_extension_works_without_telegram_mime(self):
        self.assertEqual(resolve_video_mime_type("movie.mp4", None), "video/mp4")

    def test_unknown_extension_preserves_valid_telegram_video_mime(self):
        self.assertEqual(
            resolve_video_mime_type("movie.custom", "video/x-custom"),
            "video/x-custom",
        )

    def test_unknown_extension_with_generic_mime_falls_back_safely(self):
        self.assertEqual(
            resolve_video_mime_type("movie.unknownext", "application/octet-stream"),
            "application/octet-stream",
        )


if __name__ == "__main__":
    unittest.main()
