import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from Backend.helper.log_tools import LOG_PATHS, read_recent_logs, redact_log_text


def _write_lines(path: Path, lines: list) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class LogToolsTests(unittest.TestCase):
    def tearDown(self):
        # LOG_PATHS may be patched to a temp file; restore module defaults so
        # other tests never see the temp path.
        try:
            import Backend.helper.log_tools as mod

            mod.LOG_PATHS[:] = LOG_PATHS
        except Exception:
            pass

    def test_reads_tail_of_live_log_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_file = Path(tmp) / "log.txt"
            lines = [f"line-{i:03d} info message" for i in range(30)]
            _write_lines(log_file, lines)

            with patch("Backend.helper.log_tools.LOG_PATHS", [log_file]):
                result = read_recent_logs(max_bytes=200_000)

            self.assertEqual(result["path"], str(log_file))
            self.assertGreater(result["bytes"], 0)
            self.assertIn("line-000", result["text"])
            self.assertIn("line-029", result["text"])

    def test_small_max_bytes_returns_only_tail(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_file = Path(tmp) / "log.txt"
            # Each line is ~70 bytes; 100 lines ≈ 7KB.
            lines = [f"line-{i:03d} " + "x" * 60 for i in range(100)]
            _write_lines(log_file, lines)

            with patch("Backend.helper.log_tools.LOG_PATHS", [log_file]):
                result = read_recent_logs(max_bytes=400)

            # Only the tail should survive: last line present, first line gone.
            self.assertIn("line-099", result["text"])
            self.assertNotIn("line-000", result["text"])
            self.assertLessEqual(result["bytes"], 800)

    def test_redaction_masks_tokens_and_mongo_uri(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_file = Path(tmp) / "log.txt"
            token = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefgh"
            mongo_uri = "mongodb+srv://user:secret@telegram-stremio.kolyca4.mongodb.net/app"
            _write_lines(log_file, [f"Stream started token={token}", f"DB {mongo_uri}", "plain line"])

            with patch("Backend.helper.log_tools.LOG_PATHS", [log_file]):
                result = read_recent_logs(max_bytes=200_000)

            self.assertIn("<redacted>", result["text"])
            self.assertNotIn(token, result["text"])
            self.assertNotIn(mongo_uri, result["text"])
            self.assertIn("plain line", result["text"])

    def test_no_log_file_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "does-not-exist.log"
            with patch("Backend.helper.log_tools.LOG_PATHS", [missing]):
                result = read_recent_logs()
            self.assertEqual(result, {"path": "", "text": "", "bytes": 0})


class RedactLogTextTests(unittest.TestCase):
    def test_masks_bot_token_and_mongo_uri(self):
        text = "token=123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefgh db=mongodb+srv://u:p@host/db"
        redacted = redact_log_text(text)
        self.assertIn("<redacted>", redacted)
        self.assertNotIn("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefgh", redacted)
        self.assertNotIn("u:p@host", redacted)


if __name__ == "__main__":
    unittest.main()
