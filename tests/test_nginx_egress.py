import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from Backend.helper.nginx_egress import parse_nginx_access_line, summarize_nginx_egress_logs


class NginxEgressTests(unittest.TestCase):
    def test_parse_nginx_access_line_reads_actual_body_bytes(self):
        line = (
            '203.0.113.10 - - [25/May/2026:10:15:00 +0530] '
            '"GET /downloaded/token/file.mkv HTTP/1.1" 206 8388608 "-" "Stremio"'
        )

        parsed = parse_nginx_access_line(line)

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["path"], "/downloaded/token/file.mkv")
        self.assertEqual(parsed["status"], 206)
        self.assertEqual(parsed["body_bytes"], 8388608)

    def test_summarize_nginx_egress_counts_stream_paths_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "access.log"
            log_path.write_text(
                "\n".join(
                    [
                        '203.0.113.10 - - [25/May/2026:10:15:00 +0000] "GET /downloaded/token/file.mkv HTTP/1.1" 206 100 "-" "Stremio"',
                        '203.0.113.10 - - [25/May/2026:10:16:00 +0000] "GET /dl/token/file.mkv HTTP/1.1" 200 200 "-" "Stremio"',
                        '203.0.113.10 - - [25/May/2026:10:17:00 +0000] "GET /manifest.json HTTP/1.1" 200 999 "-" "Stremio"',
                        '203.0.113.10 - - [24/May/2026:10:18:00 +0000] "GET /downloaded/token/old.mkv HTTP/1.1" 206 300 "-" "Stremio"',
                        '203.0.113.10 - - [25/Apr/2026:10:19:00 +0000] "GET /downloaded/token/old-month.mkv HTTP/1.1" 206 400 "-" "Stremio"',
                    ]
                ),
                encoding="utf-8",
            )

            summary = summarize_nginx_egress_logs(
                [str(log_path)],
                ["/dl/", "/downloaded/"],
                now=datetime(2026, 5, 25, 12, 0, tzinfo=timezone.utc),
            )

        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["today"]["bytes"], 300)
        self.assertEqual(summary["month"]["bytes"], 600)
        self.assertEqual(summary["retained"]["bytes"], 1000)
        self.assertEqual(summary["paths"]["/downloaded/"]["today_bytes"], 100)
        self.assertEqual(summary["paths"]["/dl/"]["today_bytes"], 200)


if __name__ == "__main__":
    unittest.main()
