import unittest
from Backend.fastapi.routes.stream_routes import select_telegram_chunk_size
from Backend.helper.custom_dl import ByteStreamer
from Backend.config import Telegram


class TestBurstPrefillAndChunkSizing(unittest.TestCase):
    def test_select_telegram_chunk_size_ramp_up_probe(self):
        # Range header with start=0, small request <= 256KB -> should return 256KB
        size = select_telegram_chunk_size("bytes=0-32767", start=0, req_length=32768)
        self.assertEqual(size, 256 * 1024)

        # Range header with start=0, request=256KB -> should return 256KB
        size2 = select_telegram_chunk_size("bytes=0-262143", start=0, req_length=262144)
        self.assertEqual(size2, 256 * 1024)

    def test_select_telegram_chunk_size_sustained_seek(self):
        # Range header with start > 0 (seek) -> should return 512KB
        size = select_telegram_chunk_size("bytes=104857600-105906175", start=104857600, req_length=1048576)
        self.assertEqual(size, 512 * 1024)

    def test_select_telegram_chunk_size_full_stream(self):
        # No range header (full stream) -> should return CHUNK_SIZE (1 MB)
        size = select_telegram_chunk_size(None)
        self.assertEqual(size, ByteStreamer.CHUNK_SIZE)


if __name__ == "__main__":
    unittest.main()
