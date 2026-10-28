"""Tests for per-client proxy overrides (MULTI_TOKENn_PROXY — egress diversity).

While every viewer shares the WARP netns path, some pool clients can egress
via the Filelink relay (socks5 URL in MULTI_TOKEN<n>_PROXY) so congested
windows on one path don't stall the whole pool.
"""

import os
import unittest
from unittest import mock

from Backend.pyrofork.clients import TokenParser, client_proxy_for, parse_proxy_url


class ParseProxyUrlTests(unittest.TestCase):
    def test_socks5_with_auth(self):
        proxy = parse_proxy_url("socks5://user:p%40ss@10.0.0.120:1080")
        self.assertEqual(proxy["scheme"], "socks5")
        self.assertEqual(proxy["hostname"], "10.0.0.120")
        self.assertEqual(proxy["port"], 1080)
        self.assertEqual(proxy["username"], "user")
        self.assertEqual(proxy["password"], "p@ss")

    def test_no_auth(self):
        proxy = parse_proxy_url("socks5://172.18.0.1:1080")
        self.assertEqual(proxy["hostname"], "172.18.0.1")
        self.assertNotIn("username", proxy)
        self.assertNotIn("password", proxy)

    def test_empty_returns_none(self):
        self.assertIsNone(parse_proxy_url(""))
        self.assertIsNone(parse_proxy_url(None))

    def test_invalid_returns_none(self):
        self.assertIsNone(parse_proxy_url("ftp://host:1"))
        self.assertIsNone(parse_proxy_url("socks5://no-port-here"))
        self.assertIsNone(parse_proxy_url("socks5://host:notaport"))


class ClientProxyForTests(unittest.TestCase):
    def test_reads_env_key(self):
        with mock.patch.dict(os.environ, {"MULTI_TOKEN7_PROXY": "socks5://10.0.0.120:1080"}):
            proxy = client_proxy_for("MULTI_TOKEN7")
        self.assertEqual(proxy["hostname"], "10.0.0.120")

    def test_missing_env_is_none(self):
        clean = {k: v for k, v in os.environ.items() if not k.endswith("_PROXY")}
        with mock.patch.dict(os.environ, clean, clear=True):
            self.assertIsNone(client_proxy_for("MULTI_TOKEN9"))


class TokenParserTests(unittest.TestCase):
    def test_carries_env_key_and_sorts(self):
        env = {
            "MULTI_TOKEN2": "tok-b",
            "MULTI_TOKEN1": "tok-a",
            "UNRELATED": "x",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            parsed = TokenParser.parse_from_env()
        self.assertEqual(parsed[1], {"key": "MULTI_TOKEN1", "token": "tok-a"})
        self.assertEqual(parsed[2], {"key": "MULTI_TOKEN2", "token": "tok-b"})
        self.assertEqual(len(parsed), 2)

    def test_ids_follow_sorted_order(self):
        env = {"MULTI_TOKEN1": "a", "MULTI_TOKEN3": "c"}
        with mock.patch.dict(os.environ, env, clear=True):
            parsed = TokenParser.parse_from_env()
        self.assertEqual(parsed[1]["key"], "MULTI_TOKEN1")
        self.assertEqual(parsed[2]["key"], "MULTI_TOKEN3")

    def test_proxy_vars_are_not_tokens(self):
        env = {
            "MULTI_TOKEN1": "a",
            "MULTI_TOKEN1_PROXY": "socks5://172.18.0.1:1080",
            "MULTI_TOKEN2_PROXY": "socks5://172.18.0.1:1080",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            parsed = TokenParser.parse_from_env()
        self.assertEqual(len(parsed), 1, "_PROXY vars must never become clients")
        self.assertEqual(parsed[1]["token"], "a")


if __name__ == "__main__":
    unittest.main()
