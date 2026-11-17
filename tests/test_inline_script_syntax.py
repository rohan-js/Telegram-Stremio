"""Tests: every inline <script> block must be syntactically valid JavaScript.

The v5.3.3 UI adoption left a truncated ``copyUrl`` function in dashboard.html —
an unbalanced brace that killed the ENTIRE script block (token usage, egress and
VPS card fills all silently stopped). Two more blocks sat in ``{% block title %}``
with truncated function bodies. node --check catches all of these classes;
net-brace-count scans cannot.
"""

import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "Backend" / "fastapi" / "templates"


def _js_check_ready() -> bool:
    return shutil.which("node") is not None


def _strip_jinja(js: str) -> str:
    js = re.sub(r"\{\{.*?\}\}", "0", js, flags=re.S)      # expressions -> number token
    js = re.sub(r"\{%-?.*?-?%\}", "", js, flags=re.S)     # statements -> blank
    return js


class InlineScriptSyntaxTests(unittest.TestCase):
    def test_every_inline_script_parses(self):
        if not _js_check_ready():
            self.skipTest("node not available for --check")
        templates = sorted(TEMPLATES_DIR.glob("*.html"))
        self.assertGreater(len(templates), 10, "template dir not found")
        failures = []
        for path in templates:
            src = path.read_text(encoding="utf-8")
            for m in re.finditer(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", src, re.S | re.I):
                js = _strip_jinja(m.group(1))
                with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as tf:
                    tf.write(js)
                    tmp = Path(tf.name)
                try:
                    r = subprocess.run(
                        ["node", "--check", str(tmp)],
                        capture_output=True, text=True, timeout=30,
                    )
                finally:
                    tmp.unlink(missing_ok=True)
                if r.returncode != 0:
                    first_line = (r.stderr or "").strip().splitlines()
                    detail = first_line[1] if len(first_line) > 1 else (first_line[0] if first_line else "node --check failed")
                    tpl_line = src[: m.start(1)].count("\n") + 1
                    failures.append(f"{path.name} (script at template line {tpl_line}): {detail}")
        self.assertEqual(
            failures, [],
            "invalid inline JavaScript (block is dead in the browser):\n" + "\n".join(failures),
        )


if __name__ == "__main__":
    unittest.main()
