"""Check the compiler selection used by release, development and scheduled CI."""

import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


class BranchTests(unittest.TestCase):
    def test_compiler_selection(self):
        workflow = (Path(__file__).parents[1] / ".github/workflows/ci.yml").read_text()
        script = textwrap.dedent(workflow.split("        run: |\n", 1)[1].split("      - uses:", 1)[0])
        release = "82f6f0bdf63cb92dccc5a206ea9a764845fd5a74"
        cases = [
            ("push", "main", "main", False, release),
            ("pull_request", "fix/examples", "main", False, release),
            ("push", "dev", "dev", False, "dev"),
            ("pull_request", "fix/examples", "dev", False, release),
            ("push", "fix/examples", "fix/examples", False, release),
            ("push", "fix/examples", "fix/examples", True, "fix/examples"),
            ("pull_request", "fix/examples", "main", True, "fix/examples"),
            ("schedule", "main", "main", True, "dev"),
        ]
        for event, branch, base, matching, expected in cases:
            with self.subTest(event=event, branch=branch, base=base, matching=matching):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    git = root / "git"
                    git.write_text("#!/bin/sh\nexit " + ("0" if matching else "2") + "\n")
                    git.chmod(0o755)
                    output = root / "output"
                    env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}",
                               EVENT=event, BRANCH=branch, BASE=base, GITHUB_OUTPUT=str(output))
                    result = subprocess.run(["bash", "-e", "-c", script], env=env,
                                            capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(output.read_text(), f"ref={expected}\n")

    def test_scheduled_checkouts_use_dev(self):
        workflow = (Path(__file__).parents[1] / ".github/workflows/ci.yml").read_text()
        checkouts = workflow.split("      - uses: actions/checkout@v4\n")[1:]
        self.assertEqual(len(checkouts), 4)
        for checkout in (checkouts[0], checkouts[1], checkouts[3]):
            self.assertTrue(checkout.startswith(
                "        with:\n          ref: ${{ github.event_name == 'schedule' && 'dev' || '' }}\n"))
