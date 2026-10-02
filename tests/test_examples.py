"""Run the examples with a real `spit`, so a change to SPIT's DAG format fails here.

Set SPIT to the binary, or put `spit` on PATH; CI builds it from SPIT's main branch.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

EXAMPLES = Path(__file__).parents[1] / "examples"
SPIT = os.environ.get("SPIT") or shutil.which("spit")


def spit_bash(*args, stdin=None):
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).parents[1] / "src"), SPIT=SPIT or "spit")
    return subprocess.run([sys.executable, "-m", "spit_bash", *args], input=stdin, env=env,
                          capture_output=True, text=True)


@unittest.skipUnless(SPIT, "set SPIT or put spit on PATH")
class ExampleTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.examples = Path(directory.name) / "examples"
        shutil.copytree(EXAMPLES, self.examples)

    def dag_json(self, recipe):
        return subprocess.run([SPIT, "dag", str(self.examples / recipe), "--json"],
                              check=True, capture_output=True, text=True).stdout

    def test_lines(self):
        dag = self.dag_json("lines/lines.spitin")
        result = spit_bash("run", "-", "-j", "3", stdin=dag)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = self.examples / "lines/output/merged"
        self.assertEqual((output / "group=alpha.txt").read_text(), "apple\nbanana\npear\n")
        self.assertEqual((output / "group=beta.txt").read_text(), "yak\nzebra\n")
        result = spit_bash("plan", "-", stdin=self.dag_json("lines/lines.spitin"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.count(" skip "), 5, result.stdout)

    def test_runs_a_recipe_or_pipeline_and_chooses_jobs(self):
        lines = self.examples / "lines"
        output = lines / "output/merged"
        result = spit_bash("run", str(lines / "lines.spitin"), "--only", "group=beta")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("chose 2 of 5 jobs\n", result.stderr)
        self.assertEqual((output / "group=beta.txt").read_text(), "yak\nzebra\n")
        self.assertFalse((output / "group=alpha.txt").exists())

        result = spit_bash("plan", str(lines / "lines.spit"), "--root", str(lines), "--product", "merged")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("[5] skip    merge: current", result.stdout)
        self.assertIn("[4] run     merge: dependency will run", result.stdout)

        inventory = subprocess.run([SPIT, "inputs", str(lines / "lines.spitin")],
                                   check=True, capture_output=True, text=True).stdout
        result = spit_bash("run", str(lines / "lines.spit"), "-", "--root", str(lines), stdin=inventory)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((output / "group=alpha.txt").read_text(), "apple\nbanana\npear\n")

        result = spit_bash("plan", str(lines / "missing.spitin"))
        self.assertEqual(result.returncode, 2)
        self.assertIn("`spit dag` exited with status 1", result.stderr)

    def test_verify(self):
        dag = self.examples / "verify/checked_copy.spitdag"
        subprocess.run([SPIT, "dag", str(self.examples / "verify/checked_copy.spitin"), "-o", str(dag)],
                       check=True, capture_output=True)
        result = spit_bash("run", str(dag))
        self.assertEqual(result.returncode, 0, result.stderr)
        output = self.examples / "verify/output/copied/part=one.txt"
        self.assertEqual(output.read_text(), "Verified input.\n")

        output.unlink()
        (self.examples / "verify/input/one.txt").write_text("")
        result = spit_bash("run", str(dag))
        self.assertEqual(result.returncode, 1)
        self.assertIn("verify 1 exited with status 1", result.stderr)
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
