"""Run the examples with a real `spit`, so a change to SPIT's DAG format fails here.

Set SPIT to the binary, or put `spit` on PATH; CI builds it from SPIT's dev branch,
or from SPIT's branch of the same name as this one when there is one.
"""

import os
import shutil
import subprocess
import sys
import tarfile
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

    def test_checks(self):
        recipe = str(self.examples / "checks/notes.spitin")
        result = spit_bash("run", recipe)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("2 done, 0 checked, 0 failed", result.stderr)

        # A note without the word fails its output check, though `cp` succeeded.
        (self.examples / "checks/input/tuesday.txt").write_text("Rest.\n")
        result = spit_bash("run", recipe)
        self.assertEqual(result.returncode, 1)
        self.assertIn("[2] failed: check contains(today) on output output output/copied/day=tuesday.txt "
                      "failed: exited with status 1", result.stderr)

        # A changed check is run on the files that exist, without copying again.
        pipeline = self.examples / "checks/notes.spit"
        pipeline.write_text(pipeline.read_text().replace("contains(today)", "contains(for)"))
        result = spit_bash("plan", recipe)
        self.assertIn("[1] check   copy: checks changed", result.stdout)
        self.assertIn("[2] run     copy: no successful run recorded", result.stdout)
        result = spit_bash("run", recipe)
        self.assertEqual(result.returncode, 1)
        self.assertIn("0 done, 1 checked, 1 failed", result.stderr)

    def test_folders(self):
        recipe = str(self.examples / "folders/albums.spitin")
        result = spit_bash("run", recipe, "-j", "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        album = self.examples / "folders/output/album/trip=alpha"
        self.assertEqual((album / "day1/morning.txt").read_text(), "harbour\n")
        with tarfile.open(self.examples / "folders/output/archive/trip=alpha.tar") as archive:
            self.assertEqual(sorted(archive.getnames()), [".", "./day1", "./day1/morning.txt", "./evening.txt"])

        (self.examples / "folders/input/alpha/day1/morning.txt").write_text("harbour at dawn\n")
        result = spit_bash("plan", recipe)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("[1] run     copy: input changed", result.stdout)
        self.assertIn("[2] skip    copy: current", result.stdout)
        result = spit_bash("run", recipe)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((album / "day1/morning.txt").read_text(), "harbour at dawn\n")


if __name__ == "__main__":
    unittest.main()
