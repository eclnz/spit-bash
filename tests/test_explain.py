"""`plan --job`: the evidence behind one job's decision."""

import contextlib
import copy
import io
import json
import os
import tempfile
import unittest
from pathlib import Path

from spit_bash import cli
from spit_bash.plan import adopt
from support import read, run, sample, state_at, placed
from test_checks import check, checked
from test_folders import folders_dag, pages


class ExplainTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source.txt").write_text("hello")
        self.data = sample(self.root)
        run(read(self.data), state_at(self.root))

    def explain(self, job, data=None, *flags):
        file = self.root / "d.spitdag"
        file.write_text(json.dumps(placed(data or self.data)))
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(["plan", str(file), "--root", str(self.root), "--job", str(job), *flags])
        return code, out.getvalue(), err.getvalue()

    def test_a_changed_input_shows_both_stamps_and_that_its_content_differs(self):
        (self.root / "source.txt").write_text("changed!")
        code, out, _ = self.explain(1)
        self.assertIn("decision: run, input changed", out)
        self.assertIn("recorded: 5 bytes", out)
        self.assertIn("now:      8 bytes", out)
        self.assertIn("content differs", out)
        self.assertIn("fingerprint: same", out)

    def test_a_touched_input_is_the_same_content(self):
        stat = (self.root / "source.txt").stat()
        os.utime(self.root / "source.txt", ns=(stat.st_atime_ns, stat.st_mtime_ns + 10**9))
        code, out, _ = self.explain(1)
        self.assertIn("decision: skip", out)
        self.assertIn("same content, so only touched", out)

    def test_comparing_by_stat_says_content_was_not_compared(self):
        (self.root / "source.txt").write_text("changed!")
        self.assertIn("not compared (--compare stat)", self.explain(1, None, "--compare", "stat")[1])

    def test_a_record_without_hashes_counts_as_changed(self):
        (self.root / ".spit-bash/state.jsonl").unlink()
        state = state_at(self.root)
        adopt(read(self.data), state)  # adoption records stamps and no hashes
        state.close()
        (self.root / "source.txt").write_text("changed!")
        self.assertIn("no content hash recorded", self.explain(1)[1])

    def test_a_changed_fingerprint_and_changed_checks_are_shown(self):
        data = copy.deepcopy(self.data)
        data["jobs"][0]["commands"][0]["command"].append(["extra"])
        self.assertIn("fingerprint: changed", self.explain(1, data)[1])
        data = checked(self.root, [check("before", "input", "source.txt", "hello")])
        self.assertIn("checks: differ (recorded none", self.explain(1, data)[1])

    def test_a_missing_output_is_shown(self):
        (self.root / "work/upper.txt").unlink()
        out = self.explain(1)[1]
        self.assertIn("decision: run, output missing", out)
        self.assertIn("now:      missing", out)

    def test_a_job_with_no_record_says_so(self):
        (self.root / ".spit-bash/state.jsonl").unlink()
        self.assertIn("record: none", self.explain(2)[1])

    def test_a_dependent_names_the_upstream_jobs_that_will_rewrite_its_inputs(self):
        (self.root / "source.txt").write_text("changed!")
        self.assertIn("upstream jobs that will or may rewrite its inputs: 1 (run)", self.explain(2)[1])

    def test_an_unknown_job_is_an_error_not_a_traceback(self):
        code, out, err = self.explain(99)
        self.assertEqual(code, 2)
        self.assertIn("no job 99", err)
        self.assertNotIn("Traceback", err)

    def test_explaining_does_not_touch_the_state_even_with_a_truncated_log(self):
        folder = self.root / ".spit-bash"
        with (folder / "state.jsonl").open("a") as stream:
            stream.write('{"key": "x", "rec')
        before = {p.name: p.read_bytes() for p in folder.iterdir() if p.is_file()}
        self.explain(1)
        self.assertEqual({p.name: p.read_bytes() for p in folder.iterdir() if p.is_file()}, before)

    def test_a_folder_shows_its_total_size_and_file_count(self):
        root = Path(tempfile.mkdtemp())
        pages(root)
        data = folders_dag(root)
        run(read(data), state_at(root))
        (root / "pages/3.txt").write_text("three")
        file = root / "d.spitdag"
        file.write_text(json.dumps(placed(data)))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            cli.main(["plan", str(file), "--root", str(root), "--job", "1"])
        self.assertIn("pages", out.getvalue())
        self.assertRegex(out.getvalue(), r"recorded: \d+ bytes in 2 files")
        self.assertRegex(out.getvalue(), r"now:      \d+ bytes in 3 files")


if __name__ == "__main__":
    unittest.main()
