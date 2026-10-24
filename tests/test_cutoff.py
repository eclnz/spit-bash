import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from spit_bash.cli import main
from spit_bash.plan import Status, plan
from support import EXCLAIM, job, read, run, sample, state_at


def chain(root):
    """source.txt -> work/upper.txt -> final.txt -> last.txt"""
    data = sample(root)
    data["jobs"][1]["dependents"] = [3]
    data["jobs"].append(job(3, ["final.txt"], "last.txt", EXCLAIM, depends_on=[2], fingerprint="c" * 16))
    return data


def call(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), \
            mock.patch.object(sys, "stdin", io.StringIO("")):
        code = main(list(argv))
    return code, out.getvalue(), err.getvalue()


class CutoffTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source.txt").write_text("hello")
        self.data = chain(self.root)
        self.dag = read(self.data)
        run(self.dag, state_at(self.root))

    def edit_source(self, text):
        path = self.root / "source.txt"
        later = path.stat().st_mtime_ns + 10_000_000_000
        path.write_text(text)
        os.utime(path, ns=(later, later))

    def test_an_edit_that_changes_nothing_downstream_stops_at_the_first_job(self):
        # Upper-casing makes "Hello" and "hello" the same text.
        self.edit_source("Hello")
        decisions = plan(self.dag, state_at(self.root))
        self.assertEqual([d.status for d in decisions], [Status.RUN, Status.MAYBE, Status.MAYBE])
        final = self.root / "last.txt"
        before = final.stat().st_mtime_ns
        _, results = run(self.dag, state_at(self.root))
        self.assertFalse(any(r.error for r in results))
        self.assertEqual([r.settled for r in results], [None, Status.SKIP, Status.SKIP])
        self.assertEqual(final.stat().st_mtime_ns, before)
        # The stamps were stored, so the next plan has nothing left to settle.
        self.assertEqual([d.status for d in plan(self.dag, state_at(self.root))], [Status.SKIP] * 3)

    def test_an_edit_that_changes_the_output_runs_everything_below(self):
        self.edit_source("goodbye")
        _, results = run(self.dag, state_at(self.root))
        self.assertEqual([r.settled for r in results], [None, Status.RUN, Status.RUN])
        self.assertEqual((self.root / "last.txt").read_text(), "GOODBYE!!")

    def test_a_job_with_a_reason_of_its_own_still_runs(self):
        self.edit_source("Hello")
        self.data["jobs"][2]["fingerprint"] = "d" * 16
        decisions = plan(read(self.data), state_at(self.root))
        self.assertEqual([d.status for d in decisions], [Status.RUN, Status.MAYBE, Status.RUN])
        self.assertEqual(decisions[2].reason, "job fingerprint changed")

    def test_without_hashing_a_job_below_a_rerun_runs(self):
        self.edit_source("Hello")
        decisions = plan(self.dag, state_at(self.root), hashing=False)
        self.assertEqual([d.status for d in decisions], [Status.RUN] * 3)
        self.assertEqual(decisions[1].reason, "dependency will run")

    def test_a_job_with_no_command_below_a_maybe_is_blocked(self):
        self.data["jobs"][2]["command"] = None
        self.edit_source("Hello")
        decisions = plan(read(self.data), state_at(self.root))
        self.assertEqual([d.status for d in decisions], [Status.RUN, Status.MAYBE, Status.BLOCKED])
        self.assertEqual(decisions[2].reason, "no command and dependency may run")

    def test_a_job_whose_input_is_missing_is_not_a_maybe(self):
        self.edit_source("Hello")
        (self.root / "final.txt").unlink()
        decisions = plan(self.dag, state_at(self.root))
        # Nothing is there to compare, so the job below it cannot be settled early.
        self.assertEqual([d.status for d in decisions], [Status.RUN] * 3)
        self.assertEqual([d.reason for d in decisions][1:], ["output missing", "dependency will run"])

    def test_plan_and_run_say_how_many_may_run_and_how_many_were_cut_off(self):
        self.edit_source("Hello")
        path = self.root / "jobs.spitdag"
        path.write_text(json.dumps(self.data))
        code, out, err = call("plan", str(path))
        self.assertEqual(code, 0)
        self.assertIn("[2] maybe   op2: runs if an upstream output changes", out)
        self.assertIn("1 will run, 2 more may, if an upstream output changes", err)
        code, _, err = call("run", str(path))
        self.assertEqual(code, 0, err)
        self.assertIn("1 done, 0 checked, 0 failed, 2 current, 0 blocked, "
                      "2 of the current ones not rerun because upstream output was unchanged", err)

    def test_the_estimate_counts_a_maybe_job_until_it_is_skipped(self):
        self.edit_source("Hello")
        path = self.root / "jobs.spitdag"
        path.write_text(json.dumps(self.data))
        code, out, err = call("run", str(path))
        self.assertEqual(code, 0, err)
        # Every job has a time from the first run, so there is an estimate at once.
        self.assertRegex(out, r"(?m)^1 job to run, and 2 more that may, (about|under) ")
        self.assertRegex(out, r"\[1\] done in [^;]+; 2 left, (about|under) ")
        self.assertIn("[2] skip op2: upstream output unchanged\n[3] skip op3: upstream output unchanged\n", out)

    def test_a_failed_job_stops_the_maybes_below_it(self):
        self.edit_source("Hello")
        self.data["jobs"][0]["command"][2] = ["import sys; sys.exit(3)"]
        _, results = run(read(self.data), state_at(self.root))
        self.assertEqual([bool(r.error) for r in results], [True, True, True])
        self.assertEqual(results[1].error, "dependency 1 failed")


if __name__ == "__main__":
    unittest.main()
