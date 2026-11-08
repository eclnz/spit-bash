"""What `plan` and `run` print: causes once, counts for the rest.

Each test builds a small DAG and states the exact text. Tests of output that does not exist yet
are expected failures until it does.
"""

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from spit_bash.cli import main
from support import artifact, command, document, job, sample, UPPER

MISSING = "no-such-spit-bash-program"
FAILS = "import sys; print('boom'); sys.exit(3)"


def call(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), \
            mock.patch.object(sys, "stdin", io.StringIO("")):
        code = main(list(argv))
    return code, out.getvalue(), err.getvalue()


def named(item, operation):
    item["operation"] = operation
    return item


class OutputTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        (self.root / "source.txt").write_text("hello")
        self.dag = self.root / "jobs.spitdag"

    def write(self, data):
        self.dag.write_text(json.dumps(data))
        return str(self.dag)

    def blocked_chain(self, roots=2):
        """`roots` jobs that need a program that is missing, one job after each, and two that can start."""
        jobs, inputs = [], ["source.txt"]
        for n in range(roots):
            root, after = 10 + 2 * n, 11 + 2 * n
            inputs.append(f"in{n}.txt")
            (self.root / f"in{n}.txt").write_text("x")
            blocked = named(job(root, [f"in{n}.txt"], f"made{n}.txt", UPPER, dependents=[after]), "tool")
            blocked["commands"][0]["command"][0] = [MISSING]
            jobs += [blocked, named(job(after, [f"made{n}.txt"], f"after{n}.txt", UPPER, depends_on=[root]), "after")]
        jobs += [named(job(1, ["source.txt"], "a.txt", UPPER), "ok"), named(job(2, ["source.txt"], "b.txt", UPPER), "ok")]
        data = document(self.root, sorted(jobs, key=lambda item: item["id"]), inputs)
        data["executables"] = [MISSING]
        return data

    # Plan.

    @unittest.expectedFailure
    def test_a_plan_with_nothing_to_do_is_one_line(self):
        data = sample(self.root)
        call("run", self.write(data))
        self.assertEqual(call("plan", str(self.dag))[1], "all 2 jobs are current\n")

    @unittest.expectedFailure
    def test_a_first_plan_is_counts_only(self):
        code, out, _ = call("plan", self.write(sample(self.root)))
        self.assertEqual((code, out), (0, "1 job can start, 1 job will wait for it\n"))

    @unittest.expectedFailure
    def test_a_blocked_cause_is_listed_once_with_how_many_jobs_depend_on_it(self):
        code, out, _ = call("plan", self.write(self.blocked_chain()))
        self.assertEqual(code, 1)
        self.assertEqual(out, "2 jobs can start, 4 are blocked\n"
                              f"blocked: tool x2: program not found: {MISSING} (2 more depend on them)\n")

    @unittest.expectedFailure
    def test_verbose_plan_lists_every_job_in_the_same_words(self):
        code, out, _ = call("plan", self.write(sample(self.root)), "--verbose")
        self.assertEqual(out.splitlines()[-2:], ["[1] ready   op1: output missing", "[2] waits   op2: waits for 1"])

    @unittest.expectedFailure
    def test_plan_json_names_the_root_cause_and_what_it_holds_up(self):
        _, out, _ = call("plan", self.write(self.blocked_chain(1)), "--json")
        jobs = {item["id"]: item for item in json.loads(out)["jobs"]}
        self.assertEqual((jobs[10]["caused_by"], jobs[10]["holds_up"]), (None, 1))
        self.assertEqual((jobs[11]["caused_by"], jobs[11]["holds_up"]), (10, 0))

    # Run.

    @unittest.expectedFailure
    def test_a_failure_is_reported_once_with_what_it_held_up(self):
        data = sample(self.root)
        data["jobs"][0]["commands"][0]["command"][2] = [FAILS]
        code, _, err = call("run", self.write(data))
        self.assertEqual(code, 1)
        self.assertIn("[1] failed: command exited with status 3", err)
        self.assertIn("[1] held up 1 other job", err)
        self.assertNotIn("[2] failed", err)
        self.assertIn("0 done, 0 checked, 1 failed, 1 not run (an upstream job failed), 0 current, 0 blocked", err)

    @unittest.expectedFailure
    def test_identical_failures_are_grouped(self):
        jobs = [named(job(n, ["source.txt"], f"out{n}.txt", FAILS), "import") for n in (1, 2, 3)]
        code, _, err = call("run", self.write(document(self.root, jobs, ["source.txt"])))
        self.assertEqual(err.count("last lines of"), 1)
        self.assertIn("[1] the same failure in 2 other jobs: 2, 3", err)
        self.assertIn("3 failed", err)

    @unittest.expectedFailure
    def test_verbose_run_says_why_a_job_did_not_run(self):
        data = sample(self.root)
        data["jobs"][0]["commands"][0]["command"][2] = [FAILS]
        _, _, err = call("run", self.write(data), "--verbose")
        self.assertIn("[2] not run: job 1 failed", err)

    def test_a_job_below_a_blocked_one_is_never_started(self):
        data = self.blocked_chain(1)
        marker = self.root / "started.txt"
        after = next(item for item in data["jobs"] if item["id"] == 11)
        after["commands"][0]["command"] = command(
            f"from pathlib import Path; Path({str(marker)!r}).write_text('x')", "made0.txt", "after0.txt")
        call("run", self.write(data))
        self.assertFalse(marker.exists())
        self.assertTrue((self.root / "a.txt").exists())


if __name__ == "__main__":
    unittest.main()
