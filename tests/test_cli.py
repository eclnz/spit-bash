import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from spit_bash.cli import main
from support import artifact, job, sample, UPPER


def call(*argv, stdin=None):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), \
            mock.patch.object(sys, "stdin", io.StringIO(stdin or "")):
        code = main(list(argv))
    return code, out.getvalue(), err.getvalue()


class CliTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source.txt").write_text("hello")
        self.data = sample(self.root)
        self.dag = self.root / "jobs.spitdag"

    def write(self):
        self.dag.write_text(json.dumps(self.data))
        return str(self.dag)

    def test_plan_then_run_then_plan(self):
        code, out, _ = call("plan", self.write())
        self.assertEqual(code, 0)
        self.assertIn("[1] run     op1: output missing", out)
        code, out, err = call("run", str(self.dag))
        self.assertEqual(code, 0, err)
        self.assertIn("2 done, 0 checked, 0 failed, 0 current, 0 blocked", err)
        self.assertEqual((self.root / "final.txt").read_text(), "HELLO!")
        self.assertEqual(call("plan", str(self.dag))[1].count("skip"), 2)

    def test_reads_stdin_and_needs_root_when_dag_has_none(self):
        self.data["root"] = None
        code, _, err = call("plan", "-", stdin=json.dumps(self.data))
        self.assertEqual(code, 2)
        self.assertIn("pass --root", err)
        code, out, _ = call("plan", "-", "--root", str(self.root), stdin=json.dumps(self.data))
        self.assertEqual(code, 0)
        self.assertIn("[2] run", out)

    def test_blocked_job_does_not_stop_independent_jobs(self):
        self.data["external_inputs"].append(artifact("missing.txt"))
        self.data["jobs"].append(job(3, ["missing.txt"], "other.txt", UPPER))
        code, out, err = call("run", self.write())
        self.assertEqual(code, 1)
        self.assertIn("[3] blocked op3: missing external input: missing.txt", out)
        self.assertEqual((self.root / "final.txt").read_text(), "HELLO!")
        self.assertIn("2 done, 0 checked, 0 failed, 0 current, 1 blocked", err)

    def test_failure_prints_the_end_of_the_log(self):
        self.data["jobs"][0]["command"][2] = ["import sys; print('something broke'); sys.exit(4)"]
        code, _, err = call("run", self.write())
        self.assertEqual(code, 1)
        self.assertIn("[1] failed: command exited with status 4", err)
        self.assertIn("    something broke", err)
        self.assertIn("[2] failed: dependency 1 failed", err)

    def test_plan_json_and_partial_dag(self):
        self.data["left_out"] = [{"identity": "report[id=1]", "reasons": ["missing input"]}]
        code, out, err = call("plan", self.write(), "--json")
        self.assertEqual(code, 0)
        self.assertIn("partial DAG: 1 output(s) left out", err)
        plan = json.loads(out)
        self.assertEqual(plan["left_out"], ["report[id=1]"])
        self.assertEqual(plan["jobs"][0], {"id": 1, "operation": "op1", "status": "run", "reason": "output missing"})

    def test_adopt(self):
        (self.root / "work").mkdir()
        (self.root / "work/upper.txt").write_text("HELLO")
        (self.root / "final.txt").write_text("HELLO!")
        code, out, _ = call("adopt", self.write())
        self.assertEqual(code, 0)
        self.assertEqual(out.count("adopted"), 2)
        self.assertEqual(call("plan", str(self.dag))[1].count("current"), 2)

    def test_only_runs_the_chosen_jobs_and_what_they_need(self):
        self.data["jobs"][1]["outputs"]["output"]["entities"] = {"id": "final"}
        code, out, err = call("run", self.write(), "--only", "id=final")
        self.assertEqual(code, 0, err)
        self.assertIn("chose 1 of 2 jobs, and 1 they need", err)
        self.assertEqual((self.root / "final.txt").read_text(), "HELLO!")
        code, _, err = call("plan", str(self.dag), "--only", "id=other")
        self.assertEqual(code, 2)
        self.assertIn("no output has id=other; id takes final", err)
        code, _, err = call("plan", str(self.dag), "--only", "id")
        self.assertEqual(code, 2)
        self.assertIn("expected DIMENSION=VALUE", err)

    def test_options_for_spit_need_a_recipe_or_pipeline(self):
        code, _, err = call("plan", self.write(), "--partial")
        self.assertEqual(code, 2)
        self.assertIn("--partial is for `spit dag`", err)
        code, _, err = call("plan", str(self.dag), str(self.dag))
        self.assertEqual(code, 2)
        self.assertIn("expected one .spitdag, got 2 files", err)
        code, _, err = call("plan", "study.spitin", "--spit", str(self.root / "no-spit"))
        self.assertEqual(code, 2)
        self.assertIn("put spit on PATH, or pass --spit or set SPIT", err)

    def test_corrupt_state_and_bad_jobs_count(self):
        (self.root / ".spit-bash").mkdir()
        (self.root / ".spit-bash/state.jsonl").write_text("garbage\n")
        code, _, err = call("plan", self.write())
        self.assertEqual(code, 2)
        self.assertIn("cannot read state", err)
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            main(["run", str(self.dag), "-j", "0"])


if __name__ == "__main__":
    unittest.main()
