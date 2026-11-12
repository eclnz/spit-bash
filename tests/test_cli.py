import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from spit_bash.cli import main
from support import artifact, job_entry, sample, UPPER, placed


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
        self.dag.write_text(json.dumps(placed(self.data)))
        return str(self.dag)

    def test_plan_then_run_then_plan(self):
        code, out, _ = call("plan", self.write(), "--verbose")
        self.assertEqual(code, 0)
        self.assertIn("[1] ready   op1: output missing", out)
        code, out, err = call("run", str(self.dag))
        self.assertEqual(code, 0, err)
        self.assertIn("2 done, 0 checked, 0 failed, 0 current, 0 blocked", err)
        self.assertEqual((self.root / "final.txt").read_text(), "HELLO!")
        self.assertEqual(call("plan", str(self.dag))[1], "all 2 jobs are current\n")

    def test_run_counts_current_jobs_and_lists_them_only_when_asked(self):
        call("run", self.write())
        code, out, _ = call("run", str(self.dag))
        self.assertEqual(code, 0)
        self.assertEqual(out.splitlines()[0], "all 2 jobs are current")
        self.assertNotIn("op1", out)
        _, out, _ = call("run", str(self.dag), "--verbose")
        self.assertIn("[1] current op1", out)
        self.assertIn("[2] current op2", out)

    def test_run_counts_the_jobs_that_will_do_something_and_lists_them_only_when_asked(self):
        code, out, _ = call("run", self.write())
        self.assertEqual(out.splitlines()[0], "1 job can start, 1 job will wait for it")
        self.assertNotIn("output missing", out)

    def test_a_partial_run_says_which_jobs_it_left_out_of_date(self):
        call("run", self.write())
        (self.root / "source.txt").write_text("changed")
        code, _, err = call("run", str(self.dag), "--product", "work/upper.txt")
        self.assertEqual(code, 0, err)
        self.assertIn("1 job outside the selection read outputs this run changed", err)
        self.assertEqual((self.root / "final.txt").read_text(), "HELLO!")
        code, _, err = call("run", str(self.dag))
        self.assertNotIn("outside the selection", err)

    def test_a_partial_run_that_rewrites_the_same_bytes_leaves_nothing_out_of_date(self):
        call("run", self.write())
        code, _, err = call("run", str(self.dag), "--product", "work/upper.txt", "--force")
        self.assertEqual(code, 0, err)
        self.assertIn("1 done", err)
        self.assertIn("1 output was rewritten with the same content; nothing outside the selection needs to run", err)
        self.assertNotIn("out of date", err)
        self.assertEqual(call("plan", str(self.dag))[1], "all 2 jobs are current\n")

    def test_a_partial_run_with_compare_stat_counts_every_rewritten_output_as_changed(self):
        call("run", self.write())
        _, _, err = call("run", str(self.dag), "--product", "work/upper.txt", "--force", "--compare", "stat")
        self.assertIn("1 job outside the selection read outputs this run changed", err)

    def test_reads_stdin_and_needs_root_when_dag_has_none(self):
        self.data["root"] = None
        code, _, err = call("plan", "-", stdin=json.dumps(placed(self.data)))
        self.assertEqual(code, 2)
        self.assertIn("pass --root", err)
        code, out, _ = call("plan", "-", "--root", str(self.root), stdin=json.dumps(placed(self.data)))
        self.assertEqual(code, 0)
        self.assertIn("1 job can start, 1 job will wait for it", out)

    def test_blocked_job_does_not_stop_independent_jobs(self):
        self.data["external_inputs"].append(artifact("missing.txt"))
        self.data["jobs"].append(job_entry(3, ["missing.txt"], "other.txt", UPPER))
        code, out, err = call("run", self.write(), "--partial")
        self.assertEqual(code, 1)
        self.assertIn("blocked: op3: missing external input: missing.txt", out)
        self.assertEqual((self.root / "final.txt").read_text(), "HELLO!")
        self.assertIn("2 done, 0 checked, 0 failed, 0 current, 1 blocked", err)

    def test_failure_prints_the_end_of_the_log(self):
        self.data["jobs"][0]["commands"][0]["command"][2] = ["import sys; print('something broke'); sys.exit(4)"]
        code, _, err = call("run", self.write())
        self.assertEqual(code, 1)
        self.assertIn("[1] failed: command exited with status 4", err)
        self.assertIn("    something broke", err)
        self.assertIn("[1] held up 1 other job", err)
        self.assertNotIn("[2] failed", err)

    def test_plan_json_and_partial_dag(self):
        self.data["left_out"] = [{"identity": "report[id=1]", "reasons": ["missing input"]}]
        code, out, err = call("plan", self.write(), "--json")
        self.assertEqual(code, 0)
        self.assertIn("partial DAG: 1 output(s) left out", err)
        plan = json.loads(out)
        self.assertEqual(plan["left_out"], ["report[id=1]"])
        self.assertEqual(plan["jobs"][0], {"id": 1, "operation": "op1", "status": "run", "reason": "output missing",
                                           "waits_for": [], "caused_by": None, "holds_up": 0})

    def test_adopt(self):
        (self.root / "work").mkdir()
        (self.root / "work/upper.txt").write_text("HELLO")
        (self.root / "final.txt").write_text("HELLO!")
        code, out, _ = call("adopt", self.write())
        self.assertEqual(code, 0)
        self.assertEqual(out.count("adopted"), 2)
        self.assertEqual(call("plan", str(self.dag))[1], "all 2 jobs are current\n")

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

    def test_force_only_reruns_the_matching_job_and_leaves_current_prerequisites_alone(self):
        self.data["jobs"][1]["outputs"]["output"]["entities"] = {"id": "final"}
        self.assertEqual(call("run", self.write())[0], 0)
        upstream = self.root / "work/upper.txt"
        upstream_stamp = upstream.stat().st_mtime_ns

        code, out, err = call("run", str(self.dag), "--only", "id=final", "--force")

        self.assertEqual(code, 0, err)
        self.assertIn("1 job can start, 1 job is current", out)
        self.assertEqual(upstream.stat().st_mtime_ns, upstream_stamp)
        self.assertEqual((self.root / "final.txt").read_text(), "HELLO!")

    def test_force_selected_job_still_runs_when_a_prerequisite_is_stale(self):
        self.data["jobs"][1]["outputs"]["output"]["entities"] = {"id": "final"}
        self.assertEqual(call("run", self.write())[0], 0)
        (self.root / "source.txt").write_text("goodbye")

        code, out, err = call("run", str(self.dag), "--only", "id=final", "--force")

        self.assertEqual(code, 0, err)
        self.assertIn("1 job can start, 1 job will wait for it", out)
        self.assertEqual((self.root / "work/upper.txt").read_text(), "GOODBYE")
        self.assertEqual((self.root / "final.txt").read_text(), "GOODBYE!")

    def test_force_without_a_selection_still_reruns_every_command_job(self):
        self.assertEqual(call("run", self.write())[0], 0)

        code, out, err = call("run", str(self.dag), "--force")

        self.assertEqual(code, 0, err)
        self.assertIn("1 job can start, 1 job will wait for it", out)

    def test_force_with_product_or_stage_only_forces_matching_jobs(self):
        self.data["jobs"][1]["outputs"]["output"]["product"] = "final"
        self.data["jobs"][0]["stage"] = ["prepare"]
        self.data["jobs"][1]["stage"] = ["finish"]
        self.assertEqual(call("run", self.write())[0], 0)

        for selection in (("--product", "final"), ("--stage", "finish")):
            with self.subTest(selection=selection):
                code, out, err = call("run", str(self.dag), *selection, "--force")
                self.assertEqual(code, 0, err)
                self.assertIn("1 job can start, 1 job is current", out)

    def test_a_spitdag_is_given_alone(self):
        code, _, err = call("plan", self.write(), str(self.dag))
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

    def test_a_job_needing_more_cores_than_the_run_has_runs_alone_with_a_note(self):
        self.data["jobs"][0]["cores"] = 4
        code, out, err = call("run", self.write(), "-j", "2")
        self.assertEqual(code, 0, err)
        self.assertIn("[1] needs 4 cores where the run has 2; it will run alone", out)
        self.assertEqual((self.root / "final.txt").read_text(), "HELLO!")

    def test_a_job_declaring_more_memory_than_the_run_has_is_refused(self):
        self.data["jobs"][1]["memory"] = 64 * 2**20
        code, out, err = call("run", self.write(), "--memory", "32M")
        self.assertEqual(code, 2)
        self.assertIn("[2] op2 needs 64M of memory where the run has 32M", err)
        self.assertIn("pass --no-enforce-memory to run it alone", err)
        self.assertNotIn("Running", out)
        self.assertFalse((self.root / "work/upper.txt").exists())
        # Without enforcement it runs alone, as a job needing more cores does.
        code, out, err = call("run", self.write(), "--memory", "32M", "--no-enforce-memory")
        self.assertEqual(code, 0, err)
        self.assertIn("[2] needs 64M of memory where the run has 32M; it will run alone", out)
        self.assertEqual((self.root / "final.txt").read_text(), "HELLO!")

    def test_memory_takes_a_size_with_a_unit(self):
        for bad in ("lots", "1.5G", "0", "-2G"):
            with self.subTest(bad=bad), contextlib.redirect_stderr(io.StringIO()) as err, \
                    self.assertRaises(SystemExit):
                main(["run", self.write(), f"--memory={bad}"])
            self.assertIn("expected a size such as 512M or 32G", err.getvalue())
        code, _, err = call("run", self.write(), "--memory", "64m")
        self.assertEqual(code, 0, err)


if __name__ == "__main__":
    unittest.main()
