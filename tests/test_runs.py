"""A run's folder: what it printed, its plan, its jobs' logs and the outputs it replaced; and the commands that list
logs, prune runs and restore what a run replaced."""

import json
import tempfile
import unittest
from pathlib import Path

from spit_bash import runs
from support import placed, sample, state_at
from test_cli import call


class RunFolderTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source.txt").write_text("hello")
        self.dag = self.root / "jobs.spitdag"
        self.dag.write_text(json.dumps(placed(sample(self.root))))
        self.runs = self.root / ".spit-bash/runs"

    def run_once(self, *flags):
        code, out, err = call("run", str(self.dag), *flags)
        self.assertEqual(code, 0, err)
        return runs.history(self.runs)[0], out, err

    def test_a_run_keeps_what_it_printed_its_plan_its_logs_and_how_it_ended(self):
        run, out, err = self.run_once()
        log = run.log.read_text()
        self.assertIn(out, log)
        self.assertIn("2 done, 0 checked, 0 failed, 0 current, 0 blocked", log)
        plan = json.loads(run.plan.read_text())
        self.assertEqual([job["status"] for job in plan["jobs"]], ["run", "run"])
        self.assertEqual(json.loads(run.summary.read_text())["ended"], "finished")
        self.assertEqual(json.loads(run.summary.read_text())["done"], 2)
        self.assertTrue((run.folder / "jobs/work/upper.txt.log").is_file())
        self.assertTrue((run.folder / "jobs/final.txt.log").is_file())

    def test_each_record_names_the_run_that_made_its_outputs(self):
        first, _, _ = self.run_once()
        (self.root / "final.txt").unlink()
        second, _, _ = self.run_once()
        records = state_at(self.root).jobs
        self.assertEqual(sorted(record.run for record in records.values()), sorted([first.name, second.name]))
        # A rerun writes a log of its own; the first run's stays.
        self.assertTrue(first.job_log('[["final.txt",[]]]').is_file())
        self.assertTrue(second.job_log('[["final.txt",[]]]').is_file())
        code, out, _ = call("explain", "2", str(self.dag))
        self.assertIn(f"made by run {second.name}; log at {second.folder}/jobs/final.txt.log", out)

    def test_two_runs_in_one_second_get_folders_of_their_own(self):
        first = runs.start(self.runs, "2026-11-12T10-00-00")
        second = runs.start(self.runs, "2026-11-12T10-00-00")
        tenth = [runs.start(self.runs, "2026-11-12T10-00-00") for _ in range(8)][-1]
        self.assertEqual((first.name, second.name, tenth.name),
                         ("2026-11-12T10-00-00", "2026-11-12T10-00-00-2", "2026-11-12T10-00-00-10"))
        self.assertEqual(runs.history(self.runs)[0], tenth)

    def test_log_lists_a_jobs_logs_newest_first_and_marks_the_one_behind_its_outputs(self):
        first, _, _ = self.run_once()
        (self.root / "final.txt").unlink()
        second, _, _ = self.run_once()
        code, out, _ = call("log", str(self.dag), "--product", "final.txt")
        self.assertEqual(code, 0)
        self.assertEqual(out.splitlines(), [
            "[2] op2",
            f"  {second.name}  {second.folder}/jobs/final.txt.log  (made its current outputs)",
            f"  {first.name}  {first.folder}/jobs/final.txt.log",
        ])

    def test_a_rerun_keeps_the_outputs_it_replaced_and_restore_puts_them_back(self):
        self.run_once()
        (self.root / "source.txt").write_text("changed")
        run, _, err = self.run_once()
        self.assertEqual((self.root / "final.txt").read_text(), "CHANGED!")
        self.assertIn(f"kept the 2 outputs that 2 jobs replaced (11B) in {run.folder}/replaced; "
                      f"`spit-bash restore --run {run.name}` puts them back", err)
        self.assertEqual((run.folder / "replaced/final.txt/files/final.txt").read_text(), "HELLO!")

        code, _, err = call("restore", str(self.dag), "--run", run.name, "--product", "final.txt")
        self.assertEqual(code, 0, err)
        self.assertEqual((self.root / "final.txt").read_text(), "HELLO!")
        undo = runs.history(self.runs)[0]
        self.assertEqual(err.splitlines()[-2:], [
            "restored 1 output of 1 job, with the records they had",
            f"kept the 1 output they replaced in {undo.folder}/replaced; "
            f"`spit-bash restore --run {undo.name}` puts it back",
        ])
        # The restored record describes the restored file, so the job reruns only because its input changed.
        code, out, _ = call("plan", str(self.dag), "--verbose")
        self.assertIn("op2: input changed", out)

        # The restore is undone the same way.
        call("restore", str(self.dag), "--run", undo.name)
        self.assertEqual((self.root / "final.txt").read_text(), "CHANGED!")
        code, out, _ = call("plan", str(self.dag))
        self.assertIn("2 jobs are current", out)
        # What was restored is restored once.
        code, _, err = call("restore", str(self.dag), "--run", run.name, "--product", "final.txt")
        self.assertIn("nothing to restore", err)

    def test_a_run_told_not_to_keep_what_it_replaces_deletes_it(self):
        self.run_once()
        (self.root / "source.txt").write_text("changed")
        run, _, err = self.run_once("--no-keep-replaced")
        self.assertNotIn("kept the", err)
        self.assertFalse((run.folder / "replaced").exists())

    def test_prune_keeps_the_newest_runs_and_the_logs_behind_current_outputs(self):
        first, _, _ = self.run_once()
        (self.root / "final.txt").unlink()
        second, _, _ = self.run_once()
        (self.root / "final.txt").unlink()
        third, _, _ = self.run_once()
        code, _, err = call("prune", str(self.dag), "--keep-runs", "1")
        self.assertEqual(code, 0, err)
        self.assertEqual(err.splitlines()[-1], "removed 2 run folders, keeping 1 log of current outputs")
        self.assertEqual([run.name for run in runs.history(self.runs)], [third.name, first.name])
        # The first run made work/upper.txt, which is still current; the rest of its folder is gone.
        self.assertEqual(sorted(path.relative_to(first.folder).as_posix() for path in first.folder.rglob("*")
                                if path.is_file()), ["jobs/work/upper.txt.log"])
        self.assertTrue(third.log.is_file())
        self.assertFalse(second.folder.exists())

    def test_a_log_name_comes_from_the_first_output_and_its_entities(self):
        self.assertEqual(runs.log_name('[["clean",[["voice","theo"],["digit","7"]]],["meta",[["voice","theo"]]]]'),
                         "clean/voice=theo_digit=7.log")
        self.assertEqual(runs.log_name('[["summary",[]]]'), "summary.log")


if __name__ == "__main__":
    unittest.main()
