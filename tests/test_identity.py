"""A job's fingerprint and state key follow what it makes and runs, not where its files are."""

import contextlib
import copy
import io
import json
import os
import shutil
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from spit_bash import cli, migrate
from spit_bash.migrate import Migration
from spit_bash.plan import Status, job_key, plan
from support import read, run, sample, state_at


def moved(node, rename):
    """`node` with every artifact path and command path part renamed, nothing else changed."""
    if isinstance(node, dict):
        return {k: (rename(v) if k in ("path", "of") and isinstance(v, str) else moved(v, rename))
                for k, v in node.items()}
    if isinstance(node, list):
        return [moved(item, rename) for item in node]
    return node


class IdentityTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.data = sample(self.root)

    def test_changing_only_storage_paths_keeps_fingerprints_and_keys(self):
        before = read(self.data).jobs
        after = read(moved(copy.deepcopy(self.data), lambda path: "derivatives/" + path)).jobs
        self.assertEqual([j.fingerprint for j in before], [j.fingerprint for j in after])
        self.assertEqual([job_key(j) for j in before], [job_key(j) for j in after])
        self.assertNotEqual(before[0].outputs, after[0].outputs)

    def test_a_changed_command_or_artifact_identity_changes_the_fingerprint(self):
        base = read(self.data).jobs[0].fingerprint
        changed = copy.deepcopy(self.data)
        changed["jobs"][0]["commands"][0]["command"][2] = ["print('other')"]
        self.assertNotEqual(base, read(changed).jobs[0].fingerprint)
        renamed = copy.deepcopy(self.data)
        renamed["jobs"][0]["inputs"]["input"][0]["entities"] = {"sub": "01"}
        self.assertNotEqual(base, read(renamed).jobs[0].fingerprint)

    def test_scheduling_checks_and_origin_do_not_change_the_fingerprint(self):
        base = read(self.data).jobs[0].fingerprint
        other = copy.deepcopy(self.data)
        other["jobs"][0].update(cores=4, memory=1024, id=1, stage=["s"])
        self.assertEqual(base, read(other).jobs[0].fingerprint)


class ReuseAfterPathChangeTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        (self.root / "source.txt").write_text("hello")
        self.data = sample(self.root)
        run(read(self.data), state_at(self.root))
        self.relocated = moved(copy.deepcopy(self.data), lambda p: p if p == "source.txt" else "derivatives/" + p)

    def copy_outputs(self):
        for name in ("work/upper.txt", "final.txt"):
            target = self.root / "derivatives" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(self.root / name, target)

    def statuses(self):
        return [d.status for d in plan(read(self.relocated), state_at(self.root))]

    def test_identical_files_at_the_new_paths_are_reused(self):
        self.copy_outputs()
        self.assertEqual(self.statuses(), [Status.SKIP, Status.SKIP])

    def test_missing_files_at_the_new_paths_still_run(self):
        self.assertEqual(self.statuses()[0], Status.RUN)

    def test_files_that_differ_at_the_new_paths_run(self):
        self.copy_outputs()
        (self.root / "derivatives" / "final.txt").write_text("different")
        self.assertEqual(self.statuses(), [Status.SKIP, Status.RUN])

    def test_a_changed_input_still_runs_after_a_path_change(self):
        self.copy_outputs()
        (self.root / "source.txt").write_text("changed")
        self.assertEqual(self.statuses()[0], Status.RUN)


class MigrateTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        (self.root / "source.txt").write_text("hello")
        self.data = sample(self.root)
        run(read(self.data), state_at(self.root))
        self.relocated = moved(copy.deepcopy(self.data), lambda p: p if p == "source.txt" else "derivatives/" + p)
        self.dag = read(self.relocated)
        self.state = state_at(self.root)

    def find(self):
        return {m.job.id: m for m in migrate.find(self.dag, self.state)}

    def test_moves_a_chain_in_one_pass_and_the_jobs_are_then_current(self):
        found = self.find()
        self.assertEqual({i: m.reason for i, m in found.items()}, {1: "outputs recorded at other paths"} | {
            2: "outputs recorded at other paths"})
        done = migrate.apply(self.dag, self.state, tuple(found.values()))
        self.assertTrue(all(m.moves for m in done))
        self.assertEqual((self.root / "derivatives/final.txt").read_text(), "HELLO!")
        self.assertFalse((self.root / "final.txt").exists())
        self.assertFalse((self.root / "work/upper.txt").exists())
        self.assertEqual([d.status for d in plan(self.dag, state_at(self.root))], [Status.SKIP, Status.SKIP])

    def test_reverting_the_path_rule_moves_the_files_back(self):
        migrate.apply(self.dag, self.state, tuple(self.find().values()))
        self.state.close()
        original = read(self.data)
        back = migrate.find(original, state_at(self.root), {1, 2})
        self.assertEqual([bool(m.moves) for m in back], [True, True])
        migrate.apply(original, state_at(self.root), back)
        self.assertEqual((self.root / "final.txt").read_text(), "HELLO!")
        self.assertEqual([d.status for d in plan(original, state_at(self.root))], [Status.SKIP, Status.SKIP])

    def test_an_existing_destination_is_never_overwritten(self):
        target = self.root / "derivatives/work/upper.txt"
        target.parent.mkdir(parents=True)
        target.write_text("someone else's")
        found = self.find()
        self.assertEqual(found[1].moves, ())
        self.assertIn("already exists", found[1].reason)
        migrate.apply(self.dag, self.state, tuple(found.values()))
        self.assertEqual(target.read_text(), "someone else's")
        self.assertTrue((self.root / "work/upper.txt").exists())

    def test_an_output_changed_since_the_run_is_not_moved(self):
        (self.root / "final.txt").write_text("edited by hand")
        found = self.find()
        self.assertEqual(found[2].moves, ())
        self.assertIn("missing or changed", found[2].reason)

    def test_a_job_whose_input_changed_is_not_moved(self):
        (self.root / "source.txt").write_text("changed")
        self.assertIn("input", self.find()[1].reason)
        self.assertEqual(self.find()[1].moves, ())

    def test_a_downstream_job_is_not_moved_when_its_producer_is_not(self):
        (self.root / "derivatives/work").mkdir(parents=True)
        (self.root / "derivatives/work/upper.txt").write_text("x")
        self.assertEqual(self.find()[2].moves, ())

    def test_an_old_path_another_job_still_uses_is_left_alone(self):
        data = copy.deepcopy(self.relocated)
        # Job 2 now reads and writes under the paths job 1's record left its output at.
        data["jobs"][1]["outputs"]["output"]["path"] = "work/upper.txt"
        self.dag = read(data)
        self.assertIn("still uses", self.find()[1].reason)

    def test_a_move_that_fails_part_way_puts_the_job_back(self):
        real, calls = os.rename, []

        def flaky(source, target):
            calls.append(source)
            if len(calls) == 2:
                raise OSError("disk says no")
            real(source, target)

        found = {m.job.id: m for m in migrate.find(self.dag, self.state)}
        two = Migration(found[1].job, found[1].moves + found[2].moves, found[1].reason)
        with unittest.mock.patch("os.rename", flaky):
            done = migrate.apply(self.dag, self.state, (two,))
        self.assertEqual(done[0].moves, ())
        self.assertIn("disk says no", done[0].reason)
        self.assertTrue((self.root / "work/upper.txt").exists())
        self.assertTrue((self.root / "final.txt").exists())
        self.assertFalse((self.root / "derivatives/work/upper.txt").exists())

    def test_nothing_to_move_when_paths_are_unchanged(self):
        self.assertEqual(migrate.find(read(self.data), self.state), ())


class CommandLineTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        (self.root / "source.txt").write_text("hello")
        data = sample(self.root)
        run(read(data), state_at(self.root))
        self.before = (self.root / "final.txt").stat().st_mtime_ns
        relocated = moved(data, lambda p: p if p == "source.txt" else "derivatives/" + p)
        self.file = self.root / "moved.spitdag"
        self.file.write_text(json.dumps(relocated))

    def main(self, action, *flags):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main([action, str(self.file), "--root", str(self.root), *flags])
        return code, out.getvalue(), err.getvalue()

    def test_plan_lists_the_moves_and_changes_nothing(self):
        code, out, _ = self.main("plan")
        self.assertIn("would move final.txt -> derivatives/final.txt", out)
        self.assertTrue((self.root / "final.txt").exists())
        self.assertFalse((self.root / "derivatives").exists())

    def test_run_moves_the_files_and_executes_no_job(self):
        code, out, err = self.main("run")
        self.assertEqual(code, 0, err)
        self.assertIn("moved final.txt -> derivatives/final.txt", out)
        self.assertIn("a tool that wrote those paths", err)
        self.assertFalse((self.root / "final.txt").exists())
        self.assertEqual((self.root / "derivatives/final.txt").stat().st_mtime_ns, self.before)
        self.assertNotIn("start op", out)
        code, out, _ = self.main("plan")
        self.assertNotIn("would move", out)
        self.assertNotIn("run", [line.split()[1] for line in out.splitlines() if len(line.split()) > 1])

    def test_no_migrate_runs_the_jobs_instead(self):
        code, out, err = self.main("run", "--no-migrate")
        self.assertEqual(code, 0, err)
        self.assertNotIn("moved", out)
        self.assertTrue((self.root / "final.txt").exists())
        self.assertEqual((self.root / "derivatives/final.txt").read_text(), "HELLO!")


if __name__ == "__main__":
    unittest.main()
