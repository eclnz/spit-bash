"""A job's fingerprint and state key follow what it makes and runs, not where its files are."""

import contextlib
import errno
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
from support import command, job, document, read, run, sample, state_at


def artifact_of(path):
    return {"product": path, "entities": {}, "type": None, "path": path, "kind": "file"}


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

    def test_a_changed_command_changes_the_fingerprint_but_a_renamed_artifact_does_not(self):
        base = read(self.data).jobs[0].fingerprint
        changed = copy.deepcopy(self.data)
        changed["jobs"][0]["commands"][0]["command"][2] = ["print('other')"]
        self.assertNotEqual(base, read(changed).jobs[0].fingerprint)
        renamed = copy.deepcopy(self.data)
        renamed["jobs"][0]["inputs"]["input"][0].update(product="other", entities={"sub": "01"})
        self.assertEqual(base, read(renamed).jobs[0].fingerprint)

    def test_renaming_an_operation_or_a_port_keeps_the_fingerprint(self):
        base = read(self.data).jobs[0].fingerprint
        other = copy.deepcopy(self.data)
        job_data = other["jobs"][0]
        job_data["operation"] = "renamed"
        job_data["inputs"] = {"renamed_port": v for v in job_data["inputs"].values()}
        job_data["outputs"] = {"renamed_out": v for v in job_data["outputs"].values()}
        self.assertEqual(base, read(other).jobs[0].fingerprint)

    def test_moving_a_checked_file_keeps_the_checks_key(self):
        data = copy.deepcopy(self.data)
        item = data["jobs"][0]
        target = next(iter(item["outputs"].values()))["path"]
        item["checks"] = [{"when": "after", "check": "nonempty", "port": next(iter(item["outputs"])), "path": target,
                           "command": [["test", "-s", {"path": target}]]}]
        before = read(data).jobs[0].checks_key
        after = read(moved(copy.deepcopy(data), lambda path: "derivatives/" + path)).jobs[0].checks_key
        self.assertIsNotNone(before)
        self.assertEqual(before, after)

    def test_a_changed_job_names_the_part_that_changed(self):
        (self.root / "source.txt").write_text("hello")
        run(read(self.data), state_at(self.root))
        for edit, reason in [
            (lambda item: item["commands"][0]["command"].append(["x"]), "command changed"),
            (lambda item: item["inputs"].update(again=item["inputs"]["input"]), "the artifacts it reads changed"),
        ]:
            changed = copy.deepcopy(self.data)
            edit(changed["jobs"][0])
            decision = plan(read(changed), state_at(self.root))[0]
            self.assertIn(reason, decision.reason)

    def test_a_record_without_parts_still_reports_a_changed_fingerprint(self):
        (self.root / "source.txt").write_text("hello")
        run(read(self.data), state_at(self.root))
        state = state_at(self.root)
        for key, record in list(state.jobs.items()):
            state.jobs[key] = record.model_copy(update={"parts": None})
        changed = copy.deepcopy(self.data)
        changed["jobs"][0]["commands"][0]["command"].append(["x"])
        self.assertEqual(plan(read(changed), state)[0].reason, "job fingerprint changed")

    def test_a_stranded_output_is_named_when_the_path_rules_change(self):
        (self.root / "source.txt").write_text("hello")
        run(read(self.data), state_at(self.root))
        after = read(moved(copy.deepcopy(self.data), lambda path: path if path == "source.txt" else "derivatives/" + path))
        reason = plan(after, state_at(self.root))[0].reason
        self.assertEqual(reason, "output missing; an earlier run left it at work/upper.txt")

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

    def test_a_move_across_filesystems_is_not_made_and_the_job_stays_put(self):
        def cross(source, target):
            raise OSError(errno.EXDEV, "Invalid cross-device link")

        with unittest.mock.patch("os.rename", cross):
            done = migrate.apply(self.dag, self.state, tuple(self.find().values()))
        self.assertTrue(all(m.moves == () and "not moved" in m.reason for m in done))
        self.assertTrue((self.root / "final.txt").exists() and (self.root / "work/upper.txt").exists())

    def test_nothing_to_move_when_paths_are_unchanged(self):
        self.assertEqual(migrate.find(read(self.data), self.state), ())


class OutputShapeTests(unittest.TestCase):
    """Jobs with several outputs, and with a folder as an output."""

    TWO = ("from pathlib import Path; import sys; t = Path(sys.argv[1]).read_text();"
           " Path(sys.argv[2]).write_text(t + 'a'); Path(sys.argv[3]).write_text(t + 'b')")
    FOLDER = ("from pathlib import Path; import sys; d = Path(sys.argv[2]);"
              " (d / 'one.txt').write_text(Path(sys.argv[1]).read_text())")

    def build(self, script, outputs, kind):
        self.root = Path(tempfile.mkdtemp())
        (self.root / "source.txt").write_text("hello")
        item = job(1, ["source.txt"], outputs[0], script)
        item["outputs"] = {f"o{i}": dict(artifact_of(path), kind=kind) for i, path in enumerate(outputs)}
        item["commands"][0]["command"] = command(script, "source.txt", *outputs)
        data = document(self.root, [item], ["source.txt"])
        run(read(data), state_at(self.root))
        self.data = data
        relocated = moved(copy.deepcopy(data), lambda p: p if p == "source.txt" else "new/" + p)
        self.dag = read(relocated)
        self.state = state_at(self.root)

    def test_every_output_of_a_job_moves_together(self):
        self.build(self.TWO, ["out/a.txt", "out/b.txt"], "file")
        found = migrate.find(self.dag, self.state)
        self.assertEqual(len(found[0].moves), 2)
        migrate.apply(self.dag, self.state, found)
        self.assertEqual((self.root / "new/out/a.txt").read_text(), "helloa")
        self.assertEqual((self.root / "new/out/b.txt").read_text(), "hellob")
        self.assertFalse((self.root / "out/a.txt").exists() or (self.root / "out/b.txt").exists())
        self.assertEqual([d.status for d in plan(self.dag, state_at(self.root))], [Status.SKIP])

    def test_one_blocked_output_keeps_all_of_them_where_they_are(self):
        self.build(self.TWO, ["out/a.txt", "out/b.txt"], "file")
        (self.root / "new/out").mkdir(parents=True)
        (self.root / "new/out/b.txt").write_text("not ours")
        found = migrate.find(self.dag, self.state)
        self.assertEqual(found[0].moves, ())
        migrate.apply(self.dag, self.state, found)
        self.assertTrue((self.root / "out/a.txt").exists() and (self.root / "out/b.txt").exists())
        self.assertFalse((self.root / "new/out/a.txt").exists())

    def test_a_folder_output_moves_whole(self):
        self.build(self.FOLDER, ["out/dir"], "folder")
        found = migrate.find(self.dag, self.state)
        self.assertEqual(found[0].moves, (("out/dir", "new/out/dir"),))
        migrate.apply(self.dag, self.state, found)
        self.assertEqual((self.root / "new/out/dir/one.txt").read_text(), "hello")
        self.assertFalse((self.root / "out/dir").exists())
        self.assertEqual([d.status for d in plan(self.dag, state_at(self.root))], [Status.SKIP])

    def test_a_folder_changed_since_the_run_is_not_moved(self):
        self.build(self.FOLDER, ["out/dir"], "folder")
        (self.root / "out/dir/extra.txt").write_text("added later")
        found = migrate.find(self.dag, self.state)
        self.assertEqual(found[0].moves, ())
        self.assertIn("missing or changed", found[0].reason)


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
        code, out, _ = self.main("plan", "--move-files", "--verbose")
        self.assertIn("would move final.txt -> derivatives/final.txt", out)
        self.assertTrue((self.root / "final.txt").exists())
        self.assertFalse((self.root / "derivatives").exists())
        # The listing judges the jobs as if the files were already moved.
        self.assertIn("all 2 jobs are current", out)
        self.assertIn("2 job(s) reuse outputs", _)

    def test_run_moves_the_files_and_executes_no_job(self):
        code, out, err = self.main("run", "--move-files")
        self.assertEqual(code, 0, err)
        self.assertIn("moved final.txt -> derivatives/final.txt", out)
        self.assertIn("a tool that wrote those paths", err)
        self.assertFalse((self.root / "final.txt").exists())
        self.assertEqual((self.root / "derivatives/final.txt").stat().st_mtime_ns, self.before)
        self.assertNotIn("start op", out)
        code, out, _ = self.main("plan", "--move-files")
        self.assertNotIn("would move", out)
        self.assertNotIn("run", [line.split()[1] for line in out.splitlines() if len(line.split()) > 1])

    def test_without_migrate_the_jobs_run_and_nothing_is_moved(self):
        code, out, err = self.main("run")
        self.assertEqual(code, 0, err)
        self.assertNotIn("moved", out)
        self.assertTrue((self.root / "final.txt").exists())
        self.assertEqual((self.root / "derivatives/final.txt").read_text(), "HELLO!")


if __name__ == "__main__":
    unittest.main()
