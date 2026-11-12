"""A job's fingerprint and state key follow what it makes and runs, not where its files are."""

import asyncio
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

from spit_bash import cli
from spit_bash.cause import Cause, Why
from spit_bash.execute.failure import DependencyFailed, PlaceFailed
from spit_bash.execute.job import place_job
from spit_bash.execute.options import Options
from spit_bash.execute.scheduler import execute
from spit_bash.plan import Status, explained, plan
from spit_bash.report import causes
from spit_bash.report.explain import explain
from spit_bash.report.run_text import failure_words as error_words
from spit_bash.reuse.evidence import Compare
from spit_bash.reuse.relocate import MoveCause
from support import EXCLAIM, command, error, job_entry, document, read, run, sample, state_at, placed, job_log, new_run


def artifact_of(path):
    return {"product": path, "entities": {}, "type": None, "path": path, "under": "root", "kind": "file"}


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
        self.assertEqual([j.work.fingerprint for j in before], [j.work.fingerprint for j in after])
        self.assertEqual([j.work.key for j in before], [j.work.key for j in after])
        self.assertNotEqual(before[0].output_paths, after[0].output_paths)

    def test_a_changed_command_changes_the_fingerprint_but_a_renamed_artifact_does_not(self):
        base = read(self.data).jobs[0].work.fingerprint
        changed = copy.deepcopy(self.data)
        changed["jobs"][0]["commands"][0]["command"][2] = ["print('other')"]
        self.assertNotEqual(base, read(changed).jobs[0].work.fingerprint)
        renamed = copy.deepcopy(self.data)
        renamed["jobs"][0]["inputs"]["input"][0].update(product="other", entities={"sub": "01"})
        self.assertEqual(base, read(renamed).jobs[0].work.fingerprint)

    def test_renaming_an_operation_or_a_port_keeps_the_fingerprint(self):
        base = read(self.data).jobs[0].work.fingerprint
        other = copy.deepcopy(self.data)
        job_data = other["jobs"][0]
        job_data["operation"] = "renamed"
        job_data["inputs"] = {"renamed_port": v for v in job_data["inputs"].values()}
        job_data["outputs"] = {"renamed_out": v for v in job_data["outputs"].values()}
        self.assertEqual(base, read(other).jobs[0].work.fingerprint)

    def test_moving_a_checked_file_keeps_the_checks_key(self):
        data = copy.deepcopy(self.data)
        item = data["jobs"][0]
        target = next(iter(item["outputs"].values()))["path"]
        item["checks"] = [{"when": "after", "check": "nonempty", "port": next(iter(item["outputs"])), "path": target,
                           "command": [["test", "-s", {"path": target}]]}]
        before = read(data).jobs[0].work.checks_key
        after = read(moved(copy.deepcopy(data), lambda path: "derivatives/" + path)).jobs[0].work.checks_key
        self.assertIsNotNone(before)
        self.assertEqual(before, after)

    def test_a_changed_job_names_the_part_that_changed(self):
        (self.root / "source.txt").write_text("hello")
        run(read(self.data), state_at(self.root))
        for edit, part, words in [
            (lambda item: item["commands"][0]["command"].append(["x"]), "commands", "command changed"),
            (lambda item: item["inputs"].update(again=item["inputs"]["input"]), "inputs", "the artifacts it reads changed"),
        ]:
            changed = copy.deepcopy(self.data)
            edit(changed["jobs"][0])
            decision = plan(read(changed), state_at(self.root))[0]
            self.assertEqual(decision.cause, Cause(Why.WORK_CHANGED, parts=(part,)))
            self.assertEqual(causes.cause(decision.cause), words)

    def test_a_record_without_parts_still_reports_a_changed_fingerprint(self):
        (self.root / "source.txt").write_text("hello")
        run(read(self.data), state_at(self.root))
        state = state_at(self.root)
        for key, record in list(state.jobs.items()):
            state.jobs[key] = record.model_copy(update={"parts": None})
        changed = copy.deepcopy(self.data)
        changed["jobs"][0]["commands"][0]["command"].append(["x"])
        decision = plan(read(changed), state)[0]
        self.assertEqual(decision.cause, Cause(Why.WORK_CHANGED, parts=None))
        self.assertEqual(causes.cause(decision.cause), "job fingerprint changed")

    def test_a_stranded_output_is_named_when_the_path_rules_change(self):
        (self.root / "source.txt").write_text("hello")
        run(read(self.data), state_at(self.root))
        after = read(moved(copy.deepcopy(self.data), lambda path: path if path == "source.txt" else "derivatives/" + path))
        cause = plan(after, state_at(self.root))[0].cause
        self.assertEqual(cause, Cause(Why.OUTPUT_MISSING, ("work/upper.txt",)))

    def test_scheduling_checks_and_origin_do_not_change_the_fingerprint(self):
        base = read(self.data).jobs[0].work.fingerprint
        other = copy.deepcopy(self.data)
        other["jobs"][0].update(cores=4, memory=1024, id=1, stage=["s"])
        self.assertEqual(base, read(other).jobs[0].work.fingerprint)


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


class PlaceTests(unittest.TestCase):
    """`--move-files` as the planner decides it, and as a run carries it out."""

    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        (self.root / "source.txt").write_text("hello")
        self.data = sample(self.root)
        run(read(self.data), state_at(self.root))
        self.relocated = moved(copy.deepcopy(self.data), lambda p: p if p == "source.txt" else "derivatives/" + p)
        self.dag = read(self.relocated)

    def decisions(self, dag=None):
        return {d.job.id: d for d in plan(dag or self.dag, state_at(self.root), move_files=True)}

    def place(self, dag=None):
        return run(dag or self.dag, state_at(self.root), move_files=True)

    def test_a_chain_is_planned_place_in_one_plan(self):
        found = self.decisions()
        self.assertEqual({i: d.status for i, d in found.items()}, {1: Status.PLACE, 2: Status.PLACE})
        self.assertEqual(found[2].placement.moves, (("final.txt", "derivatives/final.txt"),))
        # Neither waits for the other: a placed job's outputs arrive as recorded.
        self.assertEqual(found[2].waits_for, ())
        self.assertTrue((self.root / "final.txt").exists())
        self.assertFalse((self.root / "derivatives").exists())

    def test_without_move_files_the_jobs_run(self):
        self.assertEqual([d.status for d in plan(self.dag, state_at(self.root))], [Status.RUN, Status.RUN])

    def test_a_run_moves_a_chain_and_the_jobs_are_then_current(self):
        _, results = self.place()
        self.assertTrue(all(result.failure is None for result in results))
        self.assertEqual((self.root / "derivatives/final.txt").read_text(), "HELLO!")
        self.assertFalse((self.root / "final.txt").exists())
        self.assertFalse((self.root / "work/upper.txt").exists())
        self.assertEqual([d.status for d in plan(self.dag, state_at(self.root))], [Status.SKIP, Status.SKIP])

    def test_a_job_after_a_placed_one_waits_for_its_files(self):
        data = copy.deepcopy(self.relocated)
        # Job 2's command changed, so it runs on what job 1 moves.
        data["jobs"][1]["commands"][0]["command"][2] = [EXCLAIM.replace("'!'", "'?'")]
        dag = read(data)
        found = self.decisions(dag)
        self.assertEqual((found[1].status, found[2].status), (Status.PLACE, Status.RUN))
        self.assertEqual(found[2].waits_for, ())
        _, results = self.place(dag)
        self.assertTrue(all(result.failure is None for result in results), [error(r) for r in results])
        self.assertEqual((self.root / "derivatives/final.txt").read_text(), "HELLO?")

    def test_reverting_the_path_rule_moves_the_files_back(self):
        self.place()
        original = read(self.data)
        self.assertEqual([d.status for d in self.decisions(original).values()], [Status.PLACE, Status.PLACE])
        self.place(original)
        self.assertEqual((self.root / "final.txt").read_text(), "HELLO!")
        self.assertEqual([d.status for d in plan(original, state_at(self.root))], [Status.SKIP, Status.SKIP])

    def test_an_existing_destination_is_never_overwritten(self):
        target = self.root / "derivatives/work/upper.txt"
        target.parent.mkdir(parents=True)
        target.write_text("someone else's")
        found = self.decisions()
        self.assertEqual(found[1].status, Status.RUN)
        self.assertEqual((found[1].placement.cause, found[1].placement.detail),
                         (MoveCause.DESTINATION_EXISTS, "derivatives/work/upper.txt"))
        self.assertIn("not moved: derivatives/work/upper.txt already exists", causes.decided(found[1]))

    def test_a_destination_that_appears_after_the_plan_fails_the_placement(self):
        decisions = plan(self.dag, state_at(self.root), move_files=True)
        target = self.root / "derivatives/work/upper.txt"
        target.parent.mkdir(parents=True)
        target.write_text("someone else's")
        state = state_at(self.root)
        results = asyncio.run(execute(self.dag, decisions, state, Options(new_run(self.root / ".spit-bash/runs"))))
        state.close()
        self.assertEqual(results[0].failure, PlaceFailed("derivatives/work/upper.txt already exists", True))
        self.assertEqual(results[1].failure, DependencyFailed(1))
        self.assertEqual(target.read_text(), "someone else's")
        self.assertTrue((self.root / "work/upper.txt").exists() and (self.root / "final.txt").exists())

    def test_an_output_changed_since_the_run_is_not_moved(self):
        (self.root / "final.txt").write_text("edited by hand")
        placement = self.decisions()[2].placement
        self.assertEqual((placement.cause, placement.moves, placement.detail), (MoveCause.OUTPUT_CHANGED, (), "final.txt"))

    def test_a_job_whose_input_changed_is_not_moved(self):
        (self.root / "source.txt").write_text("changed")
        found = self.decisions()
        self.assertEqual((found[1].status, found[1].placement.cause), (Status.RUN, MoveCause.INPUT_CHANGED))

    def test_a_downstream_job_is_not_moved_when_its_producer_is_not(self):
        (self.root / "derivatives/work").mkdir(parents=True)
        (self.root / "derivatives/work/upper.txt").write_text("x")
        found = self.decisions()
        self.assertEqual((found[1].status, found[2].status), (Status.RUN, Status.RUN))
        self.assertIsNone(found[2].placement)

    def test_an_old_path_another_job_still_uses_is_left_alone(self):
        data = copy.deepcopy(self.relocated)
        # Job 2 now reads and writes under the paths job 1's record left its output at.
        data["jobs"][1]["outputs"]["output"]["path"] = "work/upper.txt"
        self.assertEqual(self.decisions(read(data))[1].placement.cause, MoveCause.STILL_USED)

    def test_a_placed_job_whose_checks_changed_runs_them_once_moved(self):
        data = copy.deepcopy(self.relocated)
        data["jobs"][0]["checks"] = [{
            "when": "after", "check": "upper", "port": "output", "path": "derivatives/work/upper.txt",
            "command": command("from pathlib import Path; import sys; sys.exit(Path(sys.argv[1]).read_text() != 'HELLO')",
                               "derivatives/work/upper.txt"),
        }]
        dag = read(data)
        found = self.decisions(dag)
        self.assertEqual((found[1].status, found[1].cause), (Status.PLACE, Cause(Why.CHECKS_NOT_RUN)))
        self.assertTrue(found[1].needs_checks)
        _, results = self.place(dag)
        self.assertTrue(all(result.failure is None for result in results), [error(r) for r in results])
        self.assertEqual([d.status for d in plan(dag, state_at(self.root))], [Status.SKIP, Status.SKIP])

    def test_a_forced_job_is_not_placed(self):
        decisions = plan(self.dag, state_at(self.root), True, move_files=True)
        self.assertEqual([d.status for d in decisions], [Status.RUN, Status.RUN])

    def test_a_move_across_filesystems_fails_and_the_job_stays_put(self):
        def cross(source, target):
            raise OSError(errno.EXDEV, "Invalid cross-device link")

        with unittest.mock.patch("os.rename", cross):
            _, results = self.place()
        self.assertIsInstance(results[0].failure, PlaceFailed)
        self.assertTrue(results[0].failure.put_back)
        self.assertEqual(results[1].failure, DependencyFailed(1))
        self.assertTrue((self.root / "final.txt").exists() and (self.root / "work/upper.txt").exists())
        # Nothing was recorded at the new paths, so the next plan finds the files where they were.
        self.assertEqual([d.status for d in self.decisions().values()], [Status.PLACE, Status.PLACE])

    def test_a_placement_is_recorded_as_soon_as_its_files_move(self):
        decisions = plan(self.dag, state_at(self.root), move_files=True)
        place_job(self.dag, decisions[0].placement)
        state = state_at(self.root)
        state.apply(decisions[0].writes)
        state.close()
        self.assertEqual(self.decisions()[1].status, Status.SKIP)

    def test_nothing_to_move_when_paths_are_unchanged(self):
        self.assertEqual([d.placement for d in self.decisions(read(self.data)).values()], [None, None])


class PlanJobTests(unittest.TestCase):
    def test_an_input_another_job_moves_is_shown_as_it_will_be(self):
        root = Path(tempfile.mkdtemp())
        (root / "source.txt").write_text("hello")
        data = sample(root)
        run(read(data), state_at(root))
        dag = read(moved(data, lambda p: p if p == "source.txt" else "derivatives/" + p))
        state = state_at(root)
        decisions = plan(dag, state, move_files=True)
        decision, found = explained(dag, state, decisions, 2, Compare.CONTENT)
        self.assertEqual((found.inputs, found.outputs), ((), ()))
        lines = explain(dag, decisions, decision, found)
        self.assertIn("  decision: place, moves final.txt -> derivatives/final.txt", lines)


class OutputShapeTests(unittest.TestCase):
    """Jobs with several outputs, and with a folder as an output."""

    TWO = ("from pathlib import Path; import sys; t = Path(sys.argv[1]).read_text();"
           " Path(sys.argv[2]).write_text(t + 'a'); Path(sys.argv[3]).write_text(t + 'b')")
    FOLDER = ("from pathlib import Path; import sys; d = Path(sys.argv[2]);"
              " (d / 'one.txt').write_text(Path(sys.argv[1]).read_text())")

    def build(self, script, outputs, kind):
        self.root = Path(tempfile.mkdtemp())
        (self.root / "source.txt").write_text("hello")
        item = job_entry(1, ["source.txt"], outputs[0], script)
        item["outputs"] = {f"o{i}": dict(artifact_of(path), kind=kind) for i, path in enumerate(outputs)}
        item["commands"][0]["command"] = command(script, "source.txt", *outputs)
        data = document(self.root, [item], ["source.txt"])
        run(read(data), state_at(self.root))
        self.data = data
        relocated = moved(copy.deepcopy(data), lambda p: p if p == "source.txt" else "new/" + p)
        self.dag = read(relocated)

    def decision(self):
        return plan(self.dag, state_at(self.root), move_files=True)[0]

    def test_every_output_of_a_job_moves_together(self):
        self.build(self.TWO, ["out/a.txt", "out/b.txt"], "file")
        self.assertEqual(len(self.decision().placement.moves), 2)
        run(self.dag, state_at(self.root), move_files=True)
        self.assertEqual((self.root / "new/out/a.txt").read_text(), "helloa")
        self.assertEqual((self.root / "new/out/b.txt").read_text(), "hellob")
        self.assertFalse((self.root / "out/a.txt").exists() or (self.root / "out/b.txt").exists())
        self.assertEqual([d.status for d in plan(self.dag, state_at(self.root))], [Status.SKIP])

    def test_a_move_that_fails_part_way_puts_the_job_back(self):
        self.build(self.TWO, ["out/a.txt", "out/b.txt"], "file")
        real, calls = os.rename, []

        def flaky(source, target):
            calls.append(source)
            if len(calls) == 2:
                raise OSError("disk says no")
            real(source, target)

        placement = self.decision().placement
        with unittest.mock.patch("os.rename", flaky):
            failure = place_job(self.dag, placement)
        self.assertEqual(failure, PlaceFailed("disk says no", True))
        self.assertTrue((self.root / "out/a.txt").exists() and (self.root / "out/b.txt").exists())
        self.assertFalse((self.root / "new/out/a.txt").exists())

    def test_a_move_whose_undo_fails_says_so(self):
        self.build(self.TWO, ["out/a.txt", "out/b.txt"], "file")
        real, calls = os.rename, []

        def broken(source, target):
            calls.append(source)
            if len(calls) >= 2:
                raise OSError("disk says no")
            real(source, target)

        with unittest.mock.patch("os.rename", broken):
            failure = place_job(self.dag, self.decision().placement)
        self.assertEqual(failure, PlaceFailed("disk says no", False))
        self.assertIn("putting them back failed", error_words(self.dag.jobs[0], failure))

    def test_one_blocked_output_keeps_all_of_them_where_they_are(self):
        self.build(self.TWO, ["out/a.txt", "out/b.txt"], "file")
        (self.root / "new/out").mkdir(parents=True)
        (self.root / "new/out/b.txt").write_text("not ours")
        decision = self.decision()
        self.assertEqual((decision.status, decision.placement.moves), (Status.RUN, ()))
        self.assertTrue((self.root / "out/a.txt").exists() and (self.root / "out/b.txt").exists())

    def test_a_folder_output_moves_whole(self):
        self.build(self.FOLDER, ["out/dir"], "folder")
        self.assertEqual(self.decision().placement.moves, (("out/dir", "new/out/dir"),))
        run(self.dag, state_at(self.root), move_files=True)
        self.assertEqual((self.root / "new/out/dir/one.txt").read_text(), "hello")
        self.assertFalse((self.root / "out/dir").exists())
        self.assertEqual([d.status for d in plan(self.dag, state_at(self.root))], [Status.SKIP])

    def test_a_folder_changed_since_the_run_is_not_moved(self):
        self.build(self.FOLDER, ["out/dir"], "folder")
        (self.root / "out/dir/extra.txt").write_text("added later")
        decision = self.decision()
        self.assertEqual((decision.placement.moves, decision.placement.cause), ((), MoveCause.OUTPUT_CHANGED))


class CommandLineTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        (self.root / "source.txt").write_text("hello")
        data = sample(self.root)
        run(read(data), state_at(self.root))
        self.before = (self.root / "final.txt").stat().st_mtime_ns
        relocated = moved(data, lambda p: p if p == "source.txt" else "derivatives/" + p)
        self.file = self.root / "moved.spitdag"
        self.file.write_text(json.dumps(placed(relocated)))

    def main(self, action, *flags):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main([action, str(self.file), "--root", str(self.root), *flags])
        return code, out.getvalue(), err.getvalue()

    def test_plan_lists_the_moves_and_changes_nothing(self):
        code, out, err = self.main("plan", "--move-files", "--verbose")
        self.assertEqual((code, err), (0, ""))
        self.assertEqual(out, "2 jobs will move their outputs\n"
                              "[1] place   op1: moves work/upper.txt -> derivatives/work/upper.txt\n"
                              "[2] place   op2: moves final.txt -> derivatives/final.txt\n")
        self.assertTrue((self.root / "final.txt").exists())
        self.assertFalse((self.root / "derivatives").exists())

    def test_plan_and_run_each_plan_once(self):
        for action in ("plan", "run"):
            with self.subTest(action), unittest.mock.patch.object(cli, "plan", wraps=cli.plan) as planned:
                code, _, err = self.main(action, "--move-files")
                self.assertEqual(code, 0, err)
                self.assertEqual(planned.call_count, 1)

    def test_plan_json_gives_the_moves(self):
        _, out, _ = self.main("plan", "--move-files", "--json")
        job = json.loads(out)["jobs"][1]
        self.assertEqual((job["status"], job["moves"]), ("place", [{"from": "final.txt", "to": "derivatives/final.txt"}]))

    def test_plan_says_why_a_job_is_not_moved(self):
        (self.root / "final.txt").write_text("edited by hand")
        _, out, _ = self.main("plan", "--move-files")
        self.assertEqual(out, "1 job can start, 1 job will move its outputs\n"
                              "[2] not moved: final.txt is missing or changed since the run\n")

    def test_run_moves_the_files_and_executes_no_job(self):
        code, out, err = self.main("run", "--move-files")
        self.assertEqual(code, 0, err)
        self.assertIn("  [2] moved   op2: final.txt -> derivatives/final.txt\n", out)
        self.assertIn("a tool that wrote those paths", err)
        self.assertIn("0 done, 2 moved, 0 checked", err)
        self.assertFalse((self.root / "final.txt").exists())
        self.assertEqual((self.root / "derivatives/final.txt").stat().st_mtime_ns, self.before)
        self.assertNotIn("start", out)
        self.assertEqual(self.main("plan", "--move-files")[1], "all 2 jobs are current\n")

    def test_without_migrate_the_jobs_run_and_nothing_is_moved(self):
        code, out, err = self.main("run")
        self.assertEqual(code, 0, err)
        self.assertNotIn("moved", out)
        self.assertTrue((self.root / "final.txt").exists())
        self.assertEqual((self.root / "derivatives/final.txt").read_text(), "HELLO!")


if __name__ == "__main__":
    unittest.main()
