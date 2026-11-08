import asyncio
import os
import tempfile
import unittest
from pathlib import Path

from spit_bash import files
from spit_bash.cause import Cause, Why
from spit_bash.model import DagError
from spit_bash.plan import Status, plan, writes
from spit_bash.reuse.evidence import Compare
from spit_bash.state.store import Store
from spit_bash.run import execute
from support import artifact, command, read, run, sample, state_at


def touch(path, seconds=10):
    """Move a file's modification time without changing what it holds."""
    later = path.stat().st_mtime_ns + seconds * 1_000_000_000
    os.utime(path, ns=(later, later))


class ContentTests(unittest.TestCase):
    def ran(self, directory):
        root = Path(directory)
        (root / "source.txt").write_text("hello")
        dag = read(sample(root))
        run(dag, state_at(root))
        return root, dag

    def statuses(self, dag, root, **options):
        return [d.status for d in plan(dag, state_at(root), **options)]

    def test_touched_input_is_still_current_and_the_new_stamp_is_kept(self):
        with tempfile.TemporaryDirectory() as directory:
            root, dag = self.ran(directory)
            touch(root / "source.txt")
            self.assertEqual(self.statuses(dag, root), [Status.SKIP, Status.SKIP])
            state = state_at(root)
            state.lock()
            state.apply(writes(plan(dag, state)))
            state.close()
            # The stored stamp now matches, so it holds even without hashing.
            self.assertEqual(self.statuses(dag, root, compare=Compare.STAT), [Status.SKIP, Status.SKIP])

    def test_changed_input_runs_its_job_and_may_run_the_one_after_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root, dag = self.ran(directory)
            (root / "source.txt").write_text("HELLO")
            touch(root / "source.txt")
            decisions = plan(dag, state_at(root))
            self.assertEqual([d.status for d in decisions], [Status.RUN, Status.MAYBE])
            self.assertEqual(decisions[0].cause, Cause(Why.INPUT_CHANGED))

    def test_touched_output_is_current_but_an_edited_one_is_not(self):
        with tempfile.TemporaryDirectory() as directory:
            root, dag = self.ran(directory)
            touch(root / "final.txt")
            self.assertEqual(self.statuses(dag, root), [Status.SKIP, Status.SKIP])
            (root / "final.txt").write_text("edited")
            self.assertEqual(self.statuses(dag, root), [Status.SKIP, Status.RUN])

    def test_comparing_by_stat_treats_a_touch_as_a_change(self):
        with tempfile.TemporaryDirectory() as directory:
            root, dag = self.ran(directory)
            touch(root / "source.txt")
            self.assertEqual(self.statuses(dag, root, compare=Compare.STAT), [Status.RUN, Status.RUN])

    def test_comparing_in_full_catches_a_change_that_kept_the_stamp(self):
        with tempfile.TemporaryDirectory() as directory:
            root, dag = self.ran(directory)
            source = root / "source.txt"
            before = source.stat().st_mtime_ns
            source.write_text("HELLO")
            os.utime(source, ns=(before, before))
            # Same size and time: only reading the bytes tells.
            self.assertEqual(self.statuses(dag, root), [Status.SKIP, Status.SKIP])
            self.assertEqual(self.statuses(dag, root, compare=Compare.FULL), [Status.RUN, Status.MAYBE])

    def test_comparing_in_full_reruns_a_job_whose_record_holds_no_hashes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            dag = read(sample(root))
            state = state_at(root)
            asyncio.run(execute(dag, plan(dag, state), state, 2, root / ".spit-bash/logs", compare=Compare.STAT))
            state.close()
            self.assertEqual(self.statuses(dag, root), [Status.SKIP, Status.SKIP])
            self.assertEqual(self.statuses(dag, root, compare=Compare.FULL), [Status.RUN, Status.RUN])

    def test_a_run_without_hashing_records_none_and_a_touch_then_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            dag = read(sample(root))
            state = state_at(root)
            asyncio.run(execute(dag, plan(dag, state), state, 2, root / ".spit-bash/logs", compare=Compare.STAT))
            state.close()
            self.assertTrue(all(r.input_hashes is None for r in state_at(root).jobs.values()))
            touch(root / "source.txt")
            self.assertEqual(self.statuses(dag, root), [Status.RUN, Status.MAYBE])

    def test_a_record_made_with_another_hash_is_not_equal(self):
        with tempfile.TemporaryDirectory() as directory:
            root, dag = self.ran(directory)
            state = state_at(root)
            for key, record in state.jobs.items():
                other = {path: "other:" + value.split(":", 1)[1] for path, value in record.input_hashes.items()}
                state.jobs[key] = record.model_copy(update={"input_hashes": other})
            touch(root / "source.txt")
            self.assertEqual([d.status for d in plan(dag, state)], [Status.RUN, Status.MAYBE])

    def test_every_input_and_output_of_a_job_is_judged_by_content(self):
        """A many port and two output ports: each file is compared on its own."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("a", "b", "c"):
                (root / f"{name}.txt").write_text(name)
            script = ("from pathlib import Path; import sys; t = ''.join(Path(p).read_text() for p in sys.argv[3:]); "
                      "Path(sys.argv[1]).write_text(t); Path(sys.argv[2]).write_text(t.upper())")
            data = sample(root)
            data["jobs"] = [{
                "id": 1, "operation": "both", "stage": [], "dependents": [], "depends_on": [],
                "inputs": {"items": [artifact(f"{n}.txt") for n in "abc"]}, "verify": [],
                "checks": [], "origin": None, "cores": 1, "memory": None,
                "outputs": {"first": artifact("one.txt"), "second": artifact("two.txt")},
                "commands": [{"command": command(script, "one.txt", "two.txt", "a.txt", "b.txt", "c.txt"),
                              "stdin": None, "stdout": None}],
            }]
            data["external_inputs"] = [artifact(f"{n}.txt") for n in "abc"]
            dag = read(data)
            run(dag, state_at(root))
            self.assertEqual((root / "two.txt").read_text(), "ABC")
            states = lambda: [d.status for d in plan(dag, state_at(root))]
            for name in ("a.txt", "c.txt", "one.txt", "two.txt"):
                touch(root / name)
            self.assertEqual(states(), [Status.SKIP])
            (root / "c.txt").write_text("C")
            self.assertEqual(states(), [Status.RUN])
            (root / "c.txt").write_text("c")
            touch(root / "c.txt", 20)
            self.assertEqual(states(), [Status.SKIP])
            (root / "two.txt").write_text("edited")
            self.assertEqual(states(), [Status.RUN])

    def test_folder_hash_follows_names_and_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory) / "pages"
            (folder / "sub").mkdir(parents=True)
            (folder / "1.txt").write_text("one")
            (folder / "sub/2.txt").write_text("two")
            first = files.content_hash(str(folder), folder=True)
            touch(folder / "1.txt")
            self.assertEqual(files.content_hash(str(folder), folder=True), first)
            (folder / "1.txt").rename(folder / "3.txt")
            self.assertNotEqual(files.content_hash(str(folder), folder=True), first)
            (folder / "3.txt").rename(folder / "1.txt")
            (folder / "sub/2.txt").write_text("TWO")
            self.assertNotEqual(files.content_hash(str(folder), folder=True), first)
            self.assertIsNone(files.content_hash(str(folder / "1.txt"), folder=True))
            self.assertIsNone(files.content_hash(str(folder)))
            self.assertIsNone(files.content_hash(str(folder / "missing")))

    def test_an_older_state_is_refused_with_advice_to_delete_it(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.jsonl"
            path.write_text('{"version": 5}\n')
            with self.assertRaisesRegex(DagError, "delete it to start from fresh records"):
                Store(path)


if __name__ == "__main__":
    unittest.main()
