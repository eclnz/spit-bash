"""A job's fingerprint and state key follow what it makes and runs, not where its files are."""

import copy
import shutil
import tempfile
import unittest
from pathlib import Path

from spit_bash import migrate
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
    setUp_base = ReuseAfterPathChangeTests.setUp
    copy_outputs = ReuseAfterPathChangeTests.copy_outputs

    def setUp(self):
        self.setUp_base()
        self.dag = read(self.relocated)
        self.state = state_at(self.root)

    def test_copies_recorded_outputs_to_the_new_paths_and_leaves_the_old(self):
        migrations = migrate.find(self.dag, self.state)
        self.assertEqual([m.reason for m in migrations], ["outputs recorded at other paths"] * 2)
        migrate.apply(self.dag, migrations)
        self.assertEqual((self.root / "derivatives/final.txt").read_text(), "HELLO!")
        self.assertTrue((self.root / "final.txt").exists())
        self.assertEqual([d.status for d in plan(self.dag, state_at(self.root))], [Status.SKIP, Status.SKIP])

    def test_an_existing_destination_is_never_overwritten(self):
        target = self.root / "derivatives/work/upper.txt"
        target.parent.mkdir(parents=True)
        target.write_text("someone else's")
        migrations = {m.job.id: m for m in migrate.find(self.dag, self.state)}
        self.assertEqual(migrations[1].copies, ())
        self.assertIn("already exists", migrations[1].reason)
        migrate.apply(self.dag, tuple(migrations.values()))
        self.assertEqual(target.read_text(), "someone else's")

    def test_an_output_changed_since_the_run_is_not_copied(self):
        (self.root / "final.txt").write_text("edited by hand")
        migrations = {m.job.id: m for m in migrate.find(self.dag, self.state)}
        self.assertEqual(migrations[2].copies, ())
        self.assertIn("missing or changed", migrations[2].reason)

    def test_nothing_to_migrate_when_paths_are_unchanged(self):
        self.assertEqual(migrate.find(read(self.data), self.state), ())


if __name__ == "__main__":
    unittest.main()
