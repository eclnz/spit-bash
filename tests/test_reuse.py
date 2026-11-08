"""The evidence a record gives against a job's files, and the verdict drawn from it."""

import copy
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from spit_bash.cause import Cause, Why
from spit_bash.files import stamps
from spit_bash.reuse import evidence as evidence_module
from spit_bash.reuse.evidence import Compare, Content, evidence
from spit_bash.reuse.verdict import Standing, Verdict, verdict
from support import read, run, sample, state_at


def touch(path, seconds=10):
    later = path.stat().st_mtime_ns + seconds * 1_000_000_000
    os.utime(path, ns=(later, later))


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source.txt").write_text("hello")
        self.data = sample(self.root)
        self.dag = read(self.data)
        run(self.dag, state_at(self.root))

    def found(self, job=0, compare=Compare.CONTENT, dag=None, every=False):
        dag = dag or self.dag
        job = dag.jobs[job]
        now = stamps(dag, (*job.input_paths, *job.output_paths))
        return evidence(dag, job, state_at(self.root).jobs.get(job.work.key), now, compare, every)

    def test_untouched_files_differ_in_nothing_and_are_current(self):
        found = self.found()
        self.assertEqual((found.work_changed, found.checks_changed, found.inputs, found.outputs),
                         (False, False, (), ()))
        self.assertEqual(verdict(found).standing, Standing.CURRENT)
        self.assertIsNone(verdict(found).refreshed)

    def test_a_touched_input_is_the_same_content_and_refreshes_the_record(self):
        touch(self.root / "source.txt")
        found = self.found()
        self.assertEqual([(d.slot, d.path, d.content) for d in found.inputs],
                         [("in0.0", "source.txt", Content.SAME)])
        self.assertNotEqual(found.inputs[0].recorded, found.inputs[0].now)
        judged = verdict(found)
        self.assertEqual(judged.standing, Standing.CURRENT)
        self.assertEqual(judged.refreshed.inputs["in0.0"][1], found.inputs[0].now.mtime_ns)

    def test_an_edited_input_differs_and_its_job_is_stale(self):
        (self.root / "source.txt").write_text("changed")
        found = self.found()
        self.assertEqual(found.inputs[0].content, Content.DIFFERS)
        self.assertEqual(verdict(found), Verdict(Standing.STALE, Cause(Why.INPUT_CHANGED)))

    def test_by_stat_a_touched_file_is_not_compared_and_counts_as_changed(self):
        touch(self.root / "source.txt")
        found = self.found(compare=Compare.STAT)
        self.assertEqual(found.inputs[0].content, Content.NOT_COMPARED)
        self.assertEqual(verdict(found).cause, Cause(Why.INPUT_CHANGED))

    def test_full_compares_every_file_whatever_its_stamp(self):
        found = self.found(compare=Compare.FULL)
        self.assertEqual([d.content for d in (*found.inputs, *found.outputs)], [Content.SAME, Content.SAME])
        self.assertEqual(verdict(found).standing, Standing.CURRENT)

    def test_a_record_without_hashes_counts_a_differing_file_as_changed(self):
        state = state_at(self.root)
        key = self.dag.jobs[0].work.key
        state.jobs[key] = state.jobs[key].model_copy(update={"input_hashes": None})
        touch(self.root / "source.txt")
        job = self.dag.jobs[0]
        found = evidence(self.dag, job, state.jobs[key], stamps(self.dag, (*job.input_paths, *job.output_paths)),
                         Compare.CONTENT)
        self.assertEqual(found.inputs[0].content, Content.NO_HASH)

    def test_a_changed_input_stops_the_comparison_before_the_outputs(self):
        (self.root / "source.txt").write_text("changed")
        touch(self.root / "work/upper.txt")
        with mock.patch.object(evidence_module, "content_hash", wraps=evidence_module.content_hash) as hashed:
            found = self.found()
        self.assertEqual(hashed.call_count, 1)
        self.assertEqual(found.outputs, ())
        # Asked for every file, it compares the output too.
        with mock.patch.object(evidence_module, "content_hash", wraps=evidence_module.content_hash) as hashed:
            found = self.found(every=True)
        self.assertEqual(hashed.call_count, 2)
        self.assertEqual(found.outputs[0].content, Content.SAME)

    def test_changed_work_names_its_parts_and_compares_no_files(self):
        changed = copy.deepcopy(self.data)
        changed["jobs"][0]["commands"][0]["command"].append(["x"])
        touch(self.root / "source.txt")
        found = self.found(dag=read(changed))
        self.assertEqual((found.work_changed, found.parts, found.inputs), (True, ("commands",), ()))
        self.assertEqual(verdict(found).cause, Cause(Why.WORK_CHANGED, parts=("commands",)))

    def test_no_record_is_stale(self):
        (self.root / ".spit-bash/state.jsonl").unlink()
        found = self.found()
        self.assertIsNone(found.record)
        self.assertEqual(verdict(found).cause, Cause(Why.NO_RECORD))

    def test_changed_checks_leave_only_the_checks_to_run(self):
        data = copy.deepcopy(self.data)
        item = data["jobs"][0]
        item["checks"] = [{"when": "after", "check": "nonempty", "port": "output", "path": "work/upper.txt",
                           "command": [["test", "-s", {"path": "work/upper.txt"}]]}]
        found = self.found(dag=read(data))
        self.assertTrue(found.checks_changed)
        self.assertEqual(verdict(found).standing, Standing.CHECKS_ONLY)


if __name__ == "__main__":
    unittest.main()
