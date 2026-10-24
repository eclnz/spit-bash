import tempfile
import unittest
from pathlib import Path
from unittest import mock

from spit_bash import progress as timing
from spit_bash.dag import Job, Output
from spit_bash.plan import State, job_key
from spit_bash.progress import Progress, duration
from support import read, run, sample, state_at


def jobs(*operations):
    return [Job(id=n, operation=op, stage=(), fingerprint="0" * 16, inputs=(),
                produced=(Output("output", f"{n}.txt", op, ()),), depends_on=(), command=("true",), verify=())
            for n, op in enumerate(operations, 1)]


class ProgressTests(unittest.TestCase):
    def test_a_job_with_no_time_takes_its_operations_mean(self):
        a1, a2, b1 = jobs("a", "a", "b")
        progress = Progress.of([a1, a2, b1], (a1, a2, b1), {}, workers=1)
        self.assertIsNone(progress.remaining())
        self.assertEqual(progress.report(), "3 left")
        with mock.patch.object(timing.time, "monotonic", return_value=0.0):
            progress.started(a1)
        progress.finished(a1, 10.0)
        # b has no time yet, so there is still no estimate.
        self.assertIsNone(progress.remaining())
        with mock.patch.object(timing.time, "monotonic", return_value=0.0):
            progress.started(b1)
        progress.finished(b1, 4.0)
        self.assertEqual(progress.remaining(), 10.0)
        self.assertEqual(progress.report(), "1 left, about 10s")

    def test_times_from_an_earlier_run_give_an_estimate_at_once(self):
        a1, a2, a3, a4 = jobs("a", "a", "a", "a")
        # a4 is current, but its time still counts for the operation's mean.
        progress = Progress.of([a1, a2, a3], (a1, a2, a3, a4), {1: 60.0, 4: 120.0}, workers=2)
        # a1 takes its own 60s; a2 and a3 take the mean of 60s and 120s.
        self.assertEqual(progress.remaining(), (60 + 90 + 90) / 2)

    def test_a_running_job_counts_for_what_it_has_left(self):
        a1, a2 = jobs("a", "a")
        progress = Progress.of([a1, a2], (a1, a2), {1: 100.0, 2: 100.0}, workers=2)
        with mock.patch.object(timing.time, "monotonic", return_value=1000.0):
            progress.started(a1)
        with mock.patch.object(timing.time, "monotonic", return_value=1030.0):
            self.assertEqual(progress.remaining(), (70 + 100) / 2)

    def test_a_dropped_job_is_no_longer_left(self):
        a1, a2 = jobs("a", "a")
        progress = Progress.of([a1, a2], (a1, a2), {1: 5.0, 2: 7.0}, workers=1)
        progress.dropped(a2)
        self.assertEqual((progress.left, progress.remaining()), (1, 5.0))

    def test_an_estimate_under_a_second_says_so(self):
        a1, = jobs("a")
        progress = Progress.of([a1], (a1,), {1: 0.2}, workers=1)
        self.assertEqual(progress.report(), "1 left, under 1s")

    def test_duration(self):
        self.assertEqual([duration(s) for s in (0.4, 45, 720, 3 * 3600 + 5 * 60)], ["under 1s", "45s", "12m", "3h05m"])

    def test_a_run_records_each_jobs_time(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            dag = read(sample(root))
            run(dag, state_at(root))
            records = State(root / ".spit-bash" / "state.jsonl").jobs
            for job in dag.jobs:
                self.assertGreater(records[job_key(job)].seconds, 0)


if __name__ == "__main__":
    unittest.main()
