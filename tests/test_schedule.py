import asyncio
import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import Job, Output, Run
from spit_bash.schedule import ReadyQueue, longest_paths, ranks, size, too_big
from support import UPPER, document, job, read, run, state_at


def chain_job(id, depends_on=(), cores=1, memory=None):
    return Job(id=id, operation=f"op{id}", stage=(), fingerprint="0" * 16, inputs=(),
               produced=(Output("output", f"{id}.txt", "p", ()),), depends_on=depends_on, commands=(Run(("true",)),), verify=(),
               cores=cores, memory=memory)


async def admitted_order(cores, ranks, ids):
    """The order in which jobs that all become ready at once are let in."""
    queue = ReadyQueue(cores, None, ranks)
    order = []

    async def one(id):
        async with queue.slot(chain_job(id)):
            order.append(id)
            await asyncio.sleep(0)

    await asyncio.gather(*(one(id) for id in ids))
    return order


class LongestPathTests(unittest.TestCase):
    def test_a_job_heading_a_chain_ranks_above_one_with_nothing_after_it(self):
        # 1 -> 2 -> 3, and 4 alone.
        jobs = [chain_job(1), chain_job(2, (1,)), chain_job(3, (2,)), chain_job(4)]
        start, after = longest_paths(jobs, lambda job: {1: 1.0, 2: 2.0, 3: 3.0, 4: 5.0}[job.id])
        self.assertEqual(start, {1: 6.0, 2: 5.0, 3: 3.0, 4: 5.0})
        self.assertEqual(after, {1: 5.0, 2: 3.0, 3: 0.0, 4: 0.0})

    def test_the_longest_of_several_dependents_counts(self):
        jobs = [chain_job(1), chain_job(2, (1,)), chain_job(3, (1,))]
        start, _ = longest_paths(jobs, lambda job: float(job.id))
        self.assertEqual(start[1], 1.0 + 3.0)

    def test_a_dependency_outside_the_jobs_is_ignored(self):
        start, _ = longest_paths([chain_job(2, (1,))], lambda job: 2.0)
        self.assertEqual(start, {2: 2.0})

    def test_a_job_with_no_time_counts_as_the_median_or_one_second(self):
        jobs = [chain_job(1), chain_job(2, (1,))]
        self.assertEqual(ranks(jobs, lambda job: None, {}), {1: 2.0, 2: 1.0})
        self.assertEqual(ranks(jobs, lambda job: None, {7: 4.0, 8: 6.0, 9: 100.0}), {1: 12.0, 2: 6.0})
        self.assertEqual(ranks(jobs, lambda job: 3.0 if job.id == 2 else None, {7: 4.0}), {1: 7.0, 2: 3.0})

    def test_a_long_chain_needs_no_recursion(self):
        jobs = [chain_job(1)] + [chain_job(n, (n - 1,)) for n in range(2, 100_001)]
        start, _ = longest_paths(jobs, lambda job: 1.0)
        self.assertEqual(start[1], 100_000.0)


class ReadyQueueTests(unittest.TestCase):
    def test_ready_jobs_start_highest_rank_first_then_by_id(self):
        order = asyncio.run(admitted_order(1, {1: 1.0, 2: 5.0, 3: 5.0, 4: 2.0}, [1, 2, 3, 4]))
        self.assertEqual(order, [2, 3, 4, 1])

    def test_the_order_does_not_depend_on_which_job_asks_first(self):
        ranks = {1: 1.0, 2: 5.0, 3: 5.0, 4: 2.0}
        self.assertEqual(asyncio.run(admitted_order(2, ranks, [4, 3, 1, 2])), [2, 3, 4, 1])

    def test_no_more_jobs_hold_cores_than_there_are_cores(self):
        async def main():
            queue = ReadyQueue(2, None, {})
            holding, most = 0, 0

            async def one(id):
                nonlocal holding, most
                async with queue.slot(chain_job(id)):
                    holding += 1
                    most = max(most, holding)
                    await asyncio.sleep(0.01)
                    holding -= 1

            await asyncio.gather(*(one(id) for id in range(1, 7)))
            return most, queue.free_cores

        self.assertEqual(asyncio.run(main()), (2, 2))

    def test_a_job_cancelled_while_waiting_gives_up_its_place(self):
        async def main():
            queue = ReadyQueue(1, None, {1: 3.0, 2: 2.0, 3: 1.0})
            order = []
            release = asyncio.Event()

            async def one(id):
                async with queue.slot(chain_job(id)):
                    order.append(id)
                    if id == 1:
                        await release.wait()

            first, second, third = (asyncio.create_task(one(id)) for id in (1, 2, 3))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            second.cancel()
            release.set()
            await asyncio.gather(first, third)
            with contextlib.suppress(asyncio.CancelledError):
                await second
            return order, queue.free_cores

        self.assertEqual(asyncio.run(main()), ([1, 3], 1))


async def overlaps(queue, jobs, hold=0.01):
    """Run each job's slot for `hold` seconds; give the order they started in and the sets that ran at once."""
    order, running, together = [], set(), []

    async def one(job):
        async with queue.slot(job):
            order.append(job.id)
            running.add(job.id)
            together.append(frozenset(running))
            await asyncio.sleep(hold)
            running.discard(job.id)

    await asyncio.wait_for(asyncio.gather(*(one(job) for job in jobs)), 5)
    return order, together


class ResourceTests(unittest.TestCase):
    def test_jobs_start_only_while_their_cores_fit(self):
        # Four cores: the 3-core job and a 1-core job fit together, the 2-core jobs do not fit beside the 3.
        jobs = [chain_job(1, cores=3), chain_job(2, cores=2), chain_job(3, cores=2), chain_job(4)]
        queue = ReadyQueue(4, None, {1: 4.0, 2: 3.0, 3: 2.0, 4: 1.0})
        order, together = asyncio.run(overlaps(queue, jobs))
        self.assertEqual(order, [1, 2, 3, 4])
        for running in together:
            self.assertLessEqual(sum(jobs[id - 1].cores for id in running), 4)
        self.assertEqual((queue.free_cores, queue.free_memory), (4, 0))

    def test_jobs_start_only_while_their_memory_fits(self):
        gib = 1024**3
        jobs = [chain_job(1, memory=3 * gib), chain_job(2, memory=2 * gib), chain_job(3, memory=gib), chain_job(4)]
        queue = ReadyQueue(8, 4 * gib, {1: 4.0, 4: 3.0, 2: 2.0, 3: 1.0})
        _, together = asyncio.run(overlaps(queue, jobs))
        for running in together:
            self.assertLessEqual(sum(jobs[id - 1].memory or 0 for id in running), 4 * gib)
        # Job 1 runs beside job 4, which claims no memory, but never beside job 2.
        self.assertFalse(any({1, 2} <= running for running in together))
        self.assertIn(frozenset({1, 4}), together)
        self.assertEqual(queue.free_memory, 4 * gib)

    def test_without_a_memory_limit_memory_does_not_hold_a_job_back(self):
        jobs = [chain_job(1, memory=2**53), chain_job(2, memory=2**53)]
        _, together = asyncio.run(overlaps(ReadyQueue(2, None, {}), jobs))
        self.assertIn(frozenset({1, 2}), together)

    def test_a_job_waiting_at_the_head_holds_back_smaller_jobs_behind_it(self):
        # Job 2 needs both cores; job 3 would fit beside job 1 but waits its turn.
        jobs = [chain_job(1), chain_job(2, cores=2), chain_job(3)]
        _, together = asyncio.run(overlaps(ReadyQueue(2, None, {1: 3.0, 2: 2.0, 3: 1.0}), jobs))
        self.assertEqual(together, [frozenset({1}), frozenset({2}), frozenset({3})])

    def test_a_job_needing_more_than_the_run_has_runs_alone(self):
        gib = 1024**3
        jobs = [chain_job(1), chain_job(2, cores=16), chain_job(3), chain_job(4, memory=64 * gib), chain_job(5)]
        queue = ReadyQueue(4, 8 * gib, {1: 5.0, 2: 4.0, 3: 3.0, 4: 2.0, 5: 1.0})
        order, together = asyncio.run(overlaps(queue, jobs))
        self.assertEqual(sorted(order), [1, 2, 3, 4, 5])
        self.assertIn(frozenset({2}), together)
        self.assertIn(frozenset({4}), together)
        self.assertFalse(any(len(running) > 1 and running & {2, 4} for running in together))
        self.assertEqual((queue.free_cores, queue.free_memory), (4, 8 * gib))

    def test_oversized_jobs_one_after_another_do_not_deadlock(self):
        jobs = [chain_job(id, cores=8, memory=2**40) for id in range(1, 5)]
        order, together = asyncio.run(overlaps(ReadyQueue(2, 2**30, {}), jobs, hold=0))
        self.assertEqual(order, [1, 2, 3, 4])
        self.assertTrue(all(len(running) == 1 for running in together))

    def test_what_a_job_needs_beyond_the_run_is_named(self):
        self.assertIsNone(too_big(chain_job(1, cores=4, memory=2**30), 4, 2**30))
        self.assertIsNone(too_big(chain_job(1, memory=2**40), 4, None))
        self.assertEqual(too_big(chain_job(1, cores=16), 8, None), "16 cores where the run has 8")
        self.assertEqual(too_big(chain_job(1, cores=16, memory=64 * 2**30), 8, 3 * 2**29),
                         "16 cores where the run has 8, and 64G of memory where the run has 1.5G")
        self.assertEqual([size(n) for n in (512, 1024, 64 * 2**20, 2**53)], ["512", "1K", "64M", "8192T"])


class RunOrderTests(unittest.TestCase):
    def test_a_run_starts_the_head_of_a_chain_before_a_lone_job(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            # Job 1 stands alone; job 2 heads a chain of 2 -> 3.
            dag = read(document(root, [
                job(1, ["source.txt"], "alone.txt", UPPER),
                job(2, ["source.txt"], "head.txt", UPPER, dependents=[3]),
                job(3, ["head.txt"], "tail.txt", UPPER, depends_on=[2]),
            ], ["source.txt"]))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                _, results = run(dag, state_at(root), cores=1)
            self.assertFalse(any(result.error for result in results))
            starts = [line.split()[0] for line in out.getvalue().splitlines() if " start " in line]
            # Without times, each job counts as 1s: job 2 heads 2s of work, job 1 only 1s,
            # and job 1 comes before job 3 by id.
            self.assertEqual(starts, ["[2]", "[1]", "[3]"])

    def test_cores_decide_how_many_jobs_a_run_starts_at_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            slow = "import time; time.sleep(0.2); " + UPPER
            data = document(root, [job(1, ["source.txt"], "a.txt", slow), job(2, ["source.txt"], "b.txt", slow)],
                            ["source.txt"])
            for item in data["jobs"]:
                item["cores"] = 2

            def events(cores):
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    _, results = run(read(data), state_at(root), cores=cores, force=True)
                self.assertFalse(any(result.error for result in results))
                return [line.split()[1] for line in out.getvalue().splitlines() if line.startswith("[")]

            # Three cores hold one two-core job at a time; four hold both.
            self.assertEqual(events(3), ["start", "done", "start", "done"])
            self.assertEqual(events(4), ["start", "start", "done", "done"])


if __name__ == "__main__":
    unittest.main()
