import asyncio
import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import Job, Output
from spit_bash.schedule import ReadyQueue, longest_paths, ranks
from support import UPPER, document, job, read, run, state_at


def chain_job(id, depends_on=()):
    return Job(id=id, operation=f"op{id}", stage=(), fingerprint="0" * 16, inputs=(),
               produced=(Output("output", f"{id}.txt", "p", ()),), depends_on=depends_on, command=("true",), verify=())


async def admitted_order(workers, ranks, ids):
    """The order in which jobs that all become ready at once are let in."""
    queue = ReadyQueue(workers, ranks)
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

    def test_no_more_jobs_hold_slots_than_there_are_workers(self):
        async def main():
            queue = ReadyQueue(2, {})
            holding, most = 0, 0

            async def one(id):
                nonlocal holding, most
                async with queue.slot(chain_job(id)):
                    holding += 1
                    most = max(most, holding)
                    await asyncio.sleep(0.01)
                    holding -= 1

            await asyncio.gather(*(one(id) for id in range(1, 7)))
            return most, queue.free

        self.assertEqual(asyncio.run(main()), (2, 2))

    def test_a_job_cancelled_while_waiting_gives_up_its_place(self):
        async def main():
            queue = ReadyQueue(1, {1: 3.0, 2: 2.0, 3: 1.0})
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
            return order, queue.free

        self.assertEqual(asyncio.run(main()), ([1, 3], 1))


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
                _, results = run(dag, state_at(root), workers=1)
            self.assertFalse(any(result.error for result in results))
            starts = [line.split()[0] for line in out.getvalue().splitlines() if " start " in line]
            # Without times, each job counts as 1s: job 2 heads 2s of work, job 1 only 1s,
            # and job 1 comes before job 3 by id.
            self.assertEqual(starts, ["[2]", "[1]", "[3]"])


if __name__ == "__main__":
    unittest.main()
