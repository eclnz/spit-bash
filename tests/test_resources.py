"""SPIT's `props` (DAG version 8): which a job asks of the machine, and how the pool shares it out."""

import asyncio
import tempfile
import unittest

from spit_bash.dag import DagError
from spit_bash.resources import Pool, parse_cpus, parse_size
from support import read, sample


def version_eight(root, props):
    """The sample DAG as version 8, with `props` on each job."""
    data = sample(root)
    data.update(version=8, pipeline_files=[], calls=[])
    for item, given in zip(data["jobs"], props):
        item["origin"] = None
        item["checks"] = []
        item["props"] = given
    return data


class PropsTest(unittest.TestCase):
    def test_a_job_asks_for_the_cpus_and_memory_its_props_give(self):
        with tempfile.TemporaryDirectory() as root:
            dag = read(version_eight(root, [{"cpus": "4", "mem": "8G", "queue": "long"}, {}]))
        first, second = dag.jobs
        self.assertEqual((first.cpus, first.memory), (4, 8 << 30))
        self.assertEqual((second.cpus, second.memory), (1, None))

    def test_props_that_cannot_be_read_name_the_job(self):
        with tempfile.TemporaryDirectory() as root:
            for props, message in [({"cpus": "0"}, "job 1: `cpus`"), ({"cpus": "two"}, "job 1: `cpus`"),
                                   ({"mem": "lots"}, "job 1: `mem`")]:
                with self.subTest(props=props), self.assertRaisesRegex(DagError, message):
                    read(version_eight(root, [props, {}]))

    def test_props_and_the_version_seven_fields_must_match_the_version(self):
        with tempfile.TemporaryDirectory() as root:
            data = version_eight(root, [{}, {}])
            data["version"] = 7
            with self.assertRaisesRegex(DagError, "came with version 8"):
                read(data)
            data = version_eight(root, [{}, {}])
            del data["calls"]
            with self.assertRaisesRegex(DagError, "came with version 7"):
                read(data)

    def test_sizes_take_a_unit(self):
        self.assertEqual([parse_size(text) for text in ("512", "2K", "512M", "8G", "1T", "8GB", "8GiB")],
                         [512, 2048, 512 << 20, 8 << 30, 1 << 40, 8 << 30, 8 << 30])
        self.assertEqual(parse_cpus("12"), 12)


class PoolTest(unittest.TestCase):
    @staticmethod
    def most_at_once(pool, asks):
        """The most jobs holding the pool together, when each of `asks` holds it briefly."""
        running = peak = 0

        async def hold(ask):
            nonlocal running, peak
            async with pool.hold(*ask):
                running += 1
                peak = max(peak, running)
                await asyncio.sleep(0.01)
                running -= 1

        async def main():
            await asyncio.gather(*(hold(ask) for ask in asks))

        asyncio.run(main())
        return peak

    def test_jobs_share_the_processors(self):
        self.assertEqual(self.most_at_once(Pool(4), [(1, None)] * 6), 4)
        self.assertEqual(self.most_at_once(Pool(4), [(2, None)] * 4), 2)
        self.assertEqual(self.most_at_once(Pool(4), [(3, None), (1, None), (3, None)]), 2)

    def test_a_job_larger_than_the_pool_runs_alone(self):
        self.assertEqual(self.most_at_once(Pool(2), [(8, None), (1, None)]), 1)

    def test_memory_is_shared_only_when_a_limit_is_given(self):
        asks = [(1, 6 << 30)] * 3
        self.assertEqual(self.most_at_once(Pool(8), asks), 3)
        self.assertEqual(self.most_at_once(Pool(8, 16 << 30), asks), 2)


if __name__ == "__main__":
    unittest.main()
