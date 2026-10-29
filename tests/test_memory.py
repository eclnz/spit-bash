"""A job that declares memory is killed when its commands hold more."""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from spit_bash import run as run_module
from spit_bash import memory as memory_module
from spit_bash.memory import group_memory
from support import read, run, sample, state_at

MIB = 2**20
# Hold 200 MiB, write the output, then wait long enough to be read.
GREEDY = ("import sys, time; from pathlib import Path\n"
          "held = bytearray(200 * 2**20)\n"
          "Path(sys.argv[2]).write_text('done')\n"
          "time.sleep(2)\n")


class GroupMemoryTests(unittest.TestCase):
    def test_counts_every_process_in_the_group(self):
        script = "import time; held = bytearray(50 * 2**20); time.sleep(30)"
        parent = subprocess.Popen(
            [sys.executable, "-c",
             f"import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', {script!r}]); "
             f"held = bytearray(50 * 2**20); time.sleep(30)"],
            start_new_session=True,
        )
        self.addCleanup(parent.wait)
        self.addCleanup(os.killpg, parent.pid, 9)
        # Wait until both processes hold their memory and the reading has settled,
        # so that /proc and `ps` are compared on the same pages.
        used, last = 0, -1
        for _ in range(200):
            used = group_memory(parent.pid)
            if used > 100 * MIB and abs(used - last) < MIB:
                break
            last = used
            subprocess.run(["sleep", "0.05"])
        self.assertGreater(used, 100 * MIB)
        # `ps`, read where there is no /proc, gives the same.
        self.assertAlmostEqual(memory_module._from_ps(parent.pid) / MIB, group_memory(parent.pid) / MIB, delta=5)

    def test_an_empty_group_holds_nothing(self):
        # A group id no process uses, as process ids are below 2^22 on Linux and 99999 on macOS.
        self.assertEqual(group_memory(2**30), 0)


class EnforcementTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source.txt").write_text("hello")
        patch = mock.patch.object(run_module, "MEMORY_POLL_SECONDS", 0.05)
        patch.start()
        self.addCleanup(patch.stop)

    def greedy(self, memory):
        data = sample(self.root)
        data["jobs"][0]["commands"][0]["command"][2] = [GREEDY]
        data["jobs"][0]["memory"] = memory
        return read(data)

    def test_a_job_holding_more_than_it_declares_is_killed(self):
        _, results = run(self.greedy(64 * MIB), state_at(self.root))
        error = results[0].error or ""
        self.assertRegex(error, r"^command used \d+(\.\d)?M of memory, more than the 64M its job declares, "
                                r"and was killed$")
        # Its output is removed as with any failure, and its dependent does not run.
        self.assertFalse((self.root / "work/upper.txt").exists())
        self.assertEqual(results[1].error, "dependency 1 failed")

    def test_a_job_within_what_it_declares_runs(self):
        _, results = run(self.greedy(1024 * MIB), state_at(self.root))
        self.assertEqual([result.error for result in results], [None, None])

    def test_a_job_that_declares_no_memory_is_not_limited(self):
        _, results = run(self.greedy(None), state_at(self.root))
        self.assertEqual([result.error for result in results], [None, None])

    def test_without_enforcement_a_job_may_hold_more(self):
        _, results = run(self.greedy(64 * MIB), state_at(self.root), enforce_memory=False)
        self.assertEqual([result.error for result in results], [None, None])


if __name__ == "__main__":
    unittest.main()
