"""A job that declares memory is killed when its commands hold more."""

import asyncio
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
from spit_bash.plan import plan
from spit_bash.run import execute
from support import document, job, read, run, sample, state_at

MIB = 2**20
# Hold 200 MiB, write the output, then wait long enough to be read.
GREEDY = ("import sys, time; from pathlib import Path\n"
          "held = bytearray(200 * 2**20)\n"
          "Path(sys.argv[2]).write_text('done')\n"
          "time.sleep(2)\n")
QUICK_GREEDY = "held = bytearray(200 * 2**20)"


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

    def test_a_quick_command_is_checked_for_its_peak_rss_after_exit(self):
        data = sample(self.root)
        data["jobs"][0]["commands"][0]["command"][2] = [QUICK_GREEDY]
        data["jobs"][0]["memory"] = 64 * MIB
        with mock.patch.object(run_module, "MEMORY_POLL_SECONDS", 5):
            _, results = run(read(data), state_at(self.root))
        self.assertRegex(results[0].error or "", r"^command used \d+(\.\d)?M of memory, more than the 64M")
        self.assertFalse((self.root / "work/upper.txt").exists())

    def test_a_descendant_is_still_checked_by_group_sampling(self):
        data = sample(self.root)
        data["jobs"][0]["commands"][0]["command"][2] = [
            "import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', "
            "'import time; held = bytearray(200 * 2**20); time.sleep(30)']); time.sleep(2)"
        ]
        data["jobs"][0]["memory"] = 64 * MIB
        _, results = run(read(data), state_at(self.root))
        self.assertRegex(results[0].error or "", r"^command used \d+(\.\d)?M of memory, more than the 64M")
        self.assertFalse((self.root / "work/upper.txt").exists())

    def test_the_group_stays_anchored_while_post_exit_usage_is_checked(self):
        ready = self.root / "child-ready"
        child = (f"import time; from pathlib import Path; held = bytearray(200 * 2**20); "
                 f"Path({str(ready)!r}).write_text('ready'); time.sleep(30)")
        parent = ("import subprocess, sys, time; from pathlib import Path; "
                  f"subprocess.Popen([sys.executable, '-c', {child!r}]); "
                  f"ready = Path({str(ready)!r}); "
                  "exec('while not ready.exists(): time.sleep(0.01)')")
        data = sample(self.root)
        data["jobs"][0]["commands"][0]["command"][2] = [parent]
        data["jobs"][0]["memory"] = 64 * MIB
        _, results = run(read(data), state_at(self.root))
        self.assertIn("was rejected after it exited", results[0].error or "")
        self.assertFalse((self.root / "work/upper.txt").exists())

    def test_a_tiny_native_command_is_not_charged_the_monitor_rss(self):
        dag = read(sample(self.root))
        with tempfile.TemporaryFile() as log:
            error = asyncio.run(run_module._command(dag, ("/usr/bin/true",), "command", log, limit=8 * MIB))
        self.assertIsNone(error)

    def test_parallel_commands_keep_peak_accounting_per_command(self):
        data = document(self.root, [
            job(1, ["source.txt"], "work/large.txt", QUICK_GREEDY),
            job(2, ["source.txt"], "work/small.txt", "from pathlib import Path; Path('work/small.txt').write_text('ok')"),
        ], ["source.txt"])
        for item in data["jobs"]:
            item["memory"] = 64 * MIB
        _, results = run(read(data), state_at(self.root), cores=2)
        self.assertIn("more than the 64M", results[0].error or "")
        self.assertIsNone(results[1].error)

    def test_cancelling_a_limited_command_stops_its_wrapper_and_group(self):
        started = self.root / "started"
        data = sample(self.root)
        data["jobs"][0]["commands"][0]["command"][2] = [
            "from pathlib import Path; import time; Path('started').write_text('yes'); time.sleep(30)"
        ]
        data["jobs"][0]["memory"] = 64 * MIB
        dag = read(data)
        state = state_at(self.root)

        async def cancel_when_started():
            task = asyncio.create_task(execute(dag, plan(dag, state), state, 1,
                                               self.root / ".spit-bash" / "logs", enforce_memory=True))
            while not started.exists():
                await asyncio.sleep(0.01)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        asyncio.run(cancel_when_started())
        state.close()
        self.assertFalse((self.root / "work/upper.txt").exists())

    def test_sigkill_reports_possible_oom_and_declared_memory(self):
        data = sample(self.root)
        data["jobs"][0]["commands"][0]["command"][2] = [
            "import os, signal; os.kill(os.getpid(), signal.SIGKILL)"
        ]
        data["jobs"][0]["memory"] = 64 * MIB
        _, results = run(read(data), state_at(self.root))
        self.assertEqual(
            results[0].error,
            "command killed by signal 9 (SIGKILL), possibly out of memory, declared memory=64M",
        )

    def test_sigkill_without_memory_declaration_still_reports_signal(self):
        data = sample(self.root)
        data["jobs"][0]["commands"][0]["command"][2] = [
            "import os, signal; os.kill(os.getpid(), signal.SIGKILL)"
        ]
        data["jobs"][0]["memory"] = None
        _, results = run(read(data), state_at(self.root))
        self.assertEqual(
            results[0].error,
            "command killed by signal 9 (SIGKILL), possibly out of memory",
        )

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
