"""A job of several commands, run in order until one fails."""

import asyncio
import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import DagError
from spit_bash.plan import Status, plan
from spit_bash.run import execute, partial
from support import command, document, job, read, run, state_at

# Write the input upper-cased to the output.
UPPER = "from pathlib import Path; import sys; Path(sys.argv[2]).write_text(Path(sys.argv[1]).read_text().upper())"
# Add a line to the output the command before wrote.
APPEND = "from pathlib import Path; import sys; p = Path(sys.argv[1]); p.write_text(p.read_text() + 'indexed\\n')"
# Print the output's text on standard output.
CAT = "from pathlib import Path; import sys; sys.stdout.write(Path(sys.argv[1]).read_text())"
FAIL = "import sys; sys.exit(1)"
# Say it started, then wait to be stopped.
WAIT = "import time; from pathlib import Path; import sys; Path(sys.argv[1]).write_text('started'); time.sleep(60)"


def step(script, *paths, stdin=None, stdout=None):
    return {"command": command(script, *paths), "stdin": stdin, "stdout": stdout}


def sequenced(root, *commands):
    """source.txt -> work/upper.txt, by each of `commands` in turn."""
    item = job(1, ["source.txt"], "work/upper.txt", UPPER)
    item["commands"] = list(commands)
    return document(root, [item], ["source.txt"])


class SequenceTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source.txt").write_text("hello\n")
        self.output = self.root / "work/upper.txt"
        self.log = self.root / ".spit-bash/logs/1-op1.log"

    def test_runs_each_command_in_order_and_logs_each(self):
        dag = read(sequenced(self.root, step(UPPER, "source.txt", "work/upper.txt"), step(APPEND, "work/upper.txt")))
        _, results = run(dag, state_at(self.root))
        self.assertEqual([r.error for r in results], [None])
        self.assertEqual(self.output.read_text(), "HELLO\nindexed\n")
        lines = [line for line in self.log.read_text().splitlines() if line.startswith("$ ")]
        self.assertEqual(len(lines), 2)
        self.assertIn("work/upper.txt", lines[1])
        self.assertEqual([d.status for d in plan(dag, state_at(self.root))], [Status.SKIP])

    def test_stops_at_the_first_failure_and_names_it(self):
        marker = self.root / "third-ran"
        dag = read(sequenced(
            self.root,
            step(UPPER, "source.txt", "work/upper.txt"),
            step(FAIL),
            step("from pathlib import Path; import sys; Path(sys.argv[1]).write_text('x')", "third-ran"),
        ))
        _, results = run(dag, state_at(self.root))
        self.assertEqual(results[0].error, "command 2 exited with status 1")
        self.assertFalse(marker.exists())
        # The first command wrote the output, which a failure after it removes.
        self.assertFalse(self.output.exists())
        self.assertIn("spit-bash: removed failed output work/upper.txt\n", self.log.read_text())

    def test_keep_failed_keeps_what_the_commands_before_the_failure_wrote(self):
        dag = read(sequenced(self.root, step(UPPER, "source.txt", "work/upper.txt"), step(FAIL)))
        _, results = run(dag, state_at(self.root), keep_failed=True)
        self.assertEqual(results[0].error, "command 2 exited with status 1")
        self.assertEqual(self.output.read_text(), "HELLO\n")

    def test_a_command_reads_the_standard_output_of_one_before_it(self):
        copy = "from pathlib import Path; import sys; Path(sys.argv[2]).write_text(Path(sys.argv[1]).read_text() + 'again\\n')"
        data = sequenced(self.root, step(CAT, "source.txt", stdout="work/upper.txt"),
                         step(copy, "work/upper.txt", "work/upper.txt"))
        dag = read(data)
        _, results = run(dag, state_at(self.root))
        self.assertEqual([r.error for r in results], [None])
        self.assertEqual(self.output.read_text(), "hello\nagain\n")
        self.assertFalse(partial(dag, "work/upper.txt").exists())
        self.assertIn(" > work/upper.txt\n", self.log.read_text())

    def test_a_failure_after_the_standard_output_is_written_removes_it(self):
        dag = read(sequenced(self.root, step(CAT, "source.txt", stdout="work/upper.txt"), step(FAIL)))
        _, results = run(dag, state_at(self.root))
        self.assertEqual(results[0].error, "command 2 exited with status 1")
        self.assertEqual(list(self.output.parent.iterdir()), [])

    def test_cancelling_a_later_command_removes_the_outputs(self):
        started = self.root / "started"
        dag = read(sequenced(self.root, step(UPPER, "source.txt", "work/upper.txt"), step(WAIT, "started")))
        state = state_at(self.root)

        async def cancel_once_started():
            task = asyncio.ensure_future(execute(dag, plan(dag, state), state, 1, self.root / "logs"))
            while not started.exists():
                await asyncio.sleep(0.01)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        asyncio.run(cancel_once_started())
        state.close()
        self.assertFalse(self.output.exists())

    def test_a_later_command_is_held_to_the_jobs_memory(self):
        greedy = "b = bytearray(512 * 2**20); import time; time.sleep(5)"
        data = sequenced(self.root, step(UPPER, "source.txt", "work/upper.txt"), step(greedy))
        data["jobs"][0]["memory"] = 64 * 2**20
        _, results = run(read(data), state_at(self.root))
        self.assertRegex(results[0].error, r"^command 2 used .* of memory, more than the 64M its job declares")

    def test_one_command_at_most_has_a_standard_output(self):
        data = sequenced(self.root, step(CAT, "source.txt", stdout="work/upper.txt"),
                         step(CAT, "source.txt", stdout="work/upper.txt"))
        with self.assertRaisesRegex(DagError, "more than one of its commands has a `stdout`"):
            read(data)

    def test_a_job_of_no_commands_does_not_run(self):
        data = sequenced(self.root)
        dag = read(data)
        self.assertEqual(dag.jobs[0].commands, ())
        self.assertEqual(plan(dag, state_at(self.root))[0].reason, "no command and output is missing")


if __name__ == "__main__":
    unittest.main()
