"""A command that reads an input on its standard input and writes an output on its standard output."""

import asyncio
import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import DagError
from spit_bash.plan import Status, plan
from spit_bash.run import execute, partial
from support import command, document, job, read, run, state_at

# Upper-case standard input onto standard output, and say so on standard error.
UPPER = "import sys; sys.stdout.write(sys.stdin.read().upper()); sys.stderr.write('upper-cased\\n')"
# Write part of the output, then fail.
HALF = "import sys; sys.stdout.write('half'); sys.stdout.flush(); sys.exit(4)"
# Write part of the output, then wait to be stopped.
WAIT = ("import sys, time; from pathlib import Path\n"
        "sys.stdout.write('half'); sys.stdout.flush()\n"
        "Path(sys.argv[1]).write_text('started')\n"
        "time.sleep(60)\n")


def streamed(root, script, *args, stdin="source.txt"):
    """source.txt -> work/upper.txt, through the command's standard input and output."""
    item = job(1, ["source.txt"], "work/upper.txt", script)
    item["commands"][0]["command"] = command(script) + [[arg] for arg in args]
    item["commands"][0]["stdin"] = stdin
    item["commands"][0]["stdout"] = "work/upper.txt"
    return document(root, [item], ["source.txt"])


class StreamTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source.txt").write_text("hello\n")
        self.output = self.root / "work/upper.txt"
        self.log = self.root / ".spit-bash/logs/1-op1.log"

    def test_reads_stdin_and_writes_stdout_to_the_output(self):
        dag = read(streamed(self.root, UPPER))
        run_ = dag.jobs[0].commands[0]
        self.assertEqual((run_.stdin, run_.stdout), ("source.txt", "work/upper.txt"))
        decisions, results = run(dag, state_at(self.root))
        self.assertEqual([r.error for r in results], [None])
        self.assertEqual(self.output.read_text(), "HELLO\n")
        self.assertFalse(partial(dag, "work/upper.txt").exists())
        log = self.log.read_text()
        # The log shows the redirections, and holds standard error but not the output.
        self.assertIn(" < source.txt > work/upper.txt\n", log)
        self.assertIn("upper-cased\n", log)
        self.assertNotIn("HELLO", log)
        self.assertEqual([d.status for d in plan(dag, state_at(self.root))], [Status.SKIP])

    def test_without_stdin_the_command_reads_nothing(self):
        dag = read(streamed(self.root, UPPER, stdin=None))
        _, results = run(dag, state_at(self.root))
        self.assertEqual([r.error for r in results], [None])
        self.assertEqual(self.output.read_text(), "")
        self.assertIn(" > work/upper.txt\n", self.log.read_text())
        self.assertNotIn(" < ", self.log.read_text())

    def test_a_failed_command_leaves_neither_the_output_nor_its_temporary_file(self):
        self.output.parent.mkdir()
        self.output.write_text("from an earlier run")
        dag = read(streamed(self.root, HALF))
        _, results = run(dag, state_at(self.root))
        self.assertEqual(results[0].error, "command exited with status 4")
        self.assertFalse(self.output.exists())
        self.assertFalse(partial(dag, "work/upper.txt").exists())
        self.assertEqual(list(self.output.parent.iterdir()), [])

    def test_keep_failed_keeps_what_a_failed_command_wrote_on_its_standard_output(self):
        dag = read(streamed(self.root, HALF))
        _, results = run(dag, state_at(self.root), keep_failed=True)
        self.assertEqual(results[0].error, "command exited with status 4")
        self.assertEqual(partial(dag, "work/upper.txt").read_text(), "half")
        self.assertFalse(self.output.exists())
        self.assertIn("spit-bash: kept failed standard output work/.upper.txt.spit-bash-partial\n",
                      self.log.read_text())

    def test_a_failed_output_check_removes_the_renamed_output(self):
        data = streamed(self.root, UPPER)
        data["jobs"][0]["checks"] = [{
            "when": "after", "check": "fails", "port": "output", "path": "work/upper.txt",
            "command": command("import sys; sys.exit(1)"),
        }]
        _, results = run(read(data), state_at(self.root))
        self.assertIn("check fails on output output work/upper.txt failed", results[0].error)
        self.assertEqual(list(self.output.parent.iterdir()), [])

    def test_cancelling_removes_the_temporary_file(self):
        started = self.root / "started"
        dag = read(streamed(self.root, WAIT, str(started)))
        state = state_at(self.root)

        async def cancel_once_started():
            task = asyncio.ensure_future(execute(dag, plan(dag, state), state, 1, self.root / "logs"))
            while not started.exists():
                await asyncio.sleep(0.01)
            self.assertEqual(partial(dag, "work/upper.txt").read_text(), "half")
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        asyncio.run(cancel_once_started())
        state.close()
        self.assertEqual(list(self.output.parent.iterdir()), [])

    def test_stdin_and_stdout_must_name_the_jobs_own_files(self):
        data = streamed(self.root, UPPER, stdin="work/upper.txt")
        with self.assertRaisesRegex(DagError, "`stdin` names work/upper.txt, which is not one of its inputs"):
            read(data)
        data = streamed(self.root, UPPER)
        data["jobs"][0]["commands"][0]["stdout"] = "source.txt"
        with self.assertRaisesRegex(DagError, "`stdout` names source.txt, which is not one of its outputs"):
            read(data)
        data = streamed(self.root, UPPER)
        data["jobs"][0]["outputs"]["output"]["kind"] = "folder"
        with self.assertRaisesRegex(DagError, "`stdout` names work/upper.txt, which is a folder"):
            read(data)
        data = streamed(self.root, UPPER)
        data["jobs"][0]["commands"].append(dict(data["jobs"][0]["commands"][0], stdin=None))
        with self.assertRaisesRegex(DagError, "more than one of its commands has a `stdout`"):
            read(data)
        data = streamed(self.root, UPPER)
        del data["jobs"][0]["commands"][0]["stdout"]
        with self.assertRaisesRegex(DagError, "stdout"):
            read(data)

    def test_reads_only_version_8(self):
        data = streamed(self.root, UPPER)
        data["version"] = 7
        with self.assertRaisesRegex(DagError, "spit-bash reads version 8 of the DAG format, not 7"):
            read(data)


if __name__ == "__main__":
    unittest.main()
