"""Job logs, with and without the time the runner received each line."""

import asyncio
import re
import tempfile
import unittest
from pathlib import Path

from spit_bash.plan import plan
from spit_bash.run import execute
from support import command, document, job_entry, read, state_at

STAMP = re.compile(rb"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}[+-]\d\d:\d\d ")
# Writes to both streams, the last line with no newline.
NOISY = ("import sys; print('to stdout'); print('to stderr', file=sys.stderr); sys.stdout.flush();"
         " sys.stderr.write('unfinished'); open(sys.argv[2], 'w').write('made')")


def execute_all(root, data, timestamps, **options):
    dag = read(data)
    state = state_at(root)
    results = asyncio.run(execute(dag, plan(dag, state), state, 2, root / "logs", timestamp_logs=timestamps, **options))
    state.close()
    return results


def strip(line):
    return STAMP.sub(b"", line)


class LogTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source.txt").write_text("in")

    def one(self, script=NOISY):
        return document(self.root, [job_entry(1, ["source.txt"], "out.txt", script)], ["source.txt"])

    def log(self, name="1-op1.log"):
        return (self.root / "logs" / name).read_bytes()

    def test_every_line_starts_with_a_date_time_and_offset(self):
        self.assertEqual([r.error for r in execute_all(self.root, self.one(), True)], [None])
        lines = self.log().splitlines()
        self.assertTrue(all(STAMP.match(line) for line in lines), lines)
        text = [strip(line) for line in lines]
        self.assertIn(b"to stdout", text)
        self.assertIn(b"to stderr", text)
        self.assertTrue(text[0].startswith(b"$ "))

    def test_a_last_line_with_no_newline_is_kept_and_ended(self):
        execute_all(self.root, self.one(), True)
        self.assertTrue(self.log().endswith(b"unfinished\n"))
        self.assertTrue(STAMP.match(self.log().splitlines()[-1]))

    def test_without_the_flag_the_log_has_no_times(self):
        execute_all(self.root, self.one(), False)
        lines = self.log().splitlines()
        self.assertFalse(any(STAMP.match(line) for line in lines))
        self.assertIn(b"to stdout", lines)

    def test_several_commands_and_a_failure_are_all_stamped(self):
        data = self.one()
        data["jobs"][0]["commands"].append(
            {"command": command("import sys; print('second'); sys.exit(3)"), "stdin": None, "stdout": None})
        results = execute_all(self.root, data, True)
        self.assertIn("exited with status 3", results[0].error)
        lines = self.log().splitlines()
        self.assertTrue(all(STAMP.match(line) for line in lines), lines)
        self.assertEqual(sum(strip(line).startswith(b"$ ") for line in lines), 2)
        self.assertIn(b"second", [strip(line) for line in lines])

    def test_a_check_is_stamped(self):
        data = self.one()
        data["jobs"][0]["checks"] = [{
            "when": "after", "check": "says", "port": "output", "path": "out.txt",
            "command": command("print('checking')"),
        }]
        execute_all(self.root, data, True)
        lines = self.log().splitlines()
        self.assertTrue(all(STAMP.match(line) for line in lines), lines)
        self.assertIn(b"checking", [strip(line) for line in lines])

    def test_concurrent_jobs_keep_their_own_lines(self):
        jobs = [job_entry(i, ["source.txt"], f"out{i}.txt", f"import time; print('job {i}'); time.sleep(0.2);"
                                                      f" open(__import__('sys').argv[2], 'w').write('x')")
                for i in (1, 2)]
        execute_all(self.root, document(self.root, jobs, ["source.txt"]), True)
        for i in (1, 2):
            lines = self.log(f"{i}-op{i}.log").splitlines()
            self.assertTrue(all(STAMP.match(line) for line in lines), lines)
            self.assertIn(f"job {i}".encode(), [strip(line) for line in lines])
            self.assertNotIn(f"job {3 - i}".encode(), b" ".join(lines))

    def test_output_written_to_an_artifact_is_not_stamped(self):
        data = self.one("import sys; print('data line'); print('note', file=sys.stderr)")
        data["jobs"][0]["commands"][0] = {"command": command(
            "import sys; print('data line'); print('note', file=sys.stderr)", "source.txt"),
            "stdin": None, "stdout": "out.txt"}
        self.assertEqual([r.error for r in execute_all(self.root, data, True)], [None])
        self.assertEqual((self.root / "out.txt").read_bytes(), b"data line\n")
        self.assertIn(b"note", [strip(line) for line in self.log().splitlines()])

    def test_blank_lines_are_kept(self):
        script = ("import sys; sys.stdout.write('a\\n\\n\\nb\\n'); sys.stdout.flush(); open(sys.argv[2], 'w').write('x')")
        execute_all(self.root, self.one(script), True)
        output = [strip(line) for line in self.log().splitlines() if not strip(line).startswith(b"$ ")]
        self.assertEqual(output, [b"a", b"", b"", b"b"])

    def test_a_very_long_line_keeps_all_its_text(self):
        script = "import sys; sys.stdout.write('a' * 300000); open(sys.argv[2], 'w').write('x')"
        execute_all(self.root, self.one(script), True)
        lines = self.log().splitlines()
        self.assertTrue(all(STAMP.match(line) for line in lines))
        output = [strip(line) for line in lines if not strip(line).startswith(b"$ ")]
        self.assertEqual(sum(line.count(b"a") for line in output), 300000)

    def test_cancelling_keeps_what_was_logged_so_far(self):
        script = ("import sys, time; print('started', flush=True); open(sys.argv[2] + '.ready', 'w').write('1');"
                  " time.sleep(60)")
        data = self.one(script)
        dag = read(data)
        state = state_at(self.root)
        ready = self.root / "out.txt.ready"

        async def cancel_once_started():
            task = asyncio.ensure_future(execute(dag, plan(dag, state), state, 1, self.root / "logs",
                                                 timestamp_logs=True))
            while not ready.exists():
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        asyncio.run(cancel_once_started())
        state.close()
        lines = self.log().splitlines()
        self.assertTrue(all(STAMP.match(line) for line in lines), lines)
        self.assertIn(b"started", [strip(line) for line in lines])


if __name__ == "__main__":
    unittest.main()
