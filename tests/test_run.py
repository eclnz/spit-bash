import asyncio
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from spit_bash.dag import load_dag
from spit_bash.plan import plan
from spit_bash.run import execute
from support import FIXTURE, command, document, job, read, run, sample, state_at

# Each job writes its own marker, then waits for the other's: they finish only
# if both run at once.
MEET = (
    "from pathlib import Path; import sys, time\n"
    "Path(sys.argv[2] + '.here').write_text('')\n"
    "other = sys.argv[3]\n"
    "deadline = time.time() + 10\n"
    "while not Path(other).exists() and time.time() < deadline: time.sleep(0.01)\n"
    "Path(sys.argv[2]).write_text(str(Path(other).exists()))\n"
)


class RunTests(unittest.TestCase):
    def test_runs_in_dependency_order_then_skips_and_reruns_stale_jobs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.txt"
            source.write_text("hello")
            data = sample(root)
            dag = read(data)
            decisions, results = run(dag, state_at(root))
            self.assertEqual([d.status for d in decisions], ["run", "run"])
            self.assertFalse(any(r.error for r in results))
            self.assertEqual((root / "final.txt").read_text(), "HELLO!")
            self.assertEqual([d.status for d in plan(dag, state_at(root))], ["skip", "skip"])

            source.write_text("changed")
            decisions, results = run(dag, state_at(root))
            self.assertEqual(decisions[0].reason, "input changed")
            self.assertFalse(any(r.error for r in results))
            self.assertEqual((root / "final.txt").read_text(), "CHANGED!")

            data["jobs"][0]["fingerprint"] = "c" * 16
            decisions = plan(read(data), state_at(root))
            self.assertEqual([d.status for d in decisions], ["run", "run"])
            self.assertEqual(decisions[0].reason, "job fingerprint changed")

    def test_failed_verify_prevents_command_and_dependent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            data = sample(root)
            data["jobs"][0]["verify"] = [command("import sys; print('input is bad'); sys.exit(3)")]
            _, results = run(read(data), state_at(root))
            self.assertIn("verify 1 exited with status 3", results[0].error)
            self.assertIn("dependency 1 failed", results[1].error)
            self.assertFalse((root / "work/upper.txt").exists())
            self.assertIn("input is bad", results[0].log.read_text())

    def test_job_output_goes_to_its_log(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            data = sample(root)
            data["jobs"][0]["command"][2] = ["import sys; print('to stdout'); print('to stderr', file=sys.stderr); " + data["jobs"][0]["command"][2][0]]
            _, results = run(read(data), state_at(root))
            log = (root / ".spit-bash/logs/1-op1.log").read_text()
            self.assertEqual(results[0].log, root / ".spit-bash/logs/1-op1.log")
            self.assertIn("to stdout", log)
            self.assertIn("to stderr", log)
            self.assertIn("$ " + sys.executable, log)

    def test_independent_jobs_run_at_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("")
            jobs = [job(1, ["source.txt"], "a.txt", MEET), job(2, ["source.txt"], "b.txt", MEET)]
            jobs[0]["command"].append(["b.txt.here"])
            jobs[1]["command"].append(["a.txt.here"])
            _, results = run(read(document(root, jobs, ["source.txt"])), state_at(root), workers=2)
            self.assertFalse(any(r.error for r in results))
            self.assertEqual((root / "a.txt").read_text(), "True")
            self.assertEqual((root / "b.txt").read_text(), "True")

    def test_cancelling_stops_the_running_command(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("")
            script = "import os, sys, time; open(sys.argv[2] + '.pid', 'w').write(str(os.getpid())); time.sleep(60)"
            dag = read(document(root, [job(1, ["source.txt"], "out.txt", script)], ["source.txt"]))
            state = state_at(root)
            pid_file = root / "out.txt.pid"

            async def cancel_once_started():
                task = asyncio.ensure_future(execute(dag, plan(dag, state), state, 1, root / "logs"))
                while not pid_file.exists() or not pid_file.read_text():
                    await asyncio.sleep(0.01)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

            asyncio.run(cancel_once_started())
            state.close()
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pid_file.read_text()), 0)
            self.assertEqual(state_at(root).jobs, {})

    @unittest.skipUnless(shutil.which("sort"), "requires sort")
    def test_runs_real_spit_version_four_fixture(self):
        with tempfile.TemporaryDirectory() as directory, FIXTURE.open(encoding="utf-8") as source:
            root = Path(directory)
            for path, content in {
                "input/alpha/01.txt": "z\na\na\n",
                "input/alpha/02.txt": "b\na\n",
                "input/beta/01.txt": "q\np\n",
            }.items():
                file = root / path
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_text(content)
            dag = load_dag(source, directory)
            decisions, results = run(dag, state_at(root), workers=3)
            self.assertEqual([d.status for d in decisions], ["run"] * 5)
            self.assertFalse(any(r.error for r in results))
            self.assertEqual((root / "merged/group=alpha.txt").read_text(), "a\nb\nz\n")
            self.assertEqual((root / "merged/group=beta.txt").read_text(), "p\nq\n")
            self.assertEqual([d.status for d in plan(dag, state_at(root))], ["skip"] * 5)


class SignalTests(unittest.TestCase):
    def test_sigterm_stops_running_jobs_and_exits_143(self):
        self.stop_with(signal.SIGTERM, 143, "terminated")

    def test_sigint_stops_running_jobs_and_exits_130(self):
        self.stop_with(signal.SIGINT, 130, "interrupted")

    def stop_with(self, signum, code, message):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("")
            script = "import os, sys, time; open(sys.argv[2] + '.pid', 'w').write(str(os.getpid())); time.sleep(60)"
            data = document(root, [job(1, ["source.txt"], "out.txt", script)], ["source.txt"])
            (root / "dag.json").write_text(json.dumps(data))
            env = dict(os.environ, PYTHONPATH=str(Path(__file__).parents[1] / "src"))
            process = subprocess.Popen(
                [sys.executable, "-m", "spit_bash", "run", str(root / "dag.json")],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
            )
            pid_file = root / "out.txt.pid"
            deadline = time.time() + 10
            while (not pid_file.exists() or not pid_file.read_text()) and time.time() < deadline:
                time.sleep(0.01)
            process.send_signal(signum)
            _, stderr = process.communicate(timeout=10)
            self.assertEqual(process.returncode, code, stderr)
            self.assertIn(message, stderr)
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pid_file.read_text()), 0)


if __name__ == "__main__":
    unittest.main()
