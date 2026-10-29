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
from unittest import mock

from spit_bash.dag import load_dag
from spit_bash.plan import Status, plan
from spit_bash import run as run_module
from spit_bash.run import execute
from support import FIXTURE, artifact, command, document, job, read, run, sample, state_at

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
            self.assertEqual([d.status for d in decisions], [Status.RUN, Status.RUN])
            self.assertFalse(any(r.error for r in results))
            self.assertEqual((root / "final.txt").read_text(), "HELLO!")
            self.assertEqual([d.status for d in plan(dag, state_at(root))], [Status.SKIP, Status.SKIP])

            source.write_text("changed")
            decisions, results = run(dag, state_at(root))
            self.assertEqual(decisions[0].reason, "input changed")
            self.assertFalse(any(r.error for r in results))
            self.assertEqual((root / "final.txt").read_text(), "CHANGED!")

            data["jobs"][0]["fingerprint"] = "c" * 16
            decisions = plan(read(data), state_at(root))
            self.assertEqual([d.status for d in decisions], [Status.RUN, Status.MAYBE])
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

    def test_a_missing_declared_output_fails_the_job_and_names_it(self):
        # As `dcm2niix -b n` does: the command exits 0 but writes no `.json`.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            data = sample(root)
            convert = data["jobs"][0]
            convert["outputs"] = {"image": artifact("work/upper.txt"), "meta": artifact("work/upper.json")}
            data["jobs"][1]["inputs"]["input"] = [artifact("work/upper.txt")]
            _, results = run(read(data), state_at(root))
            self.assertEqual(results[0].error, "command succeeded but did not create output meta work/upper.json")
            self.assertIn("dependency 1 failed", results[1].error)
            self.assertFalse((root / "final.txt").exists())
            # No success was recorded, so the next plan runs the job again.
            decisions = plan(read(data), state_at(root))
            self.assertEqual([d.status for d in decisions], [Status.RUN, Status.RUN])

    def test_multiple_outputs_report_each_missing_port_and_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            data = sample(root)
            data["jobs"][0]["outputs"] = {
                "image": artifact("work/upper.txt"),
                "meta": artifact("work/upper.json"),
                "log": artifact("work/conversion.log"),
            }
            dag = read(data)
            self.assertEqual(
                [(output.port, output.path) for output in dag.jobs[0].produced],
                [("image", "work/upper.txt"), ("meta", "work/upper.json"),
                 ("log", "work/conversion.log")],
            )
            _, results = run(dag, state_at(root))
            self.assertEqual(
                results[0].error,
                "command succeeded but did not create output meta work/upper.json, "
                "output log work/conversion.log",
            )
            # The output the command did write is removed with the failure.
            self.assertFalse((root / "work/upper.txt").exists())
            self.assertIn("removed failed output work/upper.txt", results[0].log.read_text())

    def test_a_failed_command_leaves_no_partial_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            data = sample(root)
            data["jobs"][0]["command"][2] = [
                "from pathlib import Path; import sys; Path(sys.argv[2]).write_text('HAL'); sys.exit(4)"
            ]
            _, results = run(read(data), state_at(root))
            self.assertEqual(results[0].error, "command exited with status 4")
            self.assertFalse((root / "work/upper.txt").exists())
            log = results[0].log.read_text()
            self.assertTrue(log.endswith("spit-bash: removed failed output work/upper.txt\n"), log)

    def test_keep_failed_leaves_a_failed_commands_outputs_and_reruns_the_job(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            data = sample(root)
            data["jobs"][0]["command"][2] = [
                "from pathlib import Path; import sys; Path(sys.argv[2]).write_text('HAL'); sys.exit(4)"
            ]
            dag = read(data)
            _, results = run(dag, state_at(root), keep_failed=True)
            self.assertEqual(results[0].error, "command exited with status 4")
            self.assertEqual((root / "work/upper.txt").read_text(), "HAL")
            log = results[0].log.read_text()
            self.assertTrue(log.endswith("spit-bash: kept failed output work/upper.txt\n"), log)
            # The kept output is not a finished one, so the next run runs the job again.
            decisions, _ = run(dag, state_at(root))
            self.assertEqual(decisions[0].status, Status.RUN)

    def test_a_failure_before_the_command_keeps_an_earlier_runs_outputs(self):
        # The command never ran, so the files are still the last run's, whole.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            data = sample(root)
            run(read(data), state_at(root))
            data["jobs"][0]["fingerprint"] = "c" * 16
            data["jobs"][0]["verify"] = [command("import sys; sys.exit(3)")]
            _, results = run(read(data), state_at(root))
            self.assertEqual(results[0].error, "verify 1 exited with status 3")
            self.assertEqual((root / "work/upper.txt").read_text(), "HELLO")
            self.assertNotIn("removed", results[0].log.read_text())

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
            _, results = run(read(document(root, jobs, ["source.txt"])), state_at(root), cores=2)
            self.assertFalse(any(r.error for r in results))
            self.assertEqual((root / "a.txt").read_text(), "True")
            self.assertEqual((root / "b.txt").read_text(), "True")

    def test_cancelling_stops_the_running_command(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("")
            script = (
                "import os, sys, time\n"
                "open(sys.argv[2], 'w').write('half')\n"
                "open(sys.argv[2] + '.pid', 'w').write(str(os.getpid()))\n"
                "time.sleep(60)\n"
            )
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
            # The half-written output went with the stopped command.
            self.assertFalse((root / "out.txt").exists())
            self.assertIn("removed failed output out.txt", (root / "logs/1-op1.log").read_text())

    def test_cancelling_kills_a_command_that_ignores_sigterm(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("")
            # The command and a child it starts both ignore SIGTERM, which the
            # child inherits, so only SIGKILL stops either.
            script = (
                "import os, signal, subprocess, sys, time\n"
                "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
                "open(sys.argv[2] + '.pid', 'w').write(f'{os.getpid()} {child.pid}')\n"
                "time.sleep(60)\n"
            )
            dag = read(document(root, [job(1, ["source.txt"], "out.txt", script)], ["source.txt"]))
            state = state_at(root)
            pid_file = root / "out.txt.pid"

            async def cancel_once_started():
                task = asyncio.ensure_future(execute(dag, plan(dag, state), state, 1, root / "logs"))
                while not pid_file.exists() or not pid_file.read_text():
                    await asyncio.sleep(0.01)
                started = time.monotonic()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                return time.monotonic() - started

            with mock.patch.object(run_module, "STOP_GRACE_SECONDS", 0.2):
                elapsed = asyncio.run(cancel_once_started())
            state.close()
            self.assertLess(elapsed, 5)
            for pid in pid_file.read_text().split():
                deadline = time.time() + 5
                while time.time() < deadline:
                    try:
                        os.kill(int(pid), 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.01)
                else:
                    self.fail(f"process {pid} outlived the cancelled run")
            self.assertEqual(state_at(root).jobs, {})

    @unittest.skipUnless(shutil.which("sort"), "requires sort")
    def test_runs_real_spit_fixture(self):
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
            decisions, results = run(dag, state_at(root), cores=3)
            self.assertEqual([d.status for d in decisions], [Status.RUN] * 5)
            self.assertFalse(any(r.error for r in results))
            self.assertEqual((root / "merged/group=alpha.txt").read_text(), "a\nb\nz\n")
            self.assertEqual((root / "merged/group=beta.txt").read_text(), "p\nq\n")
            self.assertEqual([d.status for d in plan(dag, state_at(root))], [Status.SKIP] * 5)


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
