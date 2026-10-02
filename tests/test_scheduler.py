import asyncio
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import DagError, load_dag
from spit_bash.plan import State, plan
from spit_bash.run import execute


def artifact(path):
    return {"product": path, "entities": {}, "type": None, "path": path}


def command(script, *paths):
    return [[sys.executable], ["-c"], [script], *[[{"path": path}] for path in paths]]


def sample(root):
    first = "from pathlib import Path; import sys; Path(sys.argv[2]).write_text(Path(sys.argv[1]).read_text().upper())"
    second = "from pathlib import Path; import sys; Path(sys.argv[2]).write_text(Path(sys.argv[1]).read_text() + '!')"
    return {
        "version": 4,
        "root": str(root),
        "external_inputs": [artifact("source.txt")],
        "left_out": [],
        "jobs": [
            {
                "id": 1, "operation": "upper", "fingerprint": "a" * 16,
                "inputs": {"source": [artifact("source.txt")]},
                "outputs": {"output": artifact("work/upper.txt")},
                "depends_on": [], "verify": [],
                "command": command(first, "source.txt", "work/upper.txt"),
            },
            {
                "id": 2, "operation": "finish", "fingerprint": "b" * 16,
                "inputs": {"input": [artifact("work/upper.txt")]},
                "outputs": {"output": artifact("final.txt")},
                "depends_on": [1], "verify": [],
                "command": command(second, "work/upper.txt", "final.txt"),
            },
        ],
    }


def read(data, root=None):
    return load_dag(io.StringIO(json.dumps(data)), root)


class SchedulerTests(unittest.TestCase):
    def test_runs_in_dependency_order_then_skips_and_reruns_stale_jobs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.txt"
            source.write_text("hello")
            data = sample(root)
            dag = read(data)
            state = State(root / ".spit-bash" / "state.json")
            decisions = plan(dag, state)
            self.assertEqual([d.status for d in decisions], ["run", "run"])
            self.assertFalse(any(r.error for r in asyncio.run(execute(dag, decisions, state, 2))))
            self.assertEqual((root / "final.txt").read_text(), "HELLO!")
            self.assertEqual([d.status for d in plan(dag, State(state.path))], ["skip", "skip"])

            source.write_text("changed")
            decisions = plan(dag, State(state.path))
            self.assertEqual([d.status for d in decisions], ["run", "run"])
            self.assertEqual(decisions[0].reason, "input changed")
            self.assertFalse(any(r.error for r in asyncio.run(execute(dag, decisions, state, 2))))
            self.assertEqual((root / "final.txt").read_text(), "CHANGED!")

            data["jobs"][0]["fingerprint"] = "c" * 16
            decisions = plan(read(data), State(state.path))
            self.assertEqual([d.status for d in decisions], ["run", "run"])
            self.assertEqual(decisions[0].reason, "job fingerprint changed")

    def test_missing_source_blocks_before_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            dag = read(sample(Path(directory)))
            decisions = plan(dag, State(Path(directory) / "state.json"))
            self.assertEqual([d.status for d in decisions], ["blocked", "blocked"])
            self.assertIn("missing external input", decisions[0].reason)

    def test_failed_verify_prevents_command_and_dependent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            data = sample(root)
            data["jobs"][0]["verify"] = [command("import sys; sys.exit(3)")]
            dag = read(data)
            state = State(root / "state.json")
            results = asyncio.run(execute(dag, plan(dag, state), state, 2))
            self.assertIn("verify 1 exited with status 3", results[0].error)
            self.assertIn("dependency 1 failed", results[1].error)
            self.assertFalse((root / "work/upper.txt").exists())

    def test_job_without_command_requires_existing_current_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("source")
            data = sample(root)
            data["jobs"] = [data["jobs"][0]]
            data["jobs"][0]["command"] = None
            dag = read(data)
            state = State(root / "state.json")
            self.assertEqual(plan(dag, state)[0].status, "blocked")
            output = root / "work/upper.txt"
            output.parent.mkdir()
            output.write_text("prepared")
            self.assertEqual(plan(dag, state)[0].status, "skip")
            source = root / "source.txt"
            source.write_text("new source")
            newer = output.stat().st_mtime_ns + 1_000_000_000
            os.utime(source, ns=(newer, newer))
            self.assertEqual(plan(dag, state)[0].status, "blocked")

    def test_rejects_escaping_path_and_bad_dependencies(self):
        with tempfile.TemporaryDirectory() as directory:
            data = sample(Path(directory))
            data["jobs"][0]["outputs"]["output"]["path"] = "../escape.txt"
            with self.assertRaisesRegex(DagError, "stay within"):
                read(data)
            data = sample(Path(directory))
            data["jobs"][1]["depends_on"] = []
            with self.assertRaisesRegex(DagError, "do not match"):
                read(data)

    def test_reads_real_spit_version_four_fixture(self):
        fixture = Path(__file__).parent / "fixtures" / "command_demo.spitdag"
        with tempfile.TemporaryDirectory() as directory, fixture.open(encoding="utf-8") as source:
            dag = load_dag(source, directory)
            self.assertEqual(len(dag.jobs), 5)
            self.assertEqual(dag.jobs[3].depends_on, (1, 2))
            self.assertEqual(dag.jobs[3].command[4], "merged/group=alpha.txt")
            self.assertEqual([d.status for d in plan(dag, State(Path(directory) / "state.json"))], ["blocked"] * 5)

    @unittest.skipUnless(shutil.which("sort"), "requires sort")
    def test_runs_real_spit_version_four_fixture(self):
        fixture = Path(__file__).parent / "fixtures" / "command_demo.spitdag"
        with tempfile.TemporaryDirectory() as directory, fixture.open(encoding="utf-8") as source:
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
            state = State(root / "state.json")
            decisions = plan(dag, state)
            self.assertEqual([d.status for d in decisions], ["run"] * 5)
            self.assertFalse(any(r.error for r in asyncio.run(execute(dag, decisions, state, 3))))
            self.assertEqual((root / "merged/group=alpha.txt").read_text(), "a\nb\nz\n")
            self.assertEqual((root / "merged/group=beta.txt").read_text(), "p\nq\n")
            self.assertEqual([d.status for d in plan(dag, State(state.path))], ["skip"] * 5)


if __name__ == "__main__":
    unittest.main()
