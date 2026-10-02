import os
import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import load_dag
from spit_bash.plan import adopt, plan
from support import FIXTURE, artifact, job, read, run, sample, state_at, UPPER


class PlanTests(unittest.TestCase):
    def test_missing_source_blocks(self):
        with tempfile.TemporaryDirectory() as directory:
            decisions = plan(read(sample(Path(directory))), state_at(directory))
            self.assertEqual([d.status for d in decisions], ["blocked", "blocked"])
            self.assertIn("missing external input", decisions[0].reason)

    def test_fixture_without_inputs_is_blocked(self):
        with tempfile.TemporaryDirectory() as directory, FIXTURE.open(encoding="utf-8") as source:
            dag = load_dag(source, directory)
            self.assertEqual([d.status for d in plan(dag, state_at(directory))], ["blocked"] * 5)

    def test_job_without_command_requires_existing_current_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("source")
            data = sample(root)
            data["jobs"] = [data["jobs"][0]]
            data["jobs"][0]["dependents"] = []
            data["jobs"][0]["command"] = None
            dag = read(data)
            state = state_at(root)
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

    def test_missing_program_blocks_its_job_and_dependents_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            data = sample(root)
            data["jobs"].append(job(3, ["source.txt"], "other.txt", UPPER))
            data["executables"] = ["spit-bash-no-such-program"]
            data["jobs"][0]["command"][0] = ["spit-bash-no-such-program"]
            data["jobs"][1]["verify"] = [[["bin/missing-check"]]]
            decisions = plan(read(data), state_at(root))
            self.assertEqual([d.status for d in decisions], ["blocked", "blocked", "run"])
            self.assertEqual(decisions[0].reason, "program not found: spit-bash-no-such-program")
            self.assertEqual(decisions[1].reason, "dependency blocked: 1")

    def test_program_named_by_a_path_is_found_from_the_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            data = sample(root)
            data["jobs"][1]["verify"] = [[["bin/check"]]]
            self.assertEqual(plan(read(data), state_at(root))[1].reason, "program not found: bin/check")
            (root / "bin").mkdir()
            (root / "bin/check").write_text("#!/bin/sh\n")
            (root / "bin/check").chmod(0o755)
            self.assertEqual([d.status for d in plan(read(data), state_at(root))], ["run", "run"])

    def test_state_survives_moving_the_dataset(self):
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first"
            first.mkdir()
            (first / "source.txt").write_text("hello")
            data = sample(None)
            run(read(data, first), state_at(first))
            moved = Path(directory) / "moved"
            first.rename(moved)
            decisions = plan(read(data, moved), state_at(moved))
            self.assertEqual([d.status for d in decisions], ["skip", "skip"])

    def test_adopt_records_existing_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.txt").write_text("hello")
            (root / "work").mkdir()
            (root / "work/upper.txt").write_text("made elsewhere")
            dag = read(sample(root))
            state = state_at(root)
            self.assertEqual([d.reason for d in plan(dag, state)], ["no successful run recorded", "dependency will run"])
            adoptions = adopt(dag, state)
            state.close()
            self.assertEqual([a.status for a in adoptions], ["adopted", "left"])
            self.assertEqual(adoptions[1].reason, "output missing")
            decisions = plan(dag, state_at(root))
            self.assertEqual([(d.status, d.reason) for d in decisions], [("skip", "current"), ("run", "output missing")])


if __name__ == "__main__":
    unittest.main()
