"""Checks of single artifacts, from DAG version 6: when they run, what a failure does, and reruns."""

import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import DagError
from spit_bash.plan import Status, adopt, job_key, plan
from support import command, read, run, sample, state_at

# A check that passes when its file holds `text`, and fails otherwise.
HOLDS = "from pathlib import Path; import sys; sys.exit(Path(sys.argv[1]).read_text() != sys.argv[2])"


def check(when, port, path, text, name=None):
    return {
        "when": when, "check": name or f"holds({text})", "port": port, "path": path,
        "command": command(HOLDS, path) + [[text]],
    }


def checked(root, first=(), second=()):
    """The sample DAG as version 6, with `first` and `second` as its jobs' checks."""
    data = sample(root)
    data["version"] = 6
    data["jobs"][0]["checks"] = list(first)
    data["jobs"][1]["checks"] = list(second)
    return data


class CheckTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source.txt").write_text("hello")

    def test_checks_run_around_the_command_and_are_recorded(self):
        data = checked(self.root, [check("before", "input", "source.txt", "hello"),
                                   check("after", "output", "work/upper.txt", "HELLO")])
        dag = read(data)
        decisions, results = run(dag, state_at(self.root))
        self.assertEqual([r.error for r in results], [None, None])
        log = (self.root / ".spit-bash/logs/1-op1.log").read_text()
        self.assertLess(log.index("source.txt"), log.index("work/upper.txt"))
        state = state_at(self.root)
        self.assertEqual([d.status for d in plan(dag, state)], [Status.SKIP, Status.SKIP])
        self.assertIsNotNone(state.jobs[job_key(dag.jobs[0])].checks)
        self.assertIsNone(state.jobs[job_key(dag.jobs[1])].checks)

    def test_a_failed_input_check_stops_the_command(self):
        data = checked(self.root, [check("before", "input", "source.txt", "bye")])
        _, results = run(read(data), state_at(self.root))
        self.assertEqual(results[0].error,
                         "check holds(bye) on input input source.txt failed: exited with status 1")
        self.assertEqual(results[1].error, "dependency 1 failed")
        self.assertFalse((self.root / "work/upper.txt").exists())

    def test_a_failed_output_check_fails_a_command_that_succeeded(self):
        data = checked(self.root, [check("after", "output", "work/upper.txt", "nope")])
        _, results = run(read(data), state_at(self.root))
        self.assertIn("check holds(nope) on output output work/upper.txt failed", results[0].error)
        self.assertEqual(results[1].error, "dependency 1 failed")
        self.assertEqual(state_at(self.root).jobs, {})

    def test_a_changed_check_reruns_the_check_not_the_command(self):
        run(read(checked(self.root)), state_at(self.root))
        upper = self.root / "work/upper.txt"
        made = upper.stat().st_mtime_ns
        data = checked(self.root, [check("after", "output", "work/upper.txt", "HELLO")])
        dag = read(data)
        decisions, results = run(dag, state_at(self.root))
        self.assertEqual([d.status for d in decisions], [Status.CHECK, Status.SKIP])
        self.assertEqual(decisions[0].reason, "checks not yet run on these files")
        self.assertEqual([r.error for r in results], [None])
        self.assertEqual(upper.stat().st_mtime_ns, made)
        self.assertEqual([d.status for d in plan(dag, state_at(self.root))], [Status.SKIP, Status.SKIP])

        data = checked(self.root, [check("after", "output", "work/upper.txt", "HELLO", name="other")])
        self.assertEqual(plan(read(data), state_at(self.root))[0].reason, "checks changed")

    def test_a_recheck_that_fails_drops_the_record_so_the_job_reruns(self):
        run(read(checked(self.root)), state_at(self.root))
        data = checked(self.root, [check("after", "output", "work/upper.txt", "nope")])
        dag = read(data)
        decisions, results = run(dag, state_at(self.root))
        self.assertEqual(decisions[0].status, Status.CHECK)
        self.assertIn("failed", results[0].error)
        self.assertEqual(plan(dag, state_at(self.root))[0].status, Status.RUN)

    def test_adopted_jobs_are_checked_on_the_next_run(self):
        run(read(checked(self.root)), state_at(self.root))
        (self.root / ".spit-bash/state.jsonl").unlink()
        dag = read(checked(self.root, [check("after", "output", "work/upper.txt", "HELLO")]))
        state = state_at(self.root)
        adopt(dag, state)
        state.close()
        self.assertEqual(plan(dag, state_at(self.root))[0].status, Status.CHECK)

    def test_a_missing_check_program_blocks_the_job(self):
        broken = check("after", "output", "work/upper.txt", "x")
        broken["command"] = [["no-such-checker-here"], [{"path": "work/upper.txt"}]]
        data = checked(self.root, [broken])
        # SPIT lists every program its commands and checks start with.
        data["executables"] = ["no-such-checker-here"]
        decisions = plan(read(data), state_at(self.root))
        self.assertEqual(decisions[0].status, Status.BLOCKED)
        self.assertEqual(decisions[0].reason, "program not found: no-such-checker-here")

    def test_checks_must_match_the_version_and_their_ports(self):
        data = sample(self.root)
        data["jobs"][0]["checks"] = []
        with self.assertRaisesRegex(DagError, "came with version 6"):
            read(data)
        data = checked(self.root)
        del data["jobs"][1]["checks"]
        with self.assertRaisesRegex(DagError, "came with version 6"):
            read(data)
        data = checked(self.root, [check("after", "output", "source.txt", "x")])
        with self.assertRaisesRegex(DagError, "not on its output output"):
            read(data)


if __name__ == "__main__":
    unittest.main()
