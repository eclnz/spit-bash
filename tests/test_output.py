"""What `plan` and `run` print: causes once, counts for the rest.

Each test of `OutputTests` builds a small DAG and states the text it is about.
`StoredOutputTests` keeps the whole of what each command prints over a set of
scenarios in tests/fixtures/output, so that a change to the code shows every
change to the output.
"""

import contextlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
import unittest
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

from spit_bash.cli import main
from support import EXCLAIM, artifact, command, document, job, sample, UPPER

MISSING = "no-such-spit-bash-program"
FAILS = "import sys; print('boom'); sys.exit(3)"


def call(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), \
            mock.patch.object(sys, "stdin", io.StringIO("")):
        code = main(list(argv))
    return code, out.getvalue(), err.getvalue()


def named(item, operation):
    item["operation"] = operation
    return item


class OutputTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        (self.root / "source.txt").write_text("hello")
        self.dag = self.root / "jobs.spitdag"

    def write(self, data):
        self.dag.write_text(json.dumps(data))
        return str(self.dag)

    def blocked_chain(self, roots=2):
        """`roots` jobs that need a program that is missing, one job after each, and two that can start."""
        jobs, inputs = [], ["source.txt"]
        for n in range(roots):
            root, after = 10 + 2 * n, 11 + 2 * n
            inputs.append(f"in{n}.txt")
            (self.root / f"in{n}.txt").write_text("x")
            blocked = named(job(root, [f"in{n}.txt"], f"made{n}.txt", UPPER, dependents=[after]), "tool")
            blocked["commands"][0]["command"][0] = [MISSING]
            jobs += [blocked, named(job(after, [f"made{n}.txt"], f"after{n}.txt", UPPER, depends_on=[root]), "after")]
        jobs += [named(job(1, ["source.txt"], "a.txt", UPPER), "ok"), named(job(2, ["source.txt"], "b.txt", UPPER), "ok")]
        data = document(self.root, sorted(jobs, key=lambda item: item["id"]), inputs)
        data["executables"] = [MISSING]
        return data

    # Plan.

    def test_a_plan_with_nothing_to_do_is_one_line(self):
        data = sample(self.root)
        call("run", self.write(data))
        self.assertEqual(call("plan", str(self.dag))[1], "all 2 jobs are current\n")

    def test_a_first_plan_is_counts_only(self):
        code, out, _ = call("plan", self.write(sample(self.root)))
        self.assertEqual((code, out), (0, "1 job can start, 1 job will wait for it\n"))

    def test_a_blocked_cause_is_listed_once_with_how_many_jobs_depend_on_it(self):
        code, out, _ = call("plan", self.write(self.blocked_chain()))
        self.assertEqual(code, 1)
        self.assertEqual(out, "2 jobs can start, 4 jobs are blocked\n"
                              f"blocked: tool x2: program not found: {MISSING} (2 more depend on them)\n")

    def test_verbose_plan_lists_every_job_in_the_same_words(self):
        code, out, _ = call("plan", self.write(sample(self.root)), "--verbose")
        self.assertEqual(out.splitlines()[-2:], ["[1] ready   op1: output missing", "[2] waits   op2: waits for 1"])

    def test_plan_json_names_the_root_cause_and_what_it_holds_up(self):
        _, out, _ = call("plan", self.write(self.blocked_chain(1)), "--json")
        jobs = {item["id"]: item for item in json.loads(out)["jobs"]}
        self.assertEqual((jobs[10]["caused_by"], jobs[10]["holds_up"]), (None, 1))
        self.assertEqual((jobs[11]["caused_by"], jobs[11]["holds_up"]), (10, 0))

    # Run.

    def test_a_failure_is_reported_once_with_what_it_held_up(self):
        data = sample(self.root)
        data["jobs"][0]["commands"][0]["command"][2] = [FAILS]
        code, _, err = call("run", self.write(data))
        self.assertEqual(code, 1)
        self.assertIn("[1] failed: command exited with status 3", err)
        self.assertIn("[1] held up 1 other job", err)
        self.assertNotIn("[2] failed", err)
        self.assertIn("0 done, 0 checked, 1 failed, 1 not run (an upstream job failed), 0 current, 0 blocked", err)

    def test_identical_failures_are_grouped(self):
        jobs = [named(job(n, ["source.txt"], f"out{n}.txt", FAILS), "import") for n in (1, 2, 3)]
        code, _, err = call("run", self.write(document(self.root, jobs, ["source.txt"])))
        self.assertEqual(err.count("last lines of"), 1)
        self.assertIn("[1] the same failure in 2 other jobs: 2, 3", err)
        self.assertIn("3 failed", err)

    def test_verbose_run_says_why_a_job_did_not_run(self):
        data = sample(self.root)
        data["jobs"][0]["commands"][0]["command"][2] = [FAILS]
        _, _, err = call("run", self.write(data), "--verbose")
        self.assertIn("[2] not run: job 1 failed", err)

    def test_a_job_below_a_blocked_one_is_never_started(self):
        data = self.blocked_chain(1)
        marker = self.root / "started.txt"
        after = next(item for item in data["jobs"] if item["id"] == 11)
        after["commands"][0]["command"] = command(
            f"from pathlib import Path; Path({str(marker)!r}).write_text('x')", "made0.txt", "after0.txt")
        call("run", self.write(data), "--partial")
        self.assertFalse(marker.exists())
        self.assertTrue((self.root / "a.txt").exists())

    def test_run_starts_nothing_while_a_job_is_blocked(self):
        code, out, err = call("run", self.write(self.blocked_chain()))
        self.assertEqual(code, 1)
        self.assertEqual(out, "2 jobs can start, 4 jobs are blocked\n"
                              f"blocked: tool x2: program not found: {MISSING} (2 more depend on them)\n")
        self.assertEqual(err, "nothing was run: 4 jobs are blocked. Fix the cause, or pass --partial "
                              "to run the jobs that are not.\n")
        self.assertFalse((self.root / "a.txt").exists())
        self.assertFalse((self.root / ".spit-bash/logs").exists())



# Stored outputs: what each command prints, byte for byte, for a set of scenarios.
#
# Each scenario is built afresh for each command, so every command starts from the
# same files and state. Commands run through `main` with one core, which fixes the
# order of the progress lines. The stored text replaces only what differs from one
# run to the next: the temporary root, modification times and durations.

STORED = Path(__file__).parent / "fixtures" / "output"
BLESS = os.environ.get("SPIT_BASH_BLESS") == "1"
# A program found by name, rather than `sys.executable`, whose path differs between
# machines and is part of each job's fingerprint, which `plan --job` prints.
PYTHON = "python3"
DURATION = r"(?:under 1s|about \d+(?:s|m|h\d\dm)|\d+(?:s|m|h\d\dm))"
VARYING = [
    (re.compile(rf"(?<= in ){DURATION}(?=;)"), "<time>"),
    (re.compile(rf"(?<=, ){DURATION}$", re.M), "<time>"),
    (re.compile(r"(?<=mtime_ns )\d+"), "<mtime>"),
]
HOLDS = "from pathlib import Path; import sys; sys.exit(Path(sys.argv[1]).read_text() != sys.argv[2])"


def portable(data):
    """`data` with every command and check started by `PYTHON`."""
    for item in data["jobs"]:
        for entry in [*item["commands"], *item["checks"]]:
            if entry["command"][0] == [sys.executable]:
                entry["command"][0] = [PYTHON]
    return data


def edit(path, text):
    """Write `text` to `path` with a later modification time, as an edit an instant after a run would have."""
    later = path.stat().st_mtime_ns + 10_000_000_000
    path.write_text(text)
    os.utime(path, ns=(later, later))


def chain(root):
    """`source.txt -> work/upper.txt -> final.txt -> last.txt`"""
    data = sample(root)
    data["jobs"][1]["dependents"] = [3]
    data["jobs"].append(job(3, ["final.txt"], "last.txt", EXCLAIM, depends_on=[2]))
    return data


def blocked(root):
    """A job missing its external input and one missing its program, each with a job after it, and one that can start."""
    missing = named(job(3, ["source.txt"], "tool.txt", UPPER, dependents=[4]), "tool")
    missing["commands"][0]["command"][0] = [MISSING]
    data = document(root, [
        named(job(1, ["absent.txt"], "read.txt", UPPER, dependents=[2]), "reader"),
        named(job(2, ["read.txt"], "after_read.txt", UPPER, depends_on=[1]), "after"),
        missing,
        named(job(4, ["tool.txt"], "after_tool.txt", UPPER, depends_on=[3]), "after"),
        named(job(5, ["source.txt"], "fine.txt", UPPER), "fine"),
    ], ["source.txt", "absent.txt"])
    data["executables"] = [MISSING]
    return data


def failing(root):
    """The first job fails, so the job after it does not run."""
    data = sample(root)
    data["jobs"][0]["commands"][0]["command"][2] = [FAILS]
    return data


def edit_source(root, data):
    """An edit that upper-casing undoes, so the jobs below the first may run and are cut off."""
    edit(root / "source.txt", "Hello")


def add_check(root, data):
    """A check on a current job's output, which runs the check and not the command."""
    data["jobs"][0]["checks"] = [{
        "when": "after", "check": "holds(HELLO)", "port": "output", "path": "work/upper.txt",
        "command": [[PYTHON], ["-c"], [HOLDS], [{"path": "work/upper.txt"}], ["HELLO"]],
    }]


def move_output(root, data):
    """A new path for the first job's output, which keeps its product and entities."""
    for item in data["jobs"]:
        for artifact in [*item["inputs"]["input"], item["outputs"]["output"]]:
            if artifact["path"] == "work/upper.txt":
                artifact["path"] = "moved/upper.txt"
        item["commands"][0]["command"] = [
            [{"path": "moved/upper.txt"}] if part == [{"path": "work/upper.txt"}] else part
            for part in item["commands"][0]["command"]]


RUN = ("run", "--cores", "1")


@dataclass(frozen=True)
class Scenario:
    """Files and state to start from, and what to print from them."""

    build: Callable[[Path], dict]
    # Whether a run comes first, and what changes after it.
    ran: bool
    change: Callable[[Path, dict], None] | None
    # The job `plan --job` explains.
    explained: int
    # Flags every command takes.
    flags: tuple[str, ...] = ()
    # Commands besides `plan` and `run` with and without `--verbose`.
    extra: tuple[tuple[str, ...], ...] = ()

    def commands(self) -> list[tuple[str, ...]]:
        # `run` takes one core, so that its progress lines come in one order.
        return [("plan",), ("plan", "--verbose"), ("plan", "--json"), ("plan", "--job", str(self.explained)),
                RUN, (*RUN, "--verbose"), *self.extra]


SCENARIOS = {
    "first_run": Scenario(sample, False, None, 2),
    "current": Scenario(sample, True, None, 2),
    "blocked": Scenario(blocked, False, None, 4, extra=((*RUN, "--partial"), (*RUN, "--verbose", "--partial"))),
    "failed": Scenario(failing, False, None, 1),
    "cutoff": Scenario(chain, True, edit_source, 2),
    "checks_only": Scenario(sample, True, add_check, 1),
    "moved": Scenario(sample, True, move_output, 2, ("--move-files",)),
}


def scenario_output(scenario):
    """What each command prints, from a fresh copy of the scenario."""
    sections = []
    for command in scenario.commands():
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / "source.txt").write_text("hello")
            data = scenario.build(root)
            dag = root / "jobs.spitdag"
            dag.write_text(json.dumps(portable(data)))
            if scenario.ran:
                code, _, err = call("run", str(dag), "--cores", "1")
                assert code == 0, err
            if scenario.change is not None:
                scenario.change(root, data)
                dag.write_text(json.dumps(portable(data)))
            argv = [command[0], "jobs.spitdag", *command[1:], *scenario.flags]
            code, out, err = call(*[str(dag) if part == "jobs.spitdag" else part for part in argv])
            sections.append(f"$ spit-bash {' '.join(argv)}\nexit {code}\n"
                            f"--- stdout\n{normalise(out, root, directory)}"
                            f"--- stderr\n{normalise(err, root, directory)}")
    return "\n".join(sections)


def normalise(text, root, directory):
    for path in (str(root), directory):
        text = text.replace(path, "<root>")
    for pattern, stand_in in VARYING:
        text = pattern.sub(stand_in, text)
    return text


@unittest.skipUnless(shutil.which(PYTHON), f"the stored outputs need {PYTHON} on PATH")
class StoredOutputTests(unittest.TestCase):
    """Each scenario's output, compared with tests/fixtures/output/<scenario>.txt.

    Set SPIT_BASH_BLESS=1 to save the outputs as they are, then read the diff.
    """

    maxDiff = None

    def test_scenarios(self):
        for name in SCENARIOS:
            with self.subTest(name):
                text = scenario_output(SCENARIOS[name])
                path = STORED / f"{name}.txt"
                if BLESS:
                    STORED.mkdir(exist_ok=True)
                    path.write_text(text)
                    continue
                self.assertTrue(path.exists(), f"no stored output {path}; run with SPIT_BASH_BLESS=1")
                self.assertEqual(text, path.read_text())


if __name__ == "__main__":
    unittest.main()
