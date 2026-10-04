"""Where jobs come from, from DAG version 7: the pipeline's files, its calls, and each job's origin."""

import json
import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import DagError
from support import read, sample
from test_cli import call

BLOB = "8f17cbea466ed03fdf66555a6d1de4d645e1d04c"


def called(root):
    """The sample DAG as version 7, with both jobs made by a call nested in another."""
    data = sample(root)
    data["version"] = 7
    data["pipeline_files"] = [{"path": "main.spit", "blob": BLOB}, {"path": "libs/lib.spit", "blob": "0" * 40}]
    data["calls"] = [
        {"operation": "L::summarise", "instance": "m", "parent": None, "file": 1, "at": {"file": 0, "line": 8}},
        {"operation": "L::tidy", "instance": "m::cleaned", "parent": 0, "file": 1,
         "at": {"file": 1, "line": 13}},
    ]
    for item in data["jobs"]:
        item["checks"] = []
        item["origin"] = None
    data["jobs"][0]["origin"] = {"call": 1, "line": 10}
    return data


class OriginTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source.txt").write_text("hello")

    def test_a_job_names_its_step_and_every_call_it_is_nested_in(self):
        dag = read(called(self.root))
        self.assertEqual(dag.jobs[0].origin, (
            "step at libs/lib.spit line 10",
            "in `m::cleaned = L::tidy(...)` at libs/lib.spit line 13",
            "in `m = L::summarise(...)` at main.spit line 8",
        ))
        self.assertEqual(dag.jobs[1].origin, ())

    def test_a_failed_job_says_where_it_comes_from(self):
        data = called(self.root)
        data["jobs"][0]["command"][2] = ["import sys; sys.exit(3)"]
        path = self.root / "jobs.spitdag"
        path.write_text(json.dumps(data))
        code, _, err = call("run", str(path))
        self.assertEqual(code, 1)
        self.assertIn(
            "[1] failed: command exited with status 3\n"
            "[1] step at libs/lib.spit line 10\n"
            "[1] in `m::cleaned = L::tidy(...)` at libs/lib.spit line 13\n"
            "[1] in `m = L::summarise(...)` at main.spit line 8\n",
            err,
        )

    def test_files_and_calls_must_match_the_version_and_each_other(self):
        data = called(self.root)
        del data["calls"]
        with self.assertRaisesRegex(DagError, "came with version 7"):
            read(data)
        data = called(self.root)
        data["version"] = 6
        with self.assertRaisesRegex(DagError, "came with version 7"):
            read(data)
        data = called(self.root)
        data["calls"][0]["at"]["file"] = 2
        with self.assertRaisesRegex(DagError, "names a file"):
            read(data)
        data = called(self.root)
        data["calls"][0]["parent"] = 1
        with self.assertRaisesRegex(DagError, "not an earlier call"):
            read(data)
        data = called(self.root)
        data["jobs"][1]["origin"] = {"call": 2, "line": 1}
        with self.assertRaisesRegex(DagError, "names a call"):
            read(data)
        data = called(self.root)
        data["pipeline_files"][0]["blob"] = "not a blob"
        with self.assertRaisesRegex(DagError, "invalid SPIT DAG"):
            read(data)


if __name__ == "__main__":
    unittest.main()
