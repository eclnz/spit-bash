import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import DagError, load_dag
from support import FIXTURE, as_version_seven, read, sample


class LoadTests(unittest.TestCase):
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

    def test_pydantic_rejects_missing_and_wrong_typed_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            data = sample(Path(directory))
            del data["jobs"][0]["verify"]
            with self.assertRaisesRegex(DagError, "verify"):
                read(data)
            data = sample(Path(directory))
            data["jobs"][0]["id"] = "1"
            with self.assertRaisesRegex(DagError, "int_type"):
                read(data)
            data = sample(Path(directory))
            data["jobs"][0]["command"][0] = [{"unknown": "program"}]
            with self.assertRaisesRegex(DagError, "unknown"):
                read(data)

    def test_requires_a_root(self):
        with tempfile.TemporaryDirectory() as directory:
            data = sample(Path(directory))
            data["root"] = None
            with self.assertRaisesRegex(DagError, "pass --root"):
                read(data)
            self.assertEqual(read(data, directory).root, Path(directory).resolve())

    def test_reads_real_spit_version_four_fixture(self):
        with tempfile.TemporaryDirectory() as directory, FIXTURE.open(encoding="utf-8") as source:
            dag = load_dag(source, directory)
            self.assertEqual(len(dag.jobs), 5)
            self.assertEqual(dag.jobs[3].depends_on, (1, 2))
            self.assertEqual(dag.jobs[3].command[4], "merged/group=alpha.txt")
            self.assertEqual(dag.executables, ("sort",))


    def test_reads_version_seven_origins_and_checks_their_references(self):
        call = {"operation": "tidy", "instance": "tidied", "parent": None, "file": 0, "at": {"file": 0, "line": 13}}
        with tempfile.TemporaryDirectory() as directory:
            data = as_version_seven(sample(Path(directory)), [call])
            data["jobs"][0]["origin"] = {"call": 0, "line": 10}
            self.assertEqual(len(read(data).jobs), 2)

            bad = as_version_seven(sample(Path(directory)), [call])
            bad["jobs"][0]["origin"] = {"call": 1, "line": 10}
            with self.assertRaisesRegex(DagError, "origin names a call"):
                read(bad)
            bad = as_version_seven(sample(Path(directory)), [dict(call, file=1)])
            with self.assertRaisesRegex(DagError, "not in `pipeline_files`"):
                read(bad)
            bad = as_version_seven(sample(Path(directory)), [dict(call, parent=0)])
            with self.assertRaisesRegex(DagError, "earlier call"):
                read(bad)

    def test_version_seven_fields_are_required_there_and_only_there(self):
        with tempfile.TemporaryDirectory() as directory:
            data = as_version_seven(sample(Path(directory)))
            del data["calls"]
            with self.assertRaisesRegex(DagError, "has `pipeline_files` and `calls`"):
                read(data)
            data = as_version_seven(sample(Path(directory)))
            del data["jobs"][1]["origin"]
            with self.assertRaisesRegex(DagError, "every job of one has it"):
                read(data)
            data = as_version_seven(sample(Path(directory)))
            data["version"] = 6
            with self.assertRaisesRegex(DagError, "came with version 7"):
                read(data)


if __name__ == "__main__":
    unittest.main()
