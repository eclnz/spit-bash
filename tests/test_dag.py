import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import DagError, load_dag
from support import FIXTURE, read, sample


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

    def test_reads_real_spit_version_seven_fixture(self):
        with tempfile.TemporaryDirectory() as directory, FIXTURE.open(encoding="utf-8") as source:
            dag = load_dag(source, directory)
            self.assertEqual(len(dag.jobs), 5)
            self.assertEqual(dag.jobs[3].depends_on, (1, 2))
            self.assertEqual(dag.jobs[3].command[4], "merged/group=alpha.txt")
            self.assertEqual(dag.executables, ("sort",))


    def test_checks_the_references_of_calls_and_origins(self):
        call = {"operation": "tidy", "instance": "tidied", "parent": None, "file": 0, "at": {"file": 0, "line": 13}}
        files = [{"path": "act.spit", "blob": "9f2c0e5d1c4b7a3e8f6d2b1a0c9e8d7f6a5b4c3d"}]
        with tempfile.TemporaryDirectory() as directory:
            def made(calls, origin=None):
                data = sample(Path(directory))
                data["pipeline_files"], data["calls"] = files, calls
                data["jobs"][0]["origin"] = origin
                return data

            self.assertEqual(len(read(made([call], {"call": 0, "line": 10})).jobs), 2)
            with self.assertRaisesRegex(DagError, "origin names a call"):
                read(made([call], {"call": 1, "line": 10}))
            with self.assertRaisesRegex(DagError, "not in `pipeline_files`"):
                read(made([dict(call, file=1)]))
            with self.assertRaisesRegex(DagError, "earlier call"):
                read(made([dict(call, parent=0)]))

    def test_only_version_seven_is_read_and_its_fields_are_required(self):
        with tempfile.TemporaryDirectory() as directory:
            for field in ("pipeline_files", "calls"):
                data = sample(Path(directory))
                del data[field]
                with self.assertRaisesRegex(DagError, field):
                    read(data)
            data = sample(Path(directory))
            del data["jobs"][1]["origin"]
            with self.assertRaisesRegex(DagError, "origin"):
                read(data)
            data = sample(Path(directory))
            del data["jobs"][1]["checks"]
            with self.assertRaisesRegex(DagError, "checks"):
                read(data)
            data = sample(Path(directory))
            data["version"] = 6
            with self.assertRaisesRegex(DagError, "version"):
                read(data)

if __name__ == "__main__":
    unittest.main()
