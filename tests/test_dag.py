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

    def test_reads_real_spit_version_four_fixture(self):
        with tempfile.TemporaryDirectory() as directory, FIXTURE.open(encoding="utf-8") as source:
            dag = load_dag(source, directory)
            self.assertEqual(len(dag.jobs), 5)
            self.assertEqual(dag.jobs[3].depends_on, (1, 2))
            self.assertEqual(dag.jobs[3].command[4], "merged/group=alpha.txt")
            self.assertEqual(dag.executables, ("sort",))


if __name__ == "__main__":
    unittest.main()
