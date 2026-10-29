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

    def test_a_link_made_after_loading_cannot_lead_outside_the_root(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            root = Path(directory)
            dag = read(sample(root))
            self.assertEqual(dag.path("work/upper.txt"), root / "work" / "upper.txt")
            (root / "work").symlink_to(outside)
            with self.assertRaisesRegex(DagError, "escapes dataset root"):
                dag.checked("work/upper.txt")

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
            data["jobs"][0]["commands"][0]["command"][0] = [{"unknown": "program"}]
            with self.assertRaisesRegex(DagError, "unknown"):
                read(data)

    def test_reads_cores_and_memory_and_rejects_bad_ones(self):
        with tempfile.TemporaryDirectory() as directory:
            data = sample(Path(directory))
            data["jobs"][1]["cores"], data["jobs"][1]["memory"] = 8, 16 * 2**30
            dag = read(data)
            self.assertEqual([(job.cores, job.memory) for job in dag.jobs], [(1, None), (8, 16 * 2**30)])
            for field, value, message in (("cores", 0, "greater_than_equal"), ("cores", True, "int_type"),
                                          ("cores", None, "int_type"), ("memory", -1, "greater_than_equal"),
                                          ("memory", 2**53 + 1, "less_than_equal"), ("memory", "16G", "int_type")):
                data = sample(Path(directory))
                data["jobs"][0][field] = value
                with self.subTest(field=field, value=value), self.assertRaisesRegex(DagError, message):
                    read(data)
            for field in ("cores", "memory"):
                data = sample(Path(directory))
                del data["jobs"][0][field]
                with self.subTest(missing=field), self.assertRaisesRegex(DagError, field):
                    read(data)

    def test_requires_a_root(self):
        with tempfile.TemporaryDirectory() as directory:
            data = sample(Path(directory))
            data["root"] = None
            with self.assertRaisesRegex(DagError, "pass --root"):
                read(data)
            self.assertEqual(read(data, directory).root, Path(directory).resolve())

    def test_reads_real_spit_fixture(self):
        with tempfile.TemporaryDirectory() as directory, FIXTURE.open(encoding="utf-8") as source:
            dag = load_dag(source, directory)
            self.assertEqual(len(dag.jobs), 5)
            self.assertEqual(dag.jobs[3].depends_on, (1, 2))
            self.assertEqual(dag.jobs[3].commands[0].arguments[4], "merged/group=alpha.txt")
            self.assertEqual(dag.executables, ("sort",))
            self.assertEqual({(job.cores, job.memory) for job in dag.jobs}, {(1, None)})


if __name__ == "__main__":
    unittest.main()
