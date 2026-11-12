"""A DAG whose outputs have a root of their own, apart from the dataset's."""

import tempfile
import unittest
from pathlib import Path

from spit_bash.plan import Status, plan
from spit_bash.state.store import Store
from support import UPPER, artifact, error, job_entry, read, run, sample, job_log

# Write the input's text, upper-cased, beside it, as an index tool does.
BESIDE = "from pathlib import Path; import sys; Path(sys.argv[2]).write_text(Path(sys.argv[1]).read_text().upper())"


class OutputRootTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.data = Path(directory.name).resolve() / "data"
        self.results = Path(directory.name).resolve() / "results"
        self.data.mkdir()
        (self.data / "source.txt").write_text("hello")

    def apart(self, data):
        data["output_root"] = str(self.results)
        return data

    def test_outputs_state_and_logs_go_under_the_output_root(self):
        dag = read(self.apart(sample(self.data)))
        self.assertEqual(dag.outputs, self.results)
        self.assertEqual(dag.path("source.txt"), self.data / "source.txt")
        self.assertEqual(dag.path("final.txt"), self.results / "final.txt")
        state = Store(dag.layout.state)
        _, results = run(dag, state)
        self.assertEqual([error(result) for result in results], [None, None])
        self.assertEqual((self.results / "final.txt").read_text(), "HELLO!")
        self.assertEqual(sorted(path.name for path in self.data.iterdir()), ["source.txt"])
        self.assertTrue((self.results / ".spit-bash" / "state.jsonl").is_file())
        self.assertTrue(job_log(self.results / ".spit-bash", "work/upper.txt").is_file())
        decisions = plan(read(self.apart(sample(self.data))), Store(dag.layout.state, False))
        self.assertEqual([decision.status for decision in decisions], [Status.SKIP, Status.SKIP])

    def test_a_command_names_a_dataset_file_by_its_absolute_path(self):
        dag = read(self.apart(sample(self.data)))
        arguments = dag.jobs[0].commands[0].arguments
        self.assertEqual(arguments[-2:], (str(self.data / "source.txt"), "work/upper.txt"))

    def test_a_file_written_beside_a_source_goes_into_the_dataset(self):
        data = sample(self.data)
        data["jobs"].append(job_entry(3, ["source.txt"], "source.txt.idx", BESIDE))
        data["jobs"][2]["outputs"]["output"] = artifact("source.txt.idx", "root")
        dag = read(self.apart(data))
        self.assertEqual(dag.path("source.txt.idx"), self.data / "source.txt.idx")
        _, results = run(dag, Store(dag.layout.state))
        self.assertEqual([error(result) for result in results], [None, None, None])
        self.assertEqual((self.data / "source.txt.idx").read_text(), "HELLO")
        self.assertFalse((self.results / "source.txt.idx").exists())

    def test_without_an_output_root_everything_is_under_the_dataset_root(self):
        dag = read(sample(self.data))
        self.assertEqual(dag.outputs, self.data)
        self.assertEqual(dag.path("final.txt"), self.data / "final.txt")
        self.assertEqual(dag.jobs[0].commands[0].arguments[-2:], ("source.txt", "work/upper.txt"))


if __name__ == "__main__":
    unittest.main()
