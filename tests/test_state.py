import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import DagError
from spit_bash.plan import RunRecord, State

RECORD = RunRecord(fingerprint="a" * 16, inputs={"in.txt": [1, 2]}, outputs={"out.txt": [3, 4]})


class StateTests(unittest.TestCase):
    def test_each_change_appends_one_line_and_close_compacts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.jsonl"
            state = State(path)
            for n in range(3):
                state.record(f"key{n}", RECORD)
            state.forget("key1")
            self.assertEqual(len(path.read_text().splitlines()), 5)
            self.assertEqual(set(State(path).jobs), {"key0", "key2"})
            state.close()
            self.assertEqual(len(path.read_text().splitlines()), 3)
            self.assertEqual(State(path).jobs["key2"], RECORD)

    def test_ignores_a_partial_last_line(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.jsonl"
            state = State(path)
            state.record("key", RECORD)
            state.close()
            with path.open("a") as stream:
                stream.write('{"key": "other", "rec')
            self.assertEqual(set(State(path).jobs), {"key"})
            self.assertTrue(path.read_text().endswith("\n"))

    def test_rejects_a_corrupt_line(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.jsonl"
            path.write_text('{"version": 2}\nnot json\n{"key": "k", "record": null}\n')
            with self.assertRaisesRegex(DagError, "cannot read state"):
                State(path)
            path.write_text('{"version": 1, "jobs": {}}\n')
            with self.assertRaisesRegex(DagError, "cannot read state"):
                State(path)

    def test_lock_allows_one_holder(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.jsonl"
            first = State(path)
            first.lock()
            with self.assertRaisesRegex(DagError, "another spit-bash run"):
                State(path).lock()
            first.close()
            second = State(path)
            second.lock()
            second.close()


if __name__ == "__main__":
    unittest.main()
