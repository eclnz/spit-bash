import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import DagError
from spit_bash.state.record import Record
from spit_bash.state.store import StateWrite, Store

RECORD = Record(fingerprint="a" * 16, inputs={"in.txt": [1, 2]}, outputs={"out.txt": [3, 4]})


class StateTests(unittest.TestCase):
    def test_each_change_appends_one_line_and_close_compacts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.jsonl"
            state = Store(path)
            for n in range(3):
                state.record(f"key{n}", RECORD)
            state.forget("key1")
            self.assertEqual(len(path.read_text().splitlines()), 5)
            self.assertEqual(set(Store(path).jobs), {"key0", "key2"})
            state.close()
            self.assertEqual(len(path.read_text().splitlines()), 3)
            self.assertEqual(Store(path).jobs["key2"], RECORD)

    def test_apply_records_and_forgets_in_order(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.jsonl"
            state = Store(path)
            state.record("old", RECORD)
            state.apply((StateWrite("new", RECORD), StateWrite("old", None), StateWrite("missing", None)))
            self.assertEqual(state.jobs, {"new": RECORD})
            # Forgetting a key with no record writes no line.
            self.assertEqual(len(path.read_text().splitlines()), 4)
            state.close()
            self.assertEqual(Store(path).jobs, {"new": RECORD})

    def test_ignores_a_partial_last_line(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.jsonl"
            state = Store(path)
            state.record("key", RECORD)
            state.close()
            with path.open("a") as stream:
                stream.write('{"key": "other", "rec')
            self.assertEqual(set(Store(path).jobs), {"key"})
            self.assertTrue(path.read_text().endswith("\n"))

    def test_rejects_a_corrupt_line(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.jsonl"
            path.write_text('{"version": 7}\nnot json\n{"key": "k", "record": null}\n')
            with self.assertRaisesRegex(DagError, "cannot read state"):
                Store(path)
            path.write_text('{"version": 1, "jobs": {}}\n')
            with self.assertRaisesRegex(DagError, "cannot read state"):
                Store(path)

    def test_lock_allows_one_holder(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.jsonl"
            first = Store(path)
            first.lock()
            with self.assertRaisesRegex(DagError, "another spit-bash run"):
                Store(path).lock()
            first.close()
            second = Store(path)
            second.lock()
            second.close()


if __name__ == "__main__":
    unittest.main()
