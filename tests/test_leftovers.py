import json
import tempfile
import unittest
from pathlib import Path

from support import sample, state_at
from test_cli import call


class LeftoverTests(unittest.TestCase):
    """A job the pipeline no longer has leaves its outputs on disk, which plan and run report."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source.txt").write_text("hello")
        self.data = sample(self.root)
        self.dag = self.root / "jobs.spitdag"
        self.dag.write_text(json.dumps(self.data))
        code, _, err = call("run", str(self.dag))
        self.assertEqual(code, 0, err)
        # The pipeline loses its second step, which made final.txt.
        self.data["jobs"] = self.data["jobs"][:1]
        self.data["jobs"][0]["dependents"] = []
        self.dag.write_text(json.dumps(self.data))

    def test_plan_reports_them_and_changes_nothing(self):
        code, _, err = call("plan", str(self.dag))
        self.assertEqual(code, 0, err)
        self.assertIn("1 output (", err)
        self.assertIn("left over from jobs the pipeline no longer has; run --remove-leftovers to delete "
                      "those unchanged since their run, or --verbose to list them", err)
        _, _, err = call("plan", str(self.dag), "--verbose")
        self.assertRegex(err, r"\n  final\.txt \(\d+ bytes\)\n")
        self.assertTrue((self.root / "final.txt").exists())

    def test_a_run_reports_them_and_deletes_them_only_when_asked(self):
        code, _, err = call("run", str(self.dag))
        self.assertEqual(code, 0, err)
        self.assertIn("left over from jobs the pipeline no longer has", err)
        self.assertTrue((self.root / "final.txt").exists())
        code, _, err = call("run", str(self.dag), "--remove-leftovers")
        self.assertEqual(code, 0, err)
        self.assertIn("; removing those unchanged since their run", err)
        self.assertFalse((self.root / "final.txt").exists())
        state = state_at(self.root)
        self.assertEqual(len(state.jobs), 1)
        state.close()
        _, _, err = call("plan", str(self.dag))
        self.assertNotIn("left over", err)

    def test_an_output_changed_since_its_run_is_kept(self):
        (self.root / "final.txt").write_text("edited by hand, and longer than before")
        code, _, err = call("run", str(self.dag), "--remove-leftovers", "--verbose")
        self.assertEqual(code, 0, err)
        self.assertIn("removing those unchanged since their run, keeping 1 changed", err)
        self.assertIn("final.txt (38 bytes, changed since its run)", err)
        self.assertEqual((self.root / "final.txt").read_text(), "edited by hand, and longer than before")
        _, _, err = call("plan", str(self.dag))
        self.assertIn("left over", err)

    def test_a_partial_dag_reports_none(self):
        self.data["left_out"] = [{"identity": "final[]", "reasons": ["missing input"]}]
        self.dag.write_text(json.dumps(self.data))
        code, _, err = call("plan", str(self.dag))
        self.assertIn("partial DAG", err)
        self.assertNotIn("left over", err)


if __name__ == "__main__":
    unittest.main()
