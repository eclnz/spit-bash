"""How jobs are named in the terminal."""

import json
import tempfile
import unittest
from pathlib import Path

from spit_bash.display import id_width, labels, name
from support import command, document, job, read


def made(id, operation, product, entities, source="source.txt"):
    """A job that writes `product[entities]`, with its output named by them."""
    path = f"out/{product}-" + "-".join(f"{k}{v}" for k, v in entities.items()) + ".txt"
    item = job(id, [source], path, "pass")
    item["operation"] = operation
    item["outputs"]["output"].update(product=product, entities=entities)
    return item


def dag_of(*items):
    root = Path(tempfile.mkdtemp())
    (root / "source.txt").write_text("")
    return read(document(root, list(items), ["source.txt"]))


class LabelTests(unittest.TestCase):
    def test_repeated_operations_for_one_subject_are_told_apart_by_task_and_run(self):
        dag = dag_of(
            made(1, "motion", "mc", {"sub": "02", "task": "balloon", "run": "01"}),
            made(2, "motion", "mc", {"sub": "02", "task": "balloon", "run": "02"}),
            made(3, "motion", "mc", {"sub": "02", "task": "rest", "run": "01"}),
        )
        work = labels(dag.jobs)
        self.assertEqual(work[1], "run=01 sub=02 task=balloon")
        self.assertEqual(len({work[i] for i in (1, 2, 3)}), 3)

    def test_a_group_job_names_the_group_it_makes_not_a_subject(self):
        dag = dag_of(made(1, "merge", "table", {"group": "alpha"}), made(2, "merge", "table", {"group": "beta"}))
        self.assertEqual(labels(dag.jobs), {1: "group=alpha", 2: "group=beta"})

    def test_jobs_with_no_entities_are_told_apart_by_product(self):
        dag = dag_of(made(1, "report", "summary", {}), made(2, "report", "appendix", {}))
        self.assertEqual(labels(dag.jobs), {1: "summary", 2: "appendix"})

    def test_a_job_that_is_alone_in_its_operation_with_no_entities_has_no_label(self):
        dag = dag_of(made(1, "report", "summary", {}))
        self.assertEqual(labels(dag.jobs), {1: ""})
        self.assertEqual(name(dag.jobs[0], ""), "report")

    def test_the_same_entities_under_different_products_are_told_apart(self):
        dag = dag_of(made(1, "split", "left", {"sub": "01"}), made(2, "split", "right", {"sub": "01"}))
        work = labels(dag.jobs)
        self.assertEqual(work, {1: "left sub=01", 2: "right sub=01"})

    def test_long_values_and_labels_stay_short(self):
        dag = dag_of(made(1, "op", "p", {"sub": "x" * 100}))
        self.assertLessEqual(len(labels(dag.jobs)[1]), 80)
        self.assertIn("…", labels(dag.jobs)[1])

    def test_ids_line_up_to_the_widest(self):
        dag = dag_of(made(9, "a", "p", {"i": "1"}), made(10, "a", "p", {"i": "2"}))
        self.assertEqual(id_width(dag.jobs), 2)


if __name__ == "__main__":
    unittest.main()
