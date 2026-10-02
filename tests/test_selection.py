import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import DagError
from spit_bash.selection import Selection, parse_only, parse_stage, select
from support import document, job, read, UPPER


def made(data, id, product, entities, stage=()):
    output = data["jobs"][id - 1]["outputs"]["output"]
    output["product"] = product
    output["entities"] = entities
    data["jobs"][id - 1]["stage"] = list(stage)


def study(root):
    """A template, a per-subject step that reads it, a stage inside per subject, and a group mean."""
    data = document(root, [
        job(1, ["source.txt"], "template.txt", UPPER, dependents=[2, 3]),
        job(2, ["template.txt"], "sub-01.txt", UPPER, depends_on=[1], dependents=[4, 5]),
        job(3, ["template.txt"], "sub-02.txt", UPPER, depends_on=[1], dependents=[6]),
        job(4, ["sub-01.txt"], "sub-01-clean.txt", UPPER, depends_on=[2], dependents=[7]),
        job(5, ["sub-01.txt"], "sub-01-qc.txt", UPPER, depends_on=[2]),
        job(6, ["sub-02.txt"], "sub-02-clean.txt", UPPER, depends_on=[3], dependents=[7]),
        job(7, ["sub-01-clean.txt", "sub-02-clean.txt"], "mean.txt", UPPER, depends_on=[4, 6]),
    ], ["source.txt"])
    made(data, 1, "template", {})
    made(data, 2, "aligned", {"sub": "01"}, ["pre"])
    made(data, 3, "aligned", {"sub": "02"}, ["pre"])
    made(data, 4, "clean", {"sub": "01"}, ["pre", "denoise"])
    made(data, 5, "qc", {"sub": "01"})
    made(data, 6, "clean", {"sub": "02"}, ["pre", "denoise"])
    made(data, 7, "mean", {})
    return data


class SelectionTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.dag = read(study(Path(directory.name)))

    def ids(self, **criteria):
        return [job.id for job in select(self.dag, Selection(**criteria)).dag.jobs]

    def test_no_criteria_keeps_everything(self):
        self.assertEqual(self.ids(), [1, 2, 3, 4, 5, 6, 7])

    def test_entities_keep_matching_jobs_and_what_they_need_but_nothing_downstream(self):
        selected = select(self.dag, Selection(only=(parse_only("sub=01"),)))
        self.assertEqual([job.id for job in selected.dag.jobs], [1, 2, 4, 5])
        self.assertEqual((selected.matched, selected.needed), (3, 1))
        self.assertEqual(self.ids(only=(parse_only("sub=01"), parse_only("sub=02"))), [1, 2, 3, 4, 5, 6])

    def test_product_and_stage_combine_with_entities(self):
        self.assertEqual(self.ids(products=("clean",)), [1, 2, 3, 4, 6])
        self.assertEqual(self.ids(products=("clean",), only=(parse_only("sub=02"),)), [1, 3, 6])
        self.assertEqual(self.ids(stages=(parse_stage("pre"),)), [1, 2, 3, 4, 6])
        self.assertEqual(self.ids(stages=(parse_stage("pre/denoise"),), only=(parse_only("sub=01"),)), [1, 2, 4])
        self.assertEqual(self.ids(products=("mean",)), [1, 2, 3, 4, 6, 7])

    def test_names_what_cannot_match(self):
        cases = [
            (Selection(only=(parse_only("subject=01"),)), "no output has dimension `subject`; dimensions: sub"),
            (Selection(only=(parse_only("sub=03"),)), "no output has sub=03; sub takes 01, 02"),
            (Selection(products=("cleaned",)), "products: aligned, clean, mean, qc, template"),
            (Selection(stages=(parse_stage("denoise"),)), "stages: pre, pre/denoise"),
            (Selection(products=("qc",), stages=(parse_stage("pre"),)), "no job's outputs match every criterion"),
        ]
        for selection, message in cases:
            with self.subTest(message), self.assertRaisesRegex(DagError, message):
                select(self.dag, selection)

    def test_parsing(self):
        self.assertEqual(parse_only("ses=2,sub=01"), (("ses", "2"), ("sub", "01")))
        self.assertEqual(parse_only("run=a=b"), (("run", "a=b"),))
        for text in ["sub", "=01", "sub=", "sub=01,sub=02", "sub=01,"]:
            with self.subTest(text), self.assertRaises(ValueError):
                parse_only(text)
        self.assertEqual(parse_stage("pre/denoise"), ("pre", "denoise"))
        with self.assertRaises(ValueError):
            parse_stage("pre/")


if __name__ == "__main__":
    unittest.main()
