"""A job's fingerprint and state key follow what it makes and runs, not where its files are."""

import copy
import tempfile
import unittest
from pathlib import Path

from spit_bash.plan import job_key
from support import read, sample


def moved(node, rename):
    """`node` with every artifact path and command path part renamed, nothing else changed."""
    if isinstance(node, dict):
        return {k: (rename(v) if k in ("path", "of") and isinstance(v, str) else moved(v, rename))
                for k, v in node.items()}
    if isinstance(node, list):
        return [moved(item, rename) for item in node]
    return node


class IdentityTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.data = sample(self.root)

    def test_changing_only_storage_paths_keeps_fingerprints_and_keys(self):
        before = read(self.data).jobs
        after = read(moved(copy.deepcopy(self.data), lambda path: "derivatives/" + path)).jobs
        self.assertEqual([j.fingerprint for j in before], [j.fingerprint for j in after])
        self.assertEqual([job_key(j) for j in before], [job_key(j) for j in after])
        self.assertNotEqual(before[0].outputs, after[0].outputs)

    def test_a_changed_command_or_artifact_identity_changes_the_fingerprint(self):
        base = read(self.data).jobs[0].fingerprint
        changed = copy.deepcopy(self.data)
        changed["jobs"][0]["commands"][0]["command"][2] = ["print('other')"]
        self.assertNotEqual(base, read(changed).jobs[0].fingerprint)
        renamed = copy.deepcopy(self.data)
        renamed["jobs"][0]["inputs"]["input"][0]["entities"] = {"sub": "01"}
        self.assertNotEqual(base, read(renamed).jobs[0].fingerprint)

    def test_scheduling_checks_and_origin_do_not_change_the_fingerprint(self):
        base = read(self.data).jobs[0].fingerprint
        other = copy.deepcopy(self.data)
        other["jobs"][0].update(cores=4, memory=1024, id=1, stage=["s"])
        self.assertEqual(base, read(other).jobs[0].fingerprint)


if __name__ == "__main__":
    unittest.main()
