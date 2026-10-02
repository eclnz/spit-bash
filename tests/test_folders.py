"""Folder artifacts: a version 5 DAG may mark an artifact `"kind": "folder"`."""

import os
import tempfile
import unittest
from pathlib import Path

from spit_bash.dag import DagError
from spit_bash.plan import Status, plan, stamp
from support import artifact, command, document, read, run, state_at

# Copy every file of the input folder into the output folder, upper-cased.
COPY_UPPER = (
    "from pathlib import Path; import sys\n"
    "source, target = Path(sys.argv[1]), Path(sys.argv[2])\n"
    "target.mkdir()\n"
    "for file in sorted(source.rglob('*.txt')):\n"
    "    (target / file.name).write_text(file.read_text().upper())\n"
)
# Join every file of the input folder into one output file.
JOIN = (
    "from pathlib import Path; import sys\n"
    "files = sorted(Path(sys.argv[1]).iterdir())\n"
    "Path(sys.argv[2]).write_text(''.join(file.read_text() for file in files))\n"
)


def folder(path):
    return dict(artifact(path), kind="folder")


def folders_dag(root):
    """pages/ -> work/upper/ -> book.txt"""
    data = document(root, [
        {
            "id": 1, "operation": "upper", "fingerprint": "a" * 16, "stage": [], "dependents": [2],
            "inputs": {"input": [folder("pages")]}, "outputs": {"output": folder("work/upper")},
            "depends_on": [], "verify": [], "command": command(COPY_UPPER, "pages", "work/upper"),
        },
        {
            "id": 2, "operation": "join", "fingerprint": "b" * 16, "stage": [], "dependents": [],
            "inputs": {"input": [folder("work/upper")]}, "outputs": {"output": artifact("book.txt")},
            "depends_on": [1], "verify": [], "command": command(JOIN, "work/upper", "book.txt"),
        },
    ], [])
    data["version"] = 5
    data["external_inputs"] = [folder("pages")]
    return data


def pages(root):
    (root / "pages/part").mkdir(parents=True)
    (root / "pages/1.txt").write_text("one ")
    (root / "pages/part/2.txt").write_text("two")


class FolderTests(unittest.TestCase):
    def test_reads_which_artifacts_are_folders(self):
        with tempfile.TemporaryDirectory() as directory:
            dag = read(folders_dag(Path(directory)))
            self.assertEqual(dag.folders, frozenset({"pages", "work/upper"}))

            data = folders_dag(Path(directory))
            data["version"] = 4
            with self.assertRaisesRegex(DagError, "version 4 DAG has no folders"):
                read(data)

            data = folders_dag(Path(directory))
            data["jobs"][1]["inputs"]["input"] = [artifact("work/upper")]
            with self.assertRaisesRegex(DagError, "both a file and a folder: work/upper"):
                read(data)

    def test_runs_folders_then_reruns_when_a_file_inside_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pages(root)
            dag = read(folders_dag(root))
            decisions, results = run(dag, state_at(root))
            self.assertEqual([d.status for d in decisions], [Status.RUN, Status.RUN])
            self.assertFalse(any(r.error for r in results), [r.error for r in results])
            self.assertEqual((root / "book.txt").read_text(), "ONE TWO")
            self.assertEqual([d.status for d in plan(dag, state_at(root))], [Status.SKIP, Status.SKIP])

            # A file deep in the folder changes, but not the folder's own time.
            (root / "pages/part/2.txt").write_text("three")
            decisions, results = run(dag, state_at(root))
            self.assertEqual(decisions[0].reason, "input changed")
            self.assertEqual((root / "book.txt").read_text(), "ONE THREE")

    def test_clears_a_folder_an_earlier_run_left(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pages(root)
            (root / "work/upper").mkdir(parents=True)
            (root / "work/upper/stale.txt").write_text("STALE ")
            dag = read(folders_dag(root))
            decisions, results = run(dag, state_at(root))
            self.assertEqual(decisions[0].reason, "no successful run recorded")
            self.assertFalse(any(r.error for r in results), [r.error for r in results])
            # The tool's `mkdir` did not fail, and the stale file is gone.
            self.assertFalse((root / "work/upper/stale.txt").exists())
            self.assertEqual((root / "book.txt").read_text(), "ONE TWO")

    def test_a_missing_folder_blocks_and_a_command_must_make_its_folder(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pages").write_text("a file, not a folder")
            decisions = plan(read(folders_dag(root)), state_at(root))
            self.assertEqual(decisions[0].status, Status.BLOCKED)
            self.assertEqual(decisions[0].reason, "missing external input: pages")

            (root / "pages").unlink()
            pages(root)
            data = folders_dag(root)
            data["jobs"][0]["command"] = command("pass")
            decisions, results = run(read(data), state_at(root))
            self.assertEqual(results[0].error, "command succeeded but did not create every output")

    def test_a_folder_stamp_follows_the_files_under_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pages(root)
            first = stamp(root / "pages", folder=True)
            self.assertEqual(first[0], len("one ") + len("two"))
            self.assertEqual(first[2], 2)
            self.assertIsNone(stamp(root / "pages"))
            self.assertIsNone(stamp(root / "pages/1.txt", folder=True))

            # Same size, same count, and the folder's own time put back.
            folder_time = os.stat(root / "pages/part").st_mtime_ns
            (root / "pages/part/2.txt").write_text("TWO")
            os.utime(root / "pages/part", ns=(folder_time, folder_time))
            os.utime(root / "pages/part/2.txt", ns=(first[1] + 1, first[1] + 1))
            self.assertNotEqual(stamp(root / "pages", folder=True), first)


if __name__ == "__main__":
    unittest.main()
