"""What a researcher's edits to a pipeline rerun, checked end to end with a real `spit`.

Each test builds and runs a small pipeline, edits it in one way, compiles it again, and
states which jobs the plan reruns and how many a run then does. The edits are the ones made
while a pipeline is being written: renames, reordering, moved paths, changed commands,
checks, and new or changed data. Set SPIT to the binary.
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from collections import Counter
from pathlib import Path

SPIT = os.environ.get("SPIT") or shutil.which("spit")

PIPELINE = """\
default entities: {@key}={@value} separated "_"
default path: out/{@product}/{@entities}.txt

check nonempty: test -s {@path}

source raw : Lines [sub]
path raw: input/{sub}.txt

operation sort_lines(input: Lines) -> Lines @check(nonempty)
command sort_lines: sort -u -o {@output} {input}
resources sort_lines: cores=1

operation tally_lines(input: Lines) -> Lines
command tally_lines: uniq -c {input} {@output}

operation merge(items: many Lines) -> Lines
command merge: sort -m -o {@output} {items}

sorted = sort_lines(raw)
tally = tally_lines(sorted)
path merged: out/merged.txt
merged = merge(sorted @vary(sub))
"""


def spit_bash(root, *args):
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).parents[1] / "src"))
    return subprocess.run([sys.executable, "-m", "spit_bash", *args, "p.spitdag"], cwd=root, env=env,
                          capture_output=True, text=True)


@unittest.skipUnless(SPIT, "set SPIT to the spit binary")
class IncrementalTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "input").mkdir()
        (self.root / "input/s1.txt").write_text("b\na\nb\n")
        (self.root / "input/s2.txt").write_text("z\ny\nz\n")
        (self.root / "p.spit").write_text(PIPELINE)
        (self.root / "p.spitin").write_text("pipeline p.spit\nroot .\n")
        self.compile()
        result = spit_bash(self.root, "run")
        self.assertIn("0 failed", result.stdout + result.stderr)

    def compile(self):
        subprocess.run([SPIT, "dag", "p.spitin", "-o", "p.spitdag"], cwd=self.root, check=True, capture_output=True)

    def edit(self, old, new, file="p.spit"):
        path = self.root / file
        text = path.read_text()
        self.assertIn(old, text)
        path.write_text(text.replace(old, new, 1))

    def outcome(self, *flags):
        """Compile the edited pipeline; return the plan's statuses and how many jobs a run did."""
        self.compile()
        plan = json.loads(spit_bash(self.root, "plan", "--json", *flags).stdout)
        statuses = Counter(job["status"] for job in plan["jobs"])
        run = spit_bash(self.root, "run", *flags)
        done = int(re.findall(r"(\d+) done,", run.stdout + run.stderr)[-1])
        return dict(statuses), done

    def assertReruns(self, statuses, done, *flags):
        self.assertEqual(self.outcome(*flags), (statuses, done))

    # Edits that change nothing the jobs do.

    def test_nothing_changed(self):
        self.assertReruns({"skip": 5}, 0)

    def test_comments_and_blank_lines(self):
        self.edit("source raw", "# a note\n\n\nsource raw")
        self.assertReruns({"skip": 5}, 0)

    def test_wrapping_a_step_in_a_stage_whose_name_is_not_in_the_path(self):
        self.edit("sorted = sort_lines(raw)", "stage first:\n    sorted = sort_lines(raw)\n")
        self.assertReruns({"skip": 5}, 0)

    def test_reordering_steps(self):
        self.edit("tally = tally_lines(sorted)\n", "")
        self.edit("merged = merge(sorted @vary(sub))\n", "merged = merge(sorted @vary(sub))\ntally = tally_lines(sorted)\n")
        self.assertReruns({"skip": 5}, 0)

    def test_renaming_an_operation(self):
        self.edit("operation tally_lines", "operation count")
        self.edit("command tally_lines", "command count")
        self.edit("= tally_lines(", "= count(")
        self.assertReruns({"skip": 5}, 0)

    def test_renaming_a_parameter(self):
        self.edit("tally_lines(input: Lines)", "tally_lines(x: Lines)")
        self.edit("uniq -c {input}", "uniq -c {x}")
        self.assertReruns({"skip": 5}, 0)

    def test_changing_resources(self):
        self.edit("cores=1", "cores=2")
        self.assertReruns({"skip": 5}, 0)

    def test_whitespace_in_a_command(self):
        self.edit("sort -u -o {@output} {input}", "sort  -u  -o {@output}  {input}")
        self.assertReruns({"skip": 5}, 0)

    def test_touching_a_source(self):
        later = time.time() + 100
        os.utime(self.root / "input/s1.txt", (later, later))
        self.assertReruns({"skip": 5}, 0)

    # Edits to paths.

    def test_changed_paths_rerun_without_move_files(self):
        self.edit("out/{@product}", "res/{@product}")
        self.edit("out/merged.txt", "res/merged.txt")
        self.assertReruns({"run": 5}, 5)

    def test_changed_paths_are_reused_with_move_files(self):
        self.edit("out/{@product}", "res/{@product}")
        self.edit("out/merged.txt", "res/merged.txt")
        self.assertReruns({"place": 5}, 0, "--move-files")
        self.assertTrue((self.root / "res/merged.txt").exists())
        self.assertFalse((self.root / "out/merged.txt").exists())
        self.assertReruns({"skip": 5}, 0, "--move-files")

    def test_a_moved_source(self):
        shutil.move(self.root / "input", self.root / "data")
        self.edit("input/{sub}", "data/{sub}")
        self.assertReruns({"skip": 5}, 0)

    # Edits to commands.

    def test_a_command_that_changes_its_output(self):
        self.edit("sort -u -o", "sort -u -r -o")
        self.assertReruns({"run": 2, "maybe": 3}, 5)

    def test_a_command_with_the_same_output_stops_the_rerun_at_the_job(self):
        self.edit("sort -u -o {@output} {input}", "sort -o {@output} -u {input}")
        self.assertReruns({"run": 2, "maybe": 3}, 2)

    def test_a_changed_command_in_the_last_step(self):
        self.edit("sort -m -o", "sort -m -u -o")
        self.assertReruns({"run": 1, "skip": 4}, 1)

    # Edits to the shape of the pipeline.

    def test_a_new_step_leaves_the_others(self):
        self.edit("merged = merge(", "again = tally_lines(sorted)\nmerged = merge(")
        self.assertReruns({"run": 2, "skip": 5}, 2)

    def test_a_step_inserted_between_two_reruns_what_follows(self):
        self.edit("tally = tally_lines(sorted)",
                  "operation keep(input: Lines) -> Lines\ncommand keep: cp {input} {@output}\n"
                  "filtered = keep(sorted)\ntally = tally_lines(filtered)")
        statuses, done = self.outcome()
        self.assertEqual(statuses["run"], 4)
        self.assertEqual(done, 4)

    def test_a_removed_step_leaves_the_rest_current(self):
        self.edit("tally = tally_lines(sorted)\n", "")
        self.assertReruns({"skip": 3}, 0)

    # Checks.

    def test_a_new_check_only_checks(self):
        self.edit("operation tally_lines(input: Lines) -> Lines", "operation tally_lines(input: Lines) -> Lines @check(nonempty)")
        self.assertReruns({"check": 2, "skip": 3}, 0)

    def test_a_changed_check_only_checks(self):
        self.edit("test -s {@path}", "test -f {@path}")
        self.assertReruns({"check": 2, "skip": 3}, 0)

    def test_moving_checked_files_does_not_check_again(self):
        self.edit("out/{@product}", "res/{@product}")
        self.edit("out/merged.txt", "res/merged.txt")
        self.assertReruns({"place": 5}, 0, "--move-files")

    # Data.

    def test_a_changed_source(self):
        (self.root / "input/s1.txt").write_text("q\nq\nw\n")
        self.assertReruns({"run": 1, "maybe": 2, "skip": 2}, 3)

    def test_a_changed_source_with_the_same_sorted_output_stops_early(self):
        (self.root / "input/s1.txt").write_text("a\nb\na\n")
        self.assertReruns({"run": 1, "maybe": 2, "skip": 2}, 1)

    def test_a_new_subject(self):
        (self.root / "input/s3.txt").write_text("m\n")
        self.assertReruns({"run": 3, "skip": 4}, 3)

    def test_a_removed_subject(self):
        (self.root / "input/s2.txt").unlink()
        self.assertReruns({"run": 1, "skip": 2}, 1)

    def test_a_deleted_output(self):
        (self.root / "out/sorted/sub=s1.txt").unlink()
        self.assertReruns({"run": 3, "skip": 2}, 3)

    def test_an_edited_output(self):
        (self.root / "out/sorted/sub=s1.txt").write_text("tampered\n")
        self.assertReruns({"run": 3, "skip": 2}, 3)

    def test_an_edited_output_is_kept_beside_the_one_its_rerun_makes(self):
        (self.root / "out/sorted/sub=s1.txt").write_text("tampered\n")
        self.compile()
        plan = spit_bash(self.root, "plan")
        self.assertIn("1 job reruns because an output was edited since it was made; "
                      "run keeps what is there as NAME.edited-<time> beside it\n", plan.stdout)
        self.assertRegex(plan.stdout, r"\[\d+\] edited: out/sorted/sub=s1.txt\n")
        spit_bash(self.root, "run")
        kept = list((self.root / "out/sorted").glob("sub=s1.txt.edited-*"))
        self.assertEqual([path.read_text() for path in kept], ["tampered\n"])
        self.assertEqual((self.root / "out/sorted/sub=s1.txt").read_text(), "a\nb\n")

    # What a plan says changed since the last run, before its counts.

    def changes(self):
        self.compile()
        lines = [line for line in spit_bash(self.root, "plan").stdout.splitlines() if line.startswith("since the last run")]
        return lines[0] if lines else None

    def test_a_plan_with_nothing_changed_says_nothing(self):
        self.assertIsNone(self.changes())

    def test_a_plan_names_new_changed_and_gone_sources(self):
        (self.root / "input/s3.txt").write_text("m\n")
        (self.root / "input/s1.txt").write_text("q\nq\nw\n")
        (self.root / "input/s2.txt").unlink()
        self.assertEqual(self.changes(), "since the last run: +1 raw (sub=s3); 1 raw changed (sub=s1); "
                                         "2 jobs from earlier runs no longer in the DAG (sub=s2)")

    def test_a_plan_names_an_operation_whose_command_changed(self):
        self.edit("command tally_lines: uniq -c", "command tally_lines: uniq -ci")
        self.assertEqual(self.changes(), "since the last run: tally_lines: command changed (2 jobs)")

    # Renames of what a job makes or reads: the recorded work is reused.

    def test_renaming_a_product_reuses_its_outputs_with_move_files(self):
        self.edit("tally = ", "counts = ")
        self.assertReruns({"place": 2, "skip": 3}, 0, "--move-files")

    def test_renaming_a_dimension_reuses_its_outputs_with_move_files(self):
        self.edit("[sub]", "[id]")
        self.edit("input/{sub}", "input/{id}")
        self.edit("@vary(sub)", "@vary(id)")
        self.assertReruns({"place": 4, "skip": 1}, 0, "--move-files")

    def test_renaming_a_dimension_moves_jobs_whose_inputs_hold_the_same_bytes(self):
        # Different sources that sort to the same lines, so each `tally_lines` reads
        # the same bytes and its records are interchangeable by content. Each job must
        # still take the record of what it made under the old name, though the jobs
        # come in numeric order, 9 before 10, and the records in text order.
        for name in ("s1", "s2"):
            (self.root / f"input/{name}.txt").unlink()
        for name, text in (("9", "a\nb\n"), ("10", "b\na\n"), ("11", "b\nb\na\n")):
            (self.root / f"input/{name}.txt").write_text(text)
        self.compile()
        self.assertIn("0 failed", spit_bash(self.root, "run").stderr)
        self.edit("[sub]", "[id]")
        self.edit("input/{sub}", "input/{id}")
        self.edit("@vary(sub)", "@vary(id)")
        self.assertReruns({"place": 6, "skip": 1}, 0, "--move-files")
        for name in ("9", "10", "11"):
            self.assertFalse((self.root / f"out/tally/sub={name}.txt").exists())
            self.assertTrue((self.root / f"out/tally/id={name}.txt").exists())

    def test_renaming_a_source_family_reuses_its_outputs_with_move_files(self):
        self.edit("source raw", "source scans")
        self.edit("path raw:", "path scans:")
        self.edit("sort_lines(raw)", "sort_lines(scans)")
        self.assertReruns({"skip": 5}, 0, "--move-files")

    def test_relabelling_entity_values_of_the_same_files_reuses_outputs_with_move_files(self):
        for old, new in (("s1", "p1"), ("s2", "p2")):
            (self.root / f"input/{old}.txt").rename(self.root / f"input/{new}.txt")
        self.assertReruns({"place": 4, "skip": 1}, 0, "--move-files")

    def test_a_renamed_product_whose_input_changed_is_not_reused(self):
        self.edit("tally = ", "counts = ")
        (self.root / "input/s1.txt").write_text("q\nq\nw\n")
        statuses, done = self.outcome("--move-files")
        self.assertEqual(done, 3)

    def test_a_renamed_product_without_move_files_reruns_only_it(self):
        self.edit("tally = ", "counts = ")
        self.assertReruns({"run": 2, "skip": 3}, 2)

    def test_a_new_layout_without_move_files_says_moving_would_rerun_nothing(self):
        self.edit("default path: out/{@product}/{@entities}.txt", "default path: results/{@product}/{@entities}.txt")
        self.compile()
        plan = spit_bash(self.root, "plan")
        self.assertIn("4 jobs rerun only because their outputs are recorded at other paths; "
                      "--move-files moves them instead\n", plan.stderr)
        moved = spit_bash(self.root, "plan", "--move-files")
        self.assertNotIn("--move-files moves them", moved.stderr)

    def test_swapping_the_content_of_two_sources_reruns_what_reads_them(self):
        a, b = (self.root / "input/s1.txt"), (self.root / "input/s2.txt")
        first, second = a.read_text(), b.read_text()
        a.write_text(second)
        b.write_text(first)
        statuses, done = self.outcome()
        self.assertGreaterEqual(done, 2)


if __name__ == "__main__":
    unittest.main()
