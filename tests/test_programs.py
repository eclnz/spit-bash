import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from spit_bash.plan import Status, adopt, job_programs, plan, program_hashes
from support import artifact, document, read, run, state_at


def write_tool(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"#!{sys.executable}\nimport sys\nfrom pathlib import Path\n{text}\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


COPY = "Path(sys.argv[2]).write_text(Path(sys.argv[1]).read_text() + {!r})"


def tool_job(program, id=1, source="source.txt", output="out.txt", dependents=()):
    return {
        "id": id, "operation": f"op{id}", "fingerprint": f"{id:016x}", "stage": [], "dependents": list(dependents),
        "inputs": {"input": [artifact(source)]}, "outputs": {"output": artifact(output)},
        "depends_on": [], "verify": [],
        "command": [[program], [{"path": source}], [{"path": output}]],
    }


class ProgramTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source.txt").write_text("hello")
        write_tool(self.root / "bin/tool", COPY.format("!"))
        self.dag = read(document(self.root, [tool_job("bin/tool")], ["source.txt"]))
        run(self.dag, state_at(self.root), programs=program_hashes(self.dag))

    def decisions(self, **options):
        return plan(self.dag, state_at(self.root), programs=program_hashes(self.dag), **options)

    def test_names_the_programs_a_job_starts_with_once_each(self):
        self.assertEqual(job_programs(self.dag.jobs[0]), ("bin/tool",))
        self.assertEqual(list(program_hashes(self.dag)), ["bin/tool"])

    def test_an_unchanged_program_leaves_the_job_current(self):
        self.assertEqual([d.status for d in self.decisions()], [Status.SKIP])

    def test_a_program_that_was_edited_reruns_its_job(self):
        write_tool(self.root / "bin/tool", COPY.format("?"))
        decisions = self.decisions()
        self.assertEqual([(d.status, d.reason) for d in decisions], [(Status.RUN, "program changed: bin/tool")])
        run(self.dag, state_at(self.root), programs=program_hashes(self.dag))
        self.assertEqual((self.root / "out.txt").read_text(), "hello?")
        self.assertEqual([d.status for d in self.decisions()], [Status.SKIP])

    def test_only_the_jobs_that_use_the_program_rerun(self):
        write_tool(self.root / "bin/other", COPY.format("#"))
        data = document(self.root, [tool_job("bin/tool"), tool_job("bin/other", 2, output="other.txt")], ["source.txt"])
        dag = read(data)
        run(dag, state_at(self.root), programs=program_hashes(dag))
        write_tool(self.root / "bin/other", COPY.format("%"))
        statuses = [d.status for d in plan(dag, state_at(self.root), programs=program_hashes(dag))]
        self.assertEqual(statuses, [Status.SKIP, Status.RUN])

    def test_ignoring_programs_keeps_the_job_current(self):
        write_tool(self.root / "bin/tool", COPY.format("?"))
        self.assertEqual([d.status for d in plan(self.dag, state_at(self.root), programs=None)], [Status.SKIP])

    def test_a_record_with_no_programs_is_unknown_not_changed(self):
        run(self.dag, state_at(self.root), force=True, programs=None)
        write_tool(self.root / "bin/tool", COPY.format("?"))
        self.assertEqual([d.status for d in self.decisions()], [Status.SKIP])
        run(self.dag, state_at(self.root), force=True, programs=program_hashes(self.dag))
        write_tool(self.root / "bin/tool", COPY.format("!"))
        self.assertEqual([d.status for d in self.decisions()], [Status.RUN])

    def test_a_program_on_path_is_found_and_hashed(self):
        bin_dir = self.root / "onpath"
        write_tool(bin_dir / "pathtool", COPY.format("+"))
        with mock.patch.dict(os.environ, {"PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}):
            dag = read(document(self.root, [tool_job("pathtool", 1, output="p.txt")], ["source.txt"]))
            run(dag, state_at(self.root), programs=program_hashes(dag))
            self.assertEqual([d.status for d in plan(dag, state_at(self.root), programs=program_hashes(dag))], [Status.SKIP])
            write_tool(bin_dir / "pathtool", COPY.format("-"))
            decisions = plan(dag, state_at(self.root), programs=program_hashes(dag))
            self.assertEqual(decisions[0].reason, "program changed: pathtool")

    def test_a_missing_program_blocks_and_is_not_hashed(self):
        (self.root / "bin/tool").unlink()
        self.assertEqual(program_hashes(self.dag), {})
        # A current job needs no program; one that must run does.
        self.assertEqual([d.status for d in self.decisions()], [Status.SKIP])
        (self.root / "source.txt").write_text("new")
        self.assertEqual([d.status for d in self.decisions()], [Status.BLOCKED])

    def test_adopt_records_the_programs_that_are_there(self):
        state = state_at(self.root)
        state.forget(next(iter(state.jobs)))
        adopt(self.dag, state, program_hashes(self.dag))
        write_tool(self.root / "bin/tool", COPY.format("?"))
        self.assertEqual([d.status for d in plan(self.dag, state, programs=program_hashes(self.dag))], [Status.RUN])


if __name__ == "__main__":
    unittest.main()
