"""Shared builders for the tests: a two-job DAG and helpers to load and run it."""

import asyncio
import io
import json
import sys
from pathlib import Path

from spit_bash.load import load_dag
from spit_bash.model import Artifact, Job, Kind, Port, Run, Work
from spit_bash.plan import plan
from spit_bash.reuse import identity
from spit_bash.state.store import Store
from spit_bash.execute.options import Options
from spit_bash.execute.scheduler import execute
from spit_bash.report import run_text

FIXTURE = Path(__file__).parent / "fixtures" / "command_demo.spitdag"


def artifact(path):
    return {"product": path, "entities": {}, "type": None, "path": path, "kind": "file"}


def command(script, *paths):
    return [[sys.executable], ["-c"], [script], *[[{"path": path}] for path in paths]]


def job_entry(id, inputs, output, script, depends_on=(), dependents=()):
    return {
        "id": id, "operation": f"op{id}",
        "stage": [], "dependents": list(dependents),
        "inputs": {"input": [artifact(path) for path in inputs]},
        "outputs": {"output": artifact(output)},
        "depends_on": list(depends_on), "verify": [], "checks": [], "origin": None,
        "commands": [{"command": command(script, *inputs, output), "stdin": None, "stdout": None}],
        "cores": 1, "memory": None,
    }


def job(id, depends_on=(), cores=1, memory=None, operation=None, product="p"):
    """A `Job` that reads nothing and runs `true` to make `{id}.txt`, for tests that need no DAG."""
    made = Artifact("out0", f"{id}.txt", Kind.FILE, product, ())
    return Job(id=id, operation=operation or f"op{id}", stage=(), work=Work("0" * 16, {}, None, identity.key((made,))), inputs=(),
               outputs=(Port("output", (made,)),), depends_on=tuple(depends_on), commands=(Run(("true",)),),
               verify=(), cores=cores, memory=memory)


def document(root, jobs, external_inputs):
    return {
        "version": 11,
        "generator": {"name": "spit", "version": "0.2.1"},
        "root": None if root is None else str(root),
        "external_inputs": [artifact(path) for path in external_inputs],
        "targets": [],
        "executables": [],
        "removed": [],
        "left_out": [],
        "pipeline_folder": None,
        "pipeline_files": [],
        "calls": [],
        "jobs": jobs,
    }


UPPER = "from pathlib import Path; import sys; Path(sys.argv[2]).write_text(Path(sys.argv[1]).read_text().upper())"
EXCLAIM = "from pathlib import Path; import sys; Path(sys.argv[2]).write_text(Path(sys.argv[1]).read_text() + '!')"


def sample(root):
    """source.txt -> work/upper.txt -> final.txt"""
    data = document(root, [
        job_entry(1, ["source.txt"], "work/upper.txt", UPPER, dependents=[2]),
        job_entry(2, ["work/upper.txt"], "final.txt", EXCLAIM, depends_on=[1]),
    ], ["source.txt"])
    return data


def read(data, root=None):
    return load_dag(io.StringIO(json.dumps(data)), root)


def run(dag, state, cores=2, force=False, memory=None, keep_failed=False, enforce_memory=True):
    decisions = plan(dag, state, force)
    options = Options(dag.layout.logs, cores, memory, keep_failed=keep_failed, enforce_memory=enforce_memory)
    results = asyncio.run(execute(dag, decisions, state, options, run_text.printer(dag)))
    state.close()
    return decisions, results


def error(result):
    """Why a run's job failed, in the words its report gives, or None when it did not."""
    return None if result.failure is None else run_text.failure_words(result.job, result.failure)


def state_at(root):
    return Store(Path(root) / ".spit-bash" / "state.jsonl")
