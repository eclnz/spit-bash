"""Shared builders for the tests: a two-job DAG and helpers to load and run it."""

import asyncio
import io
import json
import sys
from pathlib import Path

from spit_bash.dag import load_dag
from spit_bash.plan import State, plan
from spit_bash.run import execute

FIXTURE = Path(__file__).parent / "fixtures" / "command_demo.spitdag"


def artifact(path):
    return {"product": path, "entities": {}, "type": None, "path": path}


def command(script, *paths):
    return [[sys.executable], ["-c"], [script], *[[{"path": path}] for path in paths]]


def job(id, inputs, output, script, depends_on=(), dependents=(), fingerprint=None):
    return {
        "id": id, "operation": f"op{id}", "fingerprint": fingerprint or f"{id:016x}",
        "stage": [], "dependents": list(dependents),
        "inputs": {"input": [artifact(path) for path in inputs]},
        "outputs": {"output": artifact(output)},
        "depends_on": list(depends_on), "verify": [],
        "command": command(script, *inputs, output),
    }


def document(root, jobs, external_inputs):
    return {
        "version": 4,
        "generator": {"name": "spit", "version": "0.2.1"},
        "root": None if root is None else str(root),
        "external_inputs": [artifact(path) for path in external_inputs],
        "targets": [],
        "executables": [],
        "removed": [],
        "left_out": [],
        "jobs": jobs,
    }


UPPER = "from pathlib import Path; import sys; Path(sys.argv[2]).write_text(Path(sys.argv[1]).read_text().upper())"
EXCLAIM = "from pathlib import Path; import sys; Path(sys.argv[2]).write_text(Path(sys.argv[1]).read_text() + '!')"


def sample(root):
    """source.txt -> work/upper.txt -> final.txt"""
    data = document(root, [
        job(1, ["source.txt"], "work/upper.txt", UPPER, dependents=[2]),
        job(2, ["work/upper.txt"], "final.txt", EXCLAIM, depends_on=[1]),
    ], ["source.txt"])
    data["jobs"][0]["fingerprint"] = "a" * 16
    data["jobs"][1]["fingerprint"] = "b" * 16
    return data


def read(data, root=None):
    return load_dag(io.StringIO(json.dumps(data)), root)


def run(dag, state, workers=2, force=False):
    decisions = plan(dag, state, force)
    results = asyncio.run(execute(dag, decisions, state, workers, dag.root / ".spit-bash" / "logs"))
    state.close()
    return decisions, results


def state_at(root):
    return State(Path(root) / ".spit-bash" / "state.jsonl")
