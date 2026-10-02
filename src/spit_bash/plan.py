"""Decide which jobs are current from paths, fingerprints and file stamps."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .dag import Dag, DagError, Job


def job_key(dag: Dag, job: Job) -> str:
    # Job IDs may change when SPIT resolves a different inventory.
    return json.dumps(sorted(str(dag.path(path)) for path in job.outputs), separators=(",", ":"))


def stamp(path: Path) -> list[int] | None:
    try:
        info = path.stat()
    except FileNotFoundError:
        return None
    return [info.st_size, info.st_mtime_ns] if path.is_file() else None


def stamps(dag: Dag, paths: tuple[str, ...]) -> dict[str, list[int] | None]:
    return {path: stamp(dag.path(path)) for path in paths}


class State:
    def __init__(self, path: Path):
        self.path = path
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise DagError(f"cannot read state at {path}: {exc}") from exc
            if not isinstance(data, dict) or data.get("version") != 1 or not isinstance(data.get("jobs"), dict):
                raise DagError(f"unsupported state format at {path}")
            self.jobs = data["jobs"]
        else:
            self.jobs = {}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({"version": 1, "jobs": self.jobs}, sort_keys=True, indent=2) + "\n"
        fd, temporary = tempfile.mkstemp(prefix=".state-", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


@dataclass(frozen=True)
class Decision:
    job: Job
    status: str  # run, skip, blocked
    reason: str


def plan(dag: Dag, state: State, force: bool = False) -> tuple[Decision, ...]:
    decisions: list[Decision] = []
    by_id: dict[int, Decision] = {}
    missing_external = {path for path in dag.external_inputs if stamp(dag.path(path)) is None}
    for job in dag.jobs:
        missing = [path for path in job.inputs if path in missing_external]
        blocked = [dep for dep in job.depends_on if by_id[dep].status == "blocked"]
        if missing:
            decision = Decision(job, "blocked", "missing external input: " + ", ".join(missing))
        elif blocked:
            decision = Decision(job, "blocked", "dependency blocked: " + ", ".join(map(str, blocked)))
        elif job.command is None:
            outputs = stamps(dag, job.outputs)
            if any(value is None for value in outputs.values()):
                decision = Decision(job, "blocked", "no command and output is missing")
            elif any(by_id[dep].status == "run" for dep in job.depends_on):
                decision = Decision(job, "blocked", "no command and dependency will run")
            elif any(value is None for value in stamps(dag, job.inputs).values()):
                decision = Decision(job, "blocked", "input is missing")
            elif job.inputs and max(value[1] for value in stamps(dag, job.inputs).values()) > min(value[1] for value in outputs.values()):
                decision = Decision(job, "blocked", "no command and output is older than an input")
            else:
                decision = Decision(job, "skip", "no command; outputs exist")
        elif any(by_id[dep].status == "run" for dep in job.depends_on):
            decision = Decision(job, "run", "dependency will run")
        elif any(value is None for value in stamps(dag, job.outputs).values()):
            decision = Decision(job, "run", "output missing")
        elif any(value is None for value in stamps(dag, job.inputs).values()):
            decision = Decision(job, "blocked", "input is missing")
        elif force:
            decision = Decision(job, "run", "forced")
        else:
            record = state.jobs.get(job_key(dag, job))
            if not isinstance(record, dict):
                decision = Decision(job, "run", "no successful run recorded")
            elif record.get("fingerprint") != job.fingerprint:
                decision = Decision(job, "run", "job fingerprint changed")
            elif record.get("inputs") != stamps(dag, job.inputs):
                decision = Decision(job, "run", "input changed")
            elif record.get("outputs") != stamps(dag, job.outputs):
                decision = Decision(job, "run", "output changed")
            else:
                decision = Decision(job, "skip", "current")
        decisions.append(decision)
        by_id[job.id] = decision
    return tuple(decisions)
