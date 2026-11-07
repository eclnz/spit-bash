"""Copy a job's recorded outputs to the paths its pipeline now gives them."""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .dag import Dag, DagError, Job
from .plan import State, job_key, stamp


@dataclass(frozen=True)
class Migration:
    job: Job
    # (recorded path, current path) of each output to copy; empty when `reason` says why not.
    copies: tuple[tuple[str, str], ...]
    reason: str


def _plan_job(dag: Dag, job: Job, state: State) -> Migration | None:
    """The copies that bring a job's recorded outputs to its current paths, or None when it has nothing to move."""
    record = state.jobs.get(job_key(job))
    if record is None or record.output_paths is None or record.fingerprint != job.fingerprint:
        return None
    current = {job.identities[path]: path for path in job.outputs}
    moved = {key: path for key, path in current.items() if record.output_paths.get(key, path) != path}
    if not moved:
        return None
    copies = []
    for key, new in current.items():
        old = record.output_paths.get(key)
        if old is None:
            return Migration(job, (), "the record does not say where an output was made")
        if old == new:
            return Migration(job, (), "only some outputs moved; the rest are not copied")
        try:
            old_path, new_path = dag.checked(old), dag.checked(new)
        except DagError as exc:
            return Migration(job, (), str(exc))
        if os.path.lexists(new_path):
            return Migration(job, (), f"{new} already exists")
        if stamp(old_path, new in dag.folders) != record.outputs.get(key):
            return Migration(job, (), f"{old} is missing or changed since the run")
        copies.append((old, new))
    return Migration(job, tuple(copies), "outputs recorded at other paths")


def find(dag: Dag, state: State) -> tuple[Migration, ...]:
    """Every job whose recorded outputs are at other paths than its current ones."""
    found = (_plan_job(dag, job, state) for job in dag.jobs if job.commands)
    return tuple(migration for migration in found if migration is not None)


def _copy(dag: Dag, old: str, new: str) -> None:
    """Copy to a temporary name beside the destination, then rename, so a partial copy never sits at it."""
    source, target = dag.checked(old), dag.checked(new)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".migrate-", dir=target.parent)) / target.name
    try:
        if source.is_dir():
            shutil.copytree(source, temporary)
        else:
            shutil.copy2(source, temporary)
        os.rename(temporary, target)
    finally:
        shutil.rmtree(temporary.parent, ignore_errors=True)


def apply(dag: Dag, migrations: tuple[Migration, ...]) -> None:
    """Copy each migration's files. The files at the recorded paths are left where they are."""
    for migration in migrations:
        for old, new in migration.copies:
            _copy(dag, old, new)
