"""Turn a validated SPIT document into runnable jobs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TextIO

from pydantic import ValidationError

from .schema import Command, DirPart, PathPart, SpitDag, StemPart


class DagError(ValueError):
    """The DAG cannot be run as supplied."""


@dataclass(frozen=True)
class Job:
    id: int
    operation: str
    fingerprint: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    depends_on: tuple[int, ...]
    command: tuple[str, ...] | None
    verify: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class Dag:
    root: Path
    jobs: tuple[Job, ...]
    external_inputs: tuple[str, ...]
    executables: tuple[str, ...]
    left_out: tuple[str, ...]

    def path(self, relative: str) -> Path:
        path = self.root.joinpath(*PurePosixPath(relative).parts)
        if not path.resolve().is_relative_to(self.root):
            raise DagError(f"path escapes dataset root: {relative}")
        return path


def _command(command: Command) -> tuple[str, ...]:
    arguments = []
    for argument in command:
        parts = []
        for part in argument:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, PathPart):
                parts.append(part.path)
            elif isinstance(part, DirPart):
                parts.append(part.dir)
            elif isinstance(part, StemPart):
                parts.append(part.stem)
        arguments.append("".join(parts))
    if not arguments[0]:
        raise DagError("command has no executable")
    return tuple(arguments)


def load_dag(source: TextIO, root_override: str | None = None) -> Dag:
    try:
        document = SpitDag.model_validate_json(source.read())
    except (ValidationError, UnicodeError) as exc:
        raise DagError(f"invalid SPIT DAG: {exc}") from exc
    root_value = root_override if root_override is not None else document.root
    if not root_value:
        raise DagError("DAG has no root; pass --root DIRECTORY")
    root = Path(root_value).expanduser().resolve()
    if not root.is_dir():
        raise DagError(f"dataset root is not a directory: {root}")

    external_paths = tuple(artifact.path for artifact in document.external_inputs)
    external_set = set(external_paths)
    jobs = []
    seen_ids: set[int] = set()
    producers: dict[str, int] = {}
    for item in document.jobs:
        if item.id in seen_ids:
            raise DagError(f"duplicate job id: {item.id}")
        inputs = tuple(artifact.path for port in item.inputs.values() for artifact in port)
        outputs = tuple(artifact.path for artifact in item.outputs.values())
        if len(item.depends_on) != len(set(item.depends_on)) or any(dep not in seen_ids for dep in item.depends_on):
            raise DagError(f"job {item.id} dependencies must name earlier jobs once each")
        for path in outputs:
            if path in producers or path in external_set:
                raise DagError(f"multiple jobs or sources claim path: {path}")
            producers[path] = item.id
        jobs.append(Job(
            id=item.id,
            operation=item.operation,
            fingerprint=item.fingerprint,
            inputs=inputs,
            outputs=outputs,
            depends_on=tuple(item.depends_on),
            command=None if item.command is None else _command(item.command),
            verify=tuple(_command(command) for command in item.verify),
        ))
        seen_ids.add(item.id)

    for job in jobs:
        expected = {producers[path] for path in job.inputs if path in producers}
        if set(job.depends_on) != expected:
            raise DagError(f"job {job.id} dependencies do not match its input producers")
        for path in job.inputs:
            if path not in producers and path not in external_set:
                raise DagError(f"job {job.id} input is neither produced nor external: {path}")
    dag = Dag(
        root,
        tuple(jobs),
        external_paths,
        tuple(document.executables),
        tuple(item.identity for item in document.left_out),
    )
    for path in (*external_paths, *(path for job in jobs for path in (*job.inputs, *job.outputs))):
        dag.path(path)
    return dag
