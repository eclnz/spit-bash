"""Turn a validated SPIT document into runnable jobs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterator, TextIO

from pydantic import ValidationError

from .schema import Artifact, Command, DirPart, PathPart, SpitDag, StemPart


class DagError(ValueError):
    """The DAG cannot be run as supplied."""


@dataclass(frozen=True)
class Made:
    """What one output is, for choosing jobs by product and entities."""

    product: str
    entities: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Check:
    """A check of one artifact: `before` the command on an input, or `after` it on an output."""

    when: str
    name: str
    port: str
    path: str
    command: tuple[str, ...]

    def describe(self) -> str:
        side = "input" if self.when == "before" else "output"
        return f"check {self.name} on {side} {self.port} {self.path}"


@dataclass(frozen=True)
class Job:
    id: int
    operation: str
    stage: tuple[str, ...]
    fingerprint: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    made: tuple[Made, ...]
    depends_on: tuple[int, ...]
    command: tuple[str, ...] | None
    verify: tuple[tuple[str, ...], ...]
    checks: tuple[Check, ...] = ()

    @property
    def before(self) -> tuple[Check, ...]:
        return tuple(check for check in self.checks if check.when == "before")

    @property
    def after(self) -> tuple[Check, ...]:
        return tuple(check for check in self.checks if check.when == "after")

    @property
    def checks_key(self) -> str | None:
        """What a successful run records of the checks it passed; None without checks."""
        if not self.checks:
            return None
        text = json.dumps([[c.when, c.name, c.port, c.path, list(c.command)] for c in self.checks],
                          separators=(",", ":"))
        return hashlib.sha256(text.encode()).hexdigest()[:16]


@dataclass(frozen=True)
class Dag:
    root: Path
    jobs: tuple[Job, ...]
    external_inputs: tuple[str, ...]
    executables: tuple[str, ...]
    left_out: tuple[str, ...]
    # The paths of artifacts that are folders rather than files.
    folders: frozenset[str] = frozenset()

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


def _artifacts(document: SpitDag) -> Iterator[Artifact]:
    """Every artifact the document names, wherever it appears."""
    yield from document.external_inputs
    yield from document.targets
    for job in document.jobs:
        for port in job.inputs.values():
            yield from port
        yield from job.outputs.values()


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

    if document.version == 4 and any(
        artifact.kind == "folder" for artifact in _artifacts(document)
    ):
        raise DagError("a version 4 DAG has no folders; `kind` came with version 5")
    if document.version >= 7:
        if document.pipeline_files is None or document.calls is None:
            raise DagError("a version 7 DAG must have `pipeline_files` and `calls`")
        for index, call in enumerate(document.calls):
            if call.parent is not None and call.parent >= index:
                raise DagError(f"call {index} must name an earlier parent")
            if any(file is not None and file >= len(document.pipeline_files)
                   for file in (call.file, call.at.file)):
                raise DagError(f"call {index} names an unknown pipeline file")
    elif document.pipeline_files is not None or document.calls is not None:
        raise DagError("`pipeline_files` and `calls` came with version 7")
    folders = frozenset(artifact.path for artifact in _artifacts(document) if artifact.kind == "folder")
    for artifact in _artifacts(document):
        if (artifact.kind == "folder") != (artifact.path in folders):
            raise DagError(f"path is both a file and a folder: {artifact.path}")
    external_paths = tuple(artifact.path for artifact in document.external_inputs)
    external_set = set(external_paths)
    jobs = []
    seen_ids: set[int] = set()
    producers: dict[str, int] = {}
    for item in document.jobs:
        if item.id in seen_ids:
            raise DagError(f"duplicate job id: {item.id}")
        if (item.checks is not None) != (document.version >= 6):
            raise DagError(f"job {item.id}: `checks` came with version 6, and every job of one has them")
        if ("origin" in item.model_fields_set) != (document.version >= 7):
            raise DagError(f"job {item.id}: `origin` came with version 7, and every job of one has it")
        if item.origin is not None and item.origin.call >= len(document.calls or ()):
            raise DagError(f"job {item.id} names an unknown call")
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
            stage=tuple(item.stage),
            fingerprint=item.fingerprint,
            inputs=inputs,
            outputs=outputs,
            made=tuple(
                Made(artifact.product, tuple(sorted(artifact.entities.items())))
                for artifact in item.outputs.values()
            ),
            depends_on=tuple(item.depends_on),
            command=None if item.command is None else _command(item.command),
            verify=tuple(_command(command) for command in item.verify),
            checks=tuple(
                Check(check.when, check.check, check.port, check.path, _command(check.command))
                for check in item.checks or ()
            ),
        ))
        for check in item.checks or ():
            ports = item.inputs if check.when == "before" else {k: [v] for k, v in item.outputs.items()}
            if not any(artifact.path == check.path for artifact in ports.get(check.port, ())):
                raise DagError(f"job {item.id}: check {check.check} names {check.path}, "
                               f"which is not on its {'input' if check.when == 'before' else 'output'} {check.port}")
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
        folders,
    )
    for path in (*external_paths, *(path for job in jobs for path in (*job.inputs, *job.outputs))):
        dag.path(path)
    return dag
