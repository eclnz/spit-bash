"""Turn a validated SPIT document into runnable jobs."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
from typing import Iterator, TextIO

from pydantic import ValidationError

from . import schema
from .model import Artifact, Check, Dag, DagError, Job, Kind, Port, Run, Side
from .schema import Command, CommandPart, DirPart, HerePart, JobCommand, PathPart, SpitDag, SpitJob, StemPart


def _here(part: HerePart, folder: str | None) -> str:
    if folder is None:
        raise DagError("a command uses the pipeline's folder, but `pipeline_folder` is null")
    inside = PurePosixPath(part.here)
    if inside.is_absolute() or ".." in inside.parts:
        raise DagError(f"`here` names {part.here}, which is not a folder under the pipeline's")
    return str(PurePosixPath(folder) / inside)


def _command(command: Command, folder: str | None = None) -> tuple[str, ...]:
    """A command's arguments as text; `folder` is the pipeline's, which a `here` part is under."""
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
            elif isinstance(part, HerePart):
                parts.append(_here(part, folder))
        arguments.append("".join(parts))
    if not arguments[0]:
        raise DagError("command has no executable")
    return tuple(arguments)


def _slots(item: SpitJob) -> dict[str, str]:
    """Each input and output path's slot, as `Artifact.slot` gives it.

    An artifact is the same work whatever it is called, so a record names a file
    by where the job holds it, not by product or entities.
    """
    slots: dict[str, str] = {}
    for port, artifacts in enumerate(item.inputs.values()):
        for place, artifact in enumerate(artifacts):
            slots.setdefault(artifact.path, f"in{port}.{place}")
    for port, artifact in enumerate(item.outputs.values()):
        slots.setdefault(artifact.path, f"out{port}")
    return slots


def _artifact(declared: schema.Artifact, slots: dict[str, str]) -> Artifact:
    return Artifact(slots[declared.path], declared.path, Kind(declared.kind), declared.product,
                    tuple(sorted(declared.entities.items())))


def _ports(item: SpitJob, slots: dict[str, str]) -> tuple[tuple[Port, ...], tuple[Port, ...]]:
    """The job's input ports and output ports, in DAG order."""
    inputs = tuple(Port(name, tuple(_artifact(a, slots) for a in artifacts)) for name, artifacts in item.inputs.items())
    outputs = tuple(Port(name, (_artifact(a, slots),)) for name, a in item.outputs.items())
    return inputs, outputs


def _fingerprint(item: SpitJob, slots: dict[str, str]) -> tuple[str, dict[str, str]]:
    """The job's work as the runner identifies it: the shape of what it reads and
    writes, and its commands, with every path replaced by its place in the job.
    Names, storage paths, checks, scheduling and where the job came from are left
    out, so renaming or moving things changes nothing here. What the inputs hold
    is checked against the record's hashes, not here."""

    def of(path: str) -> str:
        return slots.get(path, path)

    def part(value: CommandPart) -> list | str:
        if isinstance(value, str):
            return value
        if isinstance(value, PathPart):
            return ["path", of(value.path)]
        if isinstance(value, DirPart):
            # The tool is given this text, so a change to it is a change to the work.
            return ["dir", value.dir, of(value.of_path)]
        if isinstance(value, StemPart):
            return ["stem", value.stem, of(value.of_path)]
        return ["here", value.here]

    def command(arguments: Command) -> list:
        return [[part(value) for value in argument] for argument in arguments]

    # Ports are listed in order, without their names: a renamed operation or port is the same work.
    work = {
        "inputs": [[[slots[a.path], a.kind] for a in artifacts] for artifacts in item.inputs.values()],
        "outputs": [[slots[a.path], a.kind] for a in item.outputs.values()],
        "commands": [
            [command(c.command), None if c.stdin is None else of(c.stdin), None if c.stdout is None else of(c.stdout)]
            for c in item.commands
        ],
        "verify": [command(c) for c in item.verify],
    }
    def digest(value: object) -> str:
        return hashlib.sha256(json.dumps(value, separators=(",", ":"), sort_keys=True).encode()).hexdigest()[:16]

    # The parts are hashed apart so that a record can say which of them changed.
    return digest(work), {name: digest(value) for name, value in work.items()}


def _artifacts(document: SpitDag) -> Iterator[schema.Artifact]:
    """Every artifact the document names, wherever it appears."""
    yield from document.external_inputs
    yield from document.targets
    for job in document.jobs:
        for port in job.inputs.values():
            yield from port
        yield from job.outputs.values()


def _origin(document: SpitDag, job: SpitJob) -> tuple[str, ...]:
    """The body step that made `job`, then each call it is nested in, as lines to show."""
    if job.origin is None:
        return ()
    files = document.pipeline_files
    calls = document.calls

    def place(file: int | None, line: int) -> str:
        return f"{files[file].path} line {line}" if file is not None else f"line {line}"

    lines = [f"step at {place(job.origin.file, job.origin.line)}"]
    if job.origin.call is None:
        return tuple(lines)
    call = calls[job.origin.call]
    while True:
        lines.append(f"in `{call.instance} = {call.operation}(...)` at {place(call.at.file, call.at.line)}")
        if call.parent is None:
            return tuple(lines)
        call = calls[call.parent]


def _check_origins(document: SpitDag) -> None:
    """Every reference into the pipeline's files and calls is in range."""
    files = len(document.pipeline_files)
    calls = document.calls
    for position, call in enumerate(calls):
        if any(file is not None and file >= files for file in (call.file, call.at.file)):
            raise DagError(f"call {position} names a file that `pipeline_files` does not hold")
        # A call's parent comes before it, so following parents ends.
        if call.parent is not None and call.parent >= position:
            raise DagError(f"call {position} names a parent that is not an earlier call")
    for job in document.jobs:
        if job.origin is None:
            continue
        if job.origin.call is not None and job.origin.call >= len(calls):
            raise DagError(f"job {job.id}: `origin` names a call that `calls` does not hold")
        if job.origin.file is not None and job.origin.file >= files:
            raise DagError(f"job {job.id}: `origin` names a file that `pipeline_files` does not hold")


def _check_streams(job: SpitJob, folders: frozenset[str]) -> None:
    """Each command's `stdin` names one of the job's input files, and its `stdout` one of its
    output files; one command at most has a `stdout`."""
    if sum(command.stdout is not None for command in job.commands) > 1:
        raise DagError(f"job {job.id}: more than one of its commands has a `stdout`")
    inputs = [artifact for port in job.inputs.values() for artifact in port]
    outputs = list(job.outputs.values())
    for command in job.commands:
        for field_name, path, ports, side in (
            ("stdin", command.stdin, inputs, "inputs"),
            ("stdout", command.stdout, outputs, "outputs"),
        ):
            if path is None:
                continue
            if not any(artifact.path == path for artifact in ports):
                raise DagError(f"job {job.id}: `{field_name}` names {path}, which is not one of its {side}")
            if path in folders:
                raise DagError(f"job {job.id}: `{field_name}` names {path}, which is a folder, not a file")


def _run(command: JobCommand, folder: str | None) -> Run:
    return Run(_command(command.command, folder), command.stdin, command.stdout)


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

    _check_origins(document)
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
        _check_streams(item, folders)
        outputs = tuple(artifact.path for artifact in item.outputs.values())
        if len(item.depends_on) != len(set(item.depends_on)) or any(dep not in seen_ids for dep in item.depends_on):
            raise DagError(f"job {item.id} dependencies must name earlier jobs once each")
        for path in outputs:
            if path in producers or path in external_set:
                raise DagError(f"multiple jobs or sources claim path: {path}")
            producers[path] = item.id
        slots = _slots(item)
        fingerprint, parts = _fingerprint(item, slots)
        inputs, produced = _ports(item, slots)
        jobs.append(Job(
            id=item.id,
            operation=item.operation,
            stage=tuple(item.stage),
            fingerprint=fingerprint,
            parts=parts,
            inputs=inputs,
            outputs=produced,
            depends_on=tuple(item.depends_on),
            commands=tuple(_run(command, document.pipeline_folder) for command in item.commands),
            verify=tuple(_command(command, document.pipeline_folder) for command in item.verify),
            checks=tuple(
                Check(Side(check.when), check.check, check.port, check.path, _command(check.command, document.pipeline_folder))
                for check in item.checks
            ),
            origin=_origin(document, item),
            cores=item.cores,
            memory=item.memory,
        ))
        for check in item.checks:
            ports = item.inputs if check.when == "before" else {k: [v] for k, v in item.outputs.items()}
            if not any(artifact.path == check.path for artifact in ports.get(check.port, ())):
                raise DagError(f"job {item.id}: check {check.check} names {check.path}, "
                               f"which is not on its {'input' if check.when == 'before' else 'output'} {check.port}")
        seen_ids.add(item.id)

    for job in jobs:
        expected = {producers[path] for path in job.input_paths if path in producers}
        if set(job.depends_on) != expected:
            raise DagError(f"job {job.id} dependencies do not match its input producers")
        for path in job.input_paths:
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
    for path in (*external_paths, *(path for job in jobs for path in (*job.input_paths, *job.output_paths))):
        if path not in dag.absolute:
            dag.absolute[path] = dag.checked(path)
    return dag
