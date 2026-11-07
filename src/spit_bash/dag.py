"""Turn a validated SPIT document into runnable jobs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Iterator, TextIO, TypeVar

from pydantic import ValidationError

from .schema import Artifact, Command, CommandPart, DirPart, HerePart, JobCommand, PathPart, SpitDag, SpitJob, StemPart


T = TypeVar("T")


class DagError(ValueError):
    """The DAG cannot be run as supplied."""


@dataclass(frozen=True)
class Output:
    """One produced artifact and the port that names it."""

    port: str
    path: str
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
class Run:
    """One of a job's commands: its arguments, and the input file it reads on its
    standard input and the output file its standard output is written to,
    relative to the root, or None when it does not redirect."""

    arguments: tuple[str, ...]
    stdin: str | None = None
    stdout: str | None = None

    def describe(self, index: int, of: int) -> str:
        """The command as an error names it: `command`, or `command 2` of several."""
        return "command" if of == 1 else f"command {index}"


@dataclass(frozen=True)
class Job:
    id: int
    operation: str
    stage: tuple[str, ...]
    fingerprint: str
    inputs: tuple[str, ...]
    produced: tuple[Output, ...]
    depends_on: tuple[int, ...]
    # The commands that make the outputs, run in order until one fails; empty
    # for an operation with none.
    commands: tuple[Run, ...]
    verify: tuple[tuple[str, ...], ...]
    checks: tuple[Check, ...] = ()
    # Where a job made by a call to an operation carried out by steps comes
    # from, innermost first; empty for a step written in the pipeline.
    origin: tuple[str, ...] = ()
    # What the job needs while its commands run: cores, and memory in bytes,
    # None when it claims none.
    cores: int = 1
    memory: int | None = None
    # Each input and output path's artifact identity, which a state record keys
    # its stamps and hashes by, so a record survives a change of path.
    identities: dict[str, str] = field(default_factory=dict, compare=False)

    def keyed(self, by_path: dict[str, T]) -> dict[str, T]:
        """Values read by path, re-keyed by artifact identity."""
        return {self.identities[path]: value for path, value in by_path.items()}

    @property
    def outputs(self) -> tuple[str, ...]:
        """Output paths in DAG order, for planning and state records."""
        return tuple(output.path for output in self.produced)

    @property
    def stdout(self) -> str | None:
        """The output file a command's standard output is written to, of which a job has one at most."""
        return next((run.stdout for run in self.commands if run.stdout is not None), None)

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
    # Each artifact's path under the root, joined and checked once by `load_dag`.
    absolute: dict[str, Path] = field(default_factory=dict, compare=False)

    def path(self, relative: str) -> Path:
        path = self.absolute.get(relative)
        return self.checked(relative) if path is None else path

    def checked(self, relative: str) -> Path:
        """The path under the root, resolved now, so a link made since loading cannot lead outside it."""
        path = self.root.joinpath(*PurePosixPath(relative).parts)
        if not path.resolve().is_relative_to(self.root):
            raise DagError(f"path escapes dataset root: {relative}")
        return path


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


def _identity(artifact: Artifact) -> list:
    """What an artifact is apart from where it is stored: product, entities and kind."""
    return [artifact.product, sorted(artifact.entities.items()), artifact.kind]


def _fingerprint(item: SpitJob) -> str:
    """The job's work as the runner identifies it: its operation, the artifacts it
    reads and writes, and its commands, with every path replaced by the identity of
    the artifact it names. Storage paths, checks, scheduling and where the job came
    from are left out, so moving the outputs changes nothing here."""
    named = {artifact.path: artifact for port in item.inputs.values() for artifact in port}
    named.update((artifact.path, artifact) for artifact in item.outputs.values())

    def of(path: str) -> list | str:
        artifact = named.get(path)
        return path if artifact is None else _identity(artifact)

    def part(value: CommandPart) -> list | str:
        if isinstance(value, str):
            return value
        if isinstance(value, PathPart):
            return ["path", of(value.path)]
        if isinstance(value, DirPart):
            return ["dir", of(value.of_path)]
        if isinstance(value, StemPart):
            return ["stem", of(value.of_path)]
        return ["here", value.here]

    def command(arguments: Command) -> list:
        return [[part(value) for value in argument] for argument in arguments]

    work = {
        "operation": item.operation,
        "inputs": {port: [_identity(a) for a in artifacts] for port, artifacts in item.inputs.items()},
        "outputs": {port: _identity(a) for port, a in item.outputs.items()},
        "commands": [
            [command(c.command), None if c.stdin is None else of(c.stdin), None if c.stdout is None else of(c.stdout)]
            for c in item.commands
        ],
        "verify": [command(c) for c in item.verify],
    }
    text = json.dumps(work, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _artifacts(document: SpitDag) -> Iterator[Artifact]:
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
        inputs = tuple(artifact.path for port in item.inputs.values() for artifact in port)
        produced = tuple(
            Output(port, artifact.path, artifact.product, tuple(sorted(artifact.entities.items())))
            for port, artifact in item.outputs.items()
        )
        outputs = tuple(output.path for output in produced)
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
            fingerprint=_fingerprint(item),
            inputs=inputs,
            produced=produced,
            depends_on=tuple(item.depends_on),
            commands=tuple(_run(command, document.pipeline_folder) for command in item.commands),
            verify=tuple(_command(command, document.pipeline_folder) for command in item.verify),
            checks=tuple(
                Check(check.when, check.check, check.port, check.path, _command(check.command, document.pipeline_folder))
                for check in item.checks
            ),
            origin=_origin(document, item),
            identities={artifact.path: json.dumps(_identity(artifact)[:2], separators=(",", ":"), sort_keys=True)
                        for artifact in (*(a for port in item.inputs.values() for a in port), *item.outputs.values())},
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
        if path not in dag.absolute:
            dag.absolute[path] = dag.checked(path)
    return dag
