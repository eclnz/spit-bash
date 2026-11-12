"""Turn a validated SPIT document into runnable jobs."""

from __future__ import annotations

import gc
import os
from pathlib import Path, PurePosixPath
from typing import Iterable, Iterator, TextIO

from pydantic import ValidationError

from . import schema
from .model import Artifact, Check, Dag, DagError, Job, Kind, Port, Run, Side
from .reuse import identity
from .schema import Command, DirPart, HerePart, JobCommand, PathPart, SpitDag, SpitJob, StemPart


def _here(part: HerePart, folder: str | None) -> str:
    if folder is None:
        raise DagError("a command uses the pipeline's folder, but `pipeline_folder` is null")
    inside = PurePosixPath(part.here)
    if inside.is_absolute() or ".." in inside.parts:
        raise DagError(f"`here` names {part.here}, which is not a folder under the pipeline's")
    return str(PurePosixPath(folder) / inside)


def _command(command: Command, folder: str | None = None, shown: dict[str, str] | None = None) -> tuple[str, ...]:
    """A command's arguments as text; `folder` is the pipeline's, which a `here` part is under.

    `shown` gives the text of each path under a dataset root apart from the
    output root, which commands run from: its absolute path.
    """
    shown = shown or {}
    arguments = []
    for argument in command:
        parts = []
        for part in argument:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, PathPart):
                parts.append(shown.get(part.path, part.path))
            elif isinstance(part, DirPart):
                parts.append(_dir(part, shown))
            elif isinstance(part, StemPart):
                parts.append(part.stem)
            elif isinstance(part, HerePart):
                parts.append(_here(part, folder))
        arguments.append("".join(parts))
    if not arguments[0]:
        raise DagError("command has no executable")
    return tuple(arguments)


def _dir(part: DirPart, shown: dict[str, str]) -> str:
    """A `dir` part's text: the folder as written, or under a dataset root apart from the output root, its absolute path."""
    full = shown.get(part.of_path)
    # SPIT writes a `dir` part as the folder its file is in.
    return part.dir if full is None else str(PurePosixPath(full).parent)


def _artifact(declared: schema.Artifact, slots: dict[str, str]) -> Artifact:
    return Artifact(slots[declared.path], declared.path, Kind(declared.kind), declared.product,
                    tuple(sorted(declared.entities.items())))


def _ports(item: SpitJob, slots: dict[str, str]) -> tuple[tuple[Port, ...], tuple[Port, ...]]:
    """The job's input ports and output ports, in DAG order."""
    inputs = tuple(Port(name, tuple(_artifact(a, slots) for a in artifacts)) for name, artifacts in item.inputs.items())
    outputs = tuple(Port(name, (_artifact(a, slots),)) for name, a in item.outputs.items())
    return inputs, outputs


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


def _run(command: JobCommand, folder: str | None, shown: dict[str, str]) -> Run:
    return Run(_command(command.command, folder, shown), command.stdin, command.stdout)


def _absolute(dag: Dag, uses: dict[str, str]) -> dict[str, Path]:
    """Each path joined to its root, with the check `Dag.checked` makes: resolved, it stays under the root.

    Keep in step with `Dag.checked`, which gives the error for a path that leads out. Paths share a few
    folders, so each folder is resolved once, and a file needs resolving only when it is itself a link.
    """
    folders: dict[tuple[str, str], str] = {}
    absolute: dict[str, Path] = {}
    for relative, use in uses.items():
        base = str(dag.base(relative))
        under = base.rstrip("/") + "/"
        joined = under + relative
        if os.path.islink(joined):
            real = os.path.realpath(joined)
        else:
            folder, _, name = relative.rpartition("/")
            real_folder = folders.get((base, folder))
            if real_folder is None:
                real_folder = folders[base, folder] = os.path.realpath(under + folder)
            real = f"{real_folder.rstrip('/')}/{name}"
        if real != base and not real.startswith(under):
            # Resolved again, link by link, to say which link leads out and where.
            absolute[relative] = dag.checked(relative, use)
            continue
        absolute[relative] = Path(joined)
    return absolute


def load_dag(source: TextIO, root_override: str | None = None) -> Dag:
    """The DAG `source` holds, checked, with its root from `root_override` when given."""
    # Loading makes millions of objects, nearly all kept for as long as the DAG is. With the cyclic collector
    # on, each of its passes walks them all, which more than doubles the time to load; what little is garbage
    # waits for the first pass after.
    enabled = gc.isenabled()
    gc.disable()
    try:
        return _load(source, root_override)
    finally:
        if enabled:
            gc.enable()


def _load(source: TextIO, root_override: str | None) -> Dag:
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
    # Outputs go under the dataset root unless the DAG names another folder; a
    # root given on the command line moves them with it.
    output_root = None
    if document.output_root is not None and document.output_root != document.root:
        output_root = Path(document.output_root).expanduser().resolve()
        if output_root == root:
            output_root = None

    _check_origins(document)
    folders = frozenset(artifact.path for artifact in _artifacts(document) if artifact.kind == "folder")
    for artifact in _artifacts(document):
        if (artifact.kind == "folder") != (artifact.path in folders):
            raise DagError(f"path is both a file and a folder: {artifact.path}")
    unders: dict[str, str] = {}
    for artifact in _artifacts(document):
        if unders.setdefault(artifact.path, artifact.under) != artifact.under:
            raise DagError(f"path is under both roots: {artifact.path}")
    in_dataset = frozenset(path for path, under in unders.items() if under == "root") if output_root else frozenset()
    shown = {path: str(root.joinpath(*PurePosixPath(path).parts)) for path in in_dataset}
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
        slots = identity.slots(item)
        inputs, produced = _ports(item, slots)
        commands = tuple(_run(command, document.pipeline_folder, shown) for command in item.commands)
        verify = tuple(_command(command, document.pipeline_folder, shown) for command in item.verify)
        checks = tuple(
            Check(Side(check.when), check.check, check.port, check.path,
                  _command(check.command, document.pipeline_folder, shown))
            for check in item.checks
        )
        origin = _origin(document, item)
        # Before the checks key, which names each checked path by its slot.
        for check in item.checks:
            ports = item.inputs if check.when == "before" else {k: [v] for k, v in item.outputs.items()}
            if not any(artifact.path == check.path for artifact in ports.get(check.port, ())):
                raise DagError(f"job {item.id}: check {check.check} names {check.path}, "
                               f"which is not on its {'input' if check.when == 'before' else 'output'} {check.port}")
        jobs.append(Job(
            id=item.id,
            operation=item.operation,
            stage=tuple(item.stage),
            work=identity.work(item, slots, checks, tuple(a for port in produced for a in port.artifacts), shown),
            inputs=inputs,
            outputs=produced,
            depends_on=tuple(item.depends_on),
            commands=commands,
            verify=verify,
            checks=checks,
            origin=origin,
            cores=item.cores,
            memory=item.memory,
        ))
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
        output_root=output_root,
        in_dataset=in_dataset,
    )
    dag.absolute.update(_absolute(dag, _uses(external_set, jobs)))
    return dag


def _uses(sources: set[str], jobs: list[Job]) -> dict[str, str]:
    """Each artifact's path, in DAG order, with the first job that reads or writes it, for an error to name."""
    uses: dict[str, str] = {}
    for job in jobs:
        for path in job.input_paths:
            uses.setdefault(path, f"Job {job.id} ({job.operation}) reads it{' as a source' if path in sources else ''}")
        for path in job.output_paths:
            uses.setdefault(path, f"Job {job.id} ({job.operation}) writes it")
    for path in sorted(sources - uses.keys()):
        uses[path] = "It is a source no job reads"
    return uses
