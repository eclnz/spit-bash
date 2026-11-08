"""The runnable jobs of a DAG, and the artifacts each reads and makes."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property
from pathlib import Path, PurePosixPath
from typing import TypeVar


T = TypeVar("T")


class DagError(ValueError):
    """The DAG cannot be run as supplied."""


class Kind(Enum):
    """What is at an artifact's path."""

    FILE = "file"
    FOLDER = "folder"


@dataclass(frozen=True)
class Artifact:
    """One file or folder a job reads or makes."""

    # Where the job holds the artifact, which a state record keys it by, so a
    # record survives a change of path or name: `in{port}.{place}` for an input,
    # `out{port}` for an output, counting ports in DAG order from 0. A path a job
    # reads at two places takes the first one's slot at both.
    slot: str
    # Relative to the dataset root.
    path: str
    kind: Kind
    product: str
    # The artifact's entities as (dimension, value) pairs, sorted.
    entities: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Port:
    """One of a job's named inputs or outputs, and its artifacts in DAG order; an output port has one."""

    name: str
    artifacts: tuple[Artifact, ...]


class Side(Enum):
    """Whether a check runs before the job's commands, on an input, or after them, on an output."""

    BEFORE = "before"
    AFTER = "after"


@dataclass(frozen=True)
class Check:
    """A check of one artifact on one of the job's ports, on the side that `side` names."""

    side: Side
    name: str
    port: str
    path: str
    command: tuple[str, ...]

    def describe(self) -> str:
        side = "input" if self.side is Side.BEFORE else "output"
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
class Work:
    """What a job's state record is matched against, as `reuse.identity` computes it at load."""

    # The work itself: the shape of what the job reads and writes, and its
    # commands, with paths as slots.
    fingerprint: str
    # `fingerprint`'s parts, each hashed on its own, by name, so a record can say which changed.
    parts: dict[str, str] = field(compare=False)
    # What a successful run records of the checks it passed; None without checks.
    checks_key: str | None
    # The state file's key for the job's record: what it makes, by product and entities.
    key: str


@dataclass(frozen=True)
class Job:
    id: int
    operation: str
    stage: tuple[str, ...]
    work: Work
    inputs: tuple[Port, ...]
    outputs: tuple[Port, ...]
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

    @cached_property
    def input_artifacts(self) -> tuple[Artifact, ...]:
        """Every input artifact, port by port in DAG order; a path read at two places appears twice."""
        return tuple(artifact for port in self.inputs for artifact in port.artifacts)

    @cached_property
    def output_artifacts(self) -> tuple[Artifact, ...]:
        """Every output artifact in DAG order."""
        return tuple(artifact for port in self.outputs for artifact in port.artifacts)

    @cached_property
    def input_paths(self) -> tuple[str, ...]:
        return tuple(artifact.path for artifact in self.input_artifacts)

    @cached_property
    def output_paths(self) -> tuple[str, ...]:
        return tuple(artifact.path for artifact in self.output_artifacts)

    @cached_property
    def identities(self) -> dict[str, str]:
        """Each input and output path's slot; callers do not change it."""
        return {artifact.path: artifact.slot for artifact in (*self.input_artifacts, *self.output_artifacts)}

    @property
    def input_paths_by_slot(self) -> dict[str, str]:
        """Each input's path by slot, as a new dict."""
        return {artifact.slot: artifact.path for artifact in self.input_artifacts}

    @property
    def output_paths_by_slot(self) -> dict[str, str]:
        """Each output's path by slot, as a new dict, which a state record keeps."""
        return {artifact.slot: artifact.path for artifact in self.output_artifacts}

    def keyed(self, by_path: dict[str, T]) -> dict[str, T]:
        """Values read by path, re-keyed by slot."""
        return {self.identities[path]: value for path, value in by_path.items()}

    @property
    def stdout(self) -> str | None:
        """The output file a command's standard output is written to, of which a job has one at most."""
        return next((run.stdout for run in self.commands if run.stdout is not None), None)

    @property
    def before(self) -> tuple[Check, ...]:
        return tuple(check for check in self.checks if check.side is Side.BEFORE)

    @property
    def after(self) -> tuple[Check, ...]:
        return tuple(check for check in self.checks if check.side is Side.AFTER)


@dataclass(frozen=True)
class Layout:
    """Where the runner keeps its own files: under the dataset root, and beside each output."""

    root: Path

    @property
    def folder(self) -> Path:
        return self.root / ".spit-bash"

    @property
    def state(self) -> Path:
        """The state file, unless `--state` names another."""
        return self.folder / "state.jsonl"

    @property
    def logs(self) -> Path:
        """The folder of job logs, unless `--logs` names another."""
        return self.folder / "logs"

    @staticmethod
    def partial(output: Path) -> Path:
        """The temporary file a command writes its standard output to, beside the output it becomes."""
        return output.with_name(f".{output.name}.spit-bash-partial")

    @staticmethod
    def previous(output: Path) -> Path:
        """Where an output's earlier version waits while its job runs, beside it so a move keeps it on one disk."""
        return output.with_name(f".{output.name}.spit-bash-previous")


@dataclass(frozen=True)
class Dag:
    root: Path
    jobs: tuple[Job, ...]
    external_inputs: tuple[str, ...]
    executables: tuple[str, ...]
    left_out: tuple[str, ...]
    # The paths of artifacts of kind `Kind.FOLDER`, for the membership tests that reading a stamp makes.
    folders: frozenset[str] = frozenset()
    # Each artifact's path under the root, joined and checked once by `load_dag`.
    absolute: dict[str, Path] = field(default_factory=dict, compare=False)

    @property
    def layout(self) -> Layout:
        return Layout(self.root)

    def path(self, relative: str) -> Path:
        path = self.absolute.get(relative)
        return self.checked(relative) if path is None else path

    def checked(self, relative: str) -> Path:
        """The path under the root, resolved now, so a link made since loading cannot lead outside it."""
        path = self.root.joinpath(*PurePosixPath(relative).parts)
        if not path.resolve().is_relative_to(self.root):
            raise DagError(f"path escapes dataset root: {relative}")
        return path
