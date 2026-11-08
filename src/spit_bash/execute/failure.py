"""Why a job planned to run did not succeed, as data: `report.run_text` gives the words."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Union

from ..files import Stamps
from ..model import Check
from ..plan import Decision
from .process import CommandEnd


class Phase(Enum):
    """Which of a job's command lists a failed command is in."""

    VERIFY = "verify"
    COMMAND = "command"


@dataclass(frozen=True)
class InputDisappeared:
    """An input the plan found was gone when the job was about to start."""


@dataclass(frozen=True)
class NoCommand:
    """The job has no command to run."""


@dataclass(frozen=True)
class CheckArtifactMissing:
    """An input check's artifact was not there to check."""

    check: Check


@dataclass(frozen=True)
class CheckFailed:
    check: Check
    end: CommandEnd


@dataclass(frozen=True)
class CommandFailed:
    """Command `index`, from 1, of the job's `phase` list ended other than with status 0."""

    phase: Phase
    index: int
    end: CommandEnd


@dataclass(frozen=True)
class StdoutNotMoved:
    """A command succeeded, but its standard output could not be renamed onto the output `path`."""

    path: str
    error: str


@dataclass(frozen=True)
class InputChanged:
    """The job's inputs changed while it ran.

    `before` and `after` are the inputs' stamps when it started and once its
    commands ended; `folders` are the input folders, sorted, whose entries or
    files changed.
    """

    before: Stamps
    after: Stamps
    folders: tuple[str, ...]


@dataclass(frozen=True)
class OutputsMissing:
    """The commands succeeded but did not make these outputs, each as its port's name and path."""

    missing: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class PlaceFailed:
    """A job planned `place` could not have its outputs moved: `error` says why.

    With `put_back`, every file moved was put back where it was; without, putting
    them back failed too, and `error` is that failure.
    """

    error: str
    put_back: bool


@dataclass(frozen=True)
class DependencyFailed:
    """The job did not run because a job it needs failed; `first` is the job whose own failure it is."""

    first: int


@dataclass(frozen=True)
class Blocked:
    """A job planned `maybe` was blocked when judged again, once its upstream jobs had finished."""

    decision: Decision


Failure = Union[InputDisappeared, NoCommand, CheckArtifactMissing, CheckFailed, CommandFailed, StdoutNotMoved,
                InputChanged, OutputsMissing, PlaceFailed, DependencyFailed, Blocked]
