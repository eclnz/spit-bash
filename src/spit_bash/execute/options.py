"""How a run runs its jobs, as `run`'s options set it."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..reuse.evidence import Compare


@dataclass(frozen=True)
class Options:
    # The folder each job's log is written to.
    logs: Path
    # Cores to share among the jobs that run at once.
    cores: int = 1
    # Memory in bytes to share among them, or None for no limit.
    memory: int | None = None
    # As for `plan.plan`, for the jobs planned `maybe`; under `Compare.STAT` a run records no hashes.
    compare: Compare = Compare.CONTENT
    # Keep what a failed command wrote at its job's outputs, rather than removing it.
    keep_failed: bool = False
    # Kill a command whose process group holds more than its job declares.
    enforce_memory: bool = True
    # Start each line of a job's log with the time the runner received it.
    timestamp_logs: bool = False
    # When the run started, as `2026-11-10T14-44-52`, which names the copy of an edited output it keeps.
    started: str = "run"
