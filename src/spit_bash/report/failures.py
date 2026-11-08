"""Failed jobs grouped by what went wrong, and lists of job ids, for what `run` prints."""

from __future__ import annotations

from pathlib import Path

from ..run import Result, log_tail
from . import LISTED


def _normalised(job_files: tuple[str, ...], root: Path, lines: list[str]) -> tuple[str, ...]:
    """A log's last lines without what is particular to the job: its command line and the paths of its files."""
    names = sorted({*job_files, *(str(root / path) for path in job_files)}, key=len, reverse=True)
    kept = []
    for line in lines:
        if line.lstrip().startswith("$ "):
            continue
        for file in names:
            line = line.replace(file, "<file>")
        kept.append(line)
    return tuple(kept)


def failure_groups(root: Path, failures: list[Result]) -> list[list[Result]]:
    """The failed jobs grouped by what went wrong: the same error and the same last lines of the log."""
    groups: dict[tuple, list[Result]] = {}
    for result in failures:
        files = (*result.job.input_paths, *result.job.output_paths)
        tail = log_tail(result.log) if result.log is not None else []
        groups.setdefault((result.error, _normalised(files, root, tail)), []).append(result)
    return list(groups.values())


def ids(results: list[Result]) -> str:
    shown = ", ".join(str(result.job.id) for result in results[:LISTED])
    return shown + (f", and {len(results) - LISTED} more" if len(results) > LISTED else "")
