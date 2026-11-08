"""What `run` prints: a line as each job starts and ends, its failures grouped by cause, and its summary."""

from __future__ import annotations

import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from ..display import id_width, labels, name
from ..execute import events
from ..execute.events import Event
from ..execute.failure import (Blocked, CheckArtifactMissing, CheckFailed, CommandFailed, DependencyFailed, Failure,
                               InputChanged, InputDisappeared, NoCommand, OutputsMissing, Phase, StdoutNotMoved)
from ..execute.process import CommandEnd, Exited, KilledMemory, KilledSignal, NotStarted
from ..execute.scheduler import Result
from ..files import stored
from ..logs import log_tail
from ..model import Dag, Job
from ..progress import about, duration
from ..schedule import size
from .plan_text import failure_groups, ids
from .summary import Summary


def end_words(kind: str, end: CommandEnd) -> str:
    """How a command ended, where `kind` names it, as `verify 1`, `command` or `command 2`."""
    match end:
        case Exited(code):
            return f"{kind} exited with status {code}"
        case KilledMemory(used, limit):
            return f"{kind} used {size(used)} of memory, more than the {size(limit)} its job declares, and was killed"
        case KilledSignal(limit):
            declared = f", declared memory={size(limit)}" if limit is not None else ""
            return f"{kind} killed by signal 9 (SIGKILL), possibly out of memory{declared}"
        case NotStarted(error):
            return f"{kind} could not start: {error}"


def _input_changes(failure: InputChanged) -> str:
    """Name every changed input and show which of its stamp's fields changed."""
    before, after = failure.before, failure.after
    message = "input changed during execution"
    if after != before:
        changes = []
        for path, original in before.items():
            current = after.get(path)
            if original == current:
                continue
            if original is None or current is None:
                changes.append(f"{path}: {stored(original)!r} -> {stored(current)!r}")
                continue
            changes.append(f"{path}: {', '.join(original.changes(current))}")
        message += ": " + "; ".join(changes)
    # A folder whose stamp is the same changed only in what its entries' stamps leave out.
    extra = [f"{path}: directory entries or file metadata changed" for path in failure.folders
             if before[path] == after[path]]
    return message + ("; " + "; ".join(extra) if extra else "")


def failure_words(job: Job, failure: Failure) -> str:
    """Why `job` failed, in one line."""
    match failure:
        case InputDisappeared():
            return "input disappeared before execution"
        case NoCommand():
            return "job has no command"
        case CheckArtifactMissing(check):
            return f"{check.describe()}: the artifact is missing"
        case CheckFailed(check, end):
            return end_words(check.describe(), end).replace(" exited ", " failed: exited ", 1)
        case CommandFailed(Phase.VERIFY, index, end):
            return end_words(f"verify {index}", end)
        case CommandFailed(Phase.COMMAND, index, end):
            return end_words(job.commands[index - 1].describe(index, len(job.commands)), end)
        case StdoutNotMoved(path, error):
            return f"could not move standard output onto {path}: {error}"
        case InputChanged():
            return _input_changes(failure)
        case OutputsMissing(missing):
            return "command succeeded but did not create " + ", ".join(f"output {port} {path}" for port, path in missing)
        case DependencyFailed(first):
            return f"dependency {first} failed"
        case Blocked(decision):
            return decision.reason
    raise AssertionError(f"no words for {failure!r}")


def printer(dag: Dag) -> Callable[[Event], None]:
    """What prints a run's events to standard output as they come."""
    work, width = labels(dag.jobs), id_width(dag.jobs)

    def say(job: Job, word: str, rest: str = "") -> None:
        """One progress line: the job's id, what happened, and what it works on, under the `Running` heading."""
        print(f"  [{job.id:>{width}}] {word:7} {name(job, work[job.id])}{rest}", flush=True)

    def on_event(event: Event) -> None:
        match event:
            case events.RunsAlone(job, over):
                print(f"[{job.id}] needs {over}; it will run alone, once no other job is running", flush=True)
            case events.Running(runs, maybe, remaining):
                estimate = "" if remaining is None else f", {about(remaining)}"
                more = f", and {maybe} more that may" if maybe else ""
                print(f"\nRunning {runs} job{'' if runs == 1 else 's'}{more}{estimate}", flush=True)
            case events.Started(job):
                say(job, "start")
            case events.Done(job, seconds, progress):
                say(job, "done", f" in {duration(seconds)}; {progress}")
            case events.Checking(job):
                say(job, "check")
            case events.Checked(job):
                say(job, "checked")
            case events.SkippedUnchanged(job):
                say(job, "skip", ": upstream output unchanged")
            # Failures are reported together once the run ends, grouped by cause.
            case events.JobFailed() | events.NotRun():
                pass

    return on_event


@dataclass(frozen=True)
class Shown:
    """A failed job as its report shows it: `failure_groups` groups failures that say the same."""

    job: Job
    error: str
    log: Path | None


def _shown(result: Result) -> Shown:
    assert result.failure is not None, "only a failed job is shown as one"
    return Shown(result.job, failure_words(result.job, result.failure), result.log)


def _report_failure(job: Job, error: str, log: Path | None) -> None:
    job_id = job.id
    print(f"[{job_id}] failed: {error}", file=sys.stderr)
    for line in job.origin:
        print(f"[{job_id}] {line}", file=sys.stderr)
    if log is not None:
        tail = log_tail(log)
        if tail:
            print(f"[{job_id}] last lines of {log}:", file=sys.stderr)
            for line in tail:
                print(f"    {line}", file=sys.stderr)
        else:
            print(f"[{job_id}] log: {log}", file=sys.stderr)


def report_failures(dag: Dag, summary: Summary, verbose: bool) -> None:
    """Print each distinct failure once, with what it held up, then with `verbose` the jobs that did not run."""
    held = Counter(result.caused_by for result in summary.not_run)
    for group in failure_groups(dag.root, [_shown(result) for result in summary.failed]):
        first, others = group[0], group[1:]
        _report_failure(first.job, first.error, first.log)
        if others:
            print(f"[{first.job.id}] the same failure in {len(others)} other job{'' if len(others) == 1 else 's'}: {ids(others)}", file=sys.stderr)
        total = sum(held[shown.job.id] for shown in group)
        if total:
            lead = "together they held up" if others else "held up"
            print(f"[{first.job.id}] {lead} {total} other job{'' if total == 1 else 's'}", file=sys.stderr)
    if verbose:
        for result in summary.not_run:
            print(f"[{result.job.id}] not run: job {result.caused_by} failed", file=sys.stderr)


def summary_line(summary: Summary) -> str:
    """How many jobs were done, checked, failed, not run, current and blocked."""
    not_run = len(summary.not_run)
    line = (
        f"{summary.done} done, {summary.checked} checked, {len(summary.failed)} failed, "
        + (f"{not_run} not run (an upstream job failed), " if not_run else "")
        + f"{summary.current} current, {summary.blocked} blocked"
    )
    if summary.cutoff:
        line += f", {summary.cutoff} of the current ones not rerun because upstream output was unchanged"
    return line


def stale_line(stale: int) -> str:
    """What to do about the jobs outside a selection that read outputs the run changed."""
    return (f"{stale} job{'' if stale == 1 else 's'} outside the selection read outputs this run changed; "
            "run without the selection to bring them up to date")
