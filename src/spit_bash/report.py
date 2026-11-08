"""What `plan` and `run` print: counts, the causes that need action, and failures grouped by cause."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

from .display import name
from .plan import Decision, Status
from .run import Result, log_tail

# The most causes or job ids listed before the rest are counted.
LISTED = 10


def jobs(count: int) -> str:
    return f"{count} job" + ("" if count == 1 else "s")


def counts(decisions: tuple[Decision, ...]) -> str:
    """One line: how many jobs can start, will wait, may run, only need checks, are blocked or are current."""
    kinds: Counter[str] = Counter()
    for decision in decisions:
        match decision.status:
            case Status.RUN:
                kinds["wait" if decision.waits_for else "start"] += 1
            case Status.MAYBE:
                kinds["maybe"] += 1
            case Status.CHECK:
                kinds["check"] += 1
            case Status.BLOCKED:
                kinds["blocked"] += 1
            case Status.SKIP:
                kinds["current"] += 1
    total = len(decisions)
    if not total:
        return "no jobs"
    if kinds["current"] == total:
        return "1 job is current" if total == 1 else f"all {total} jobs are current"
    # What the waiting jobs wait for: the jobs that can start, and those that may run.
    upstream = kinds["start"] + kinds["maybe"]
    waits_for = "" if not upstream else " for it" if upstream == 1 else " for them"
    parts = []
    if kinds["start"]:
        parts.append(f"{jobs(kinds['start'])} can start")
    if kinds["wait"]:
        parts.append(f"{jobs(kinds['wait'])} will wait{waits_for}")
    if kinds["maybe"]:
        parts.append(f"{jobs(kinds['maybe'])} may run if an upstream output changes")
    if kinds["check"]:
        one = kinds["check"] == 1
        parts.append(f"{jobs(kinds['check'])} {'needs' if one else 'need'} only {'its' if one else 'their'} checks")
    if kinds["blocked"]:
        parts.append(f"{jobs(kinds['blocked'])} {'is' if kinds['blocked'] == 1 else 'are'} blocked")
    if kinds["current"]:
        parts.append(f"{jobs(kinds['current'])} {'is' if kinds['current'] == 1 else 'are'} current")
    return ", ".join(parts)


def blocked_lines(decisions: tuple[Decision, ...]) -> list[str]:
    """A line for each cause of blocked jobs: an operation and reason, with how many jobs depend on it."""
    held = Counter(decision.caused_by for decision in decisions if decision.caused_by is not None)
    causes: dict[tuple[str, str], list[Decision]] = {}
    for decision in decisions:
        if decision.status is Status.BLOCKED and decision.caused_by is None:
            causes.setdefault((decision.job.operation, decision.reason), []).append(decision)
    lines = []
    for (operation, reason), members in list(causes.items())[:LISTED]:
        line = f"blocked: {operation}" + (f" x{len(members)}" if len(members) > 1 else "") + f": {reason}"
        more = sum(held[member.job.id] for member in members)
        if more:
            single = len(members) == 1
            line += f" ({more} more {'depends' if more == 1 else 'depend'} on {'it' if single else 'them'})"
        lines.append(line)
    if len(causes) > LISTED:
        lines.append(f"... and {len(causes) - LISTED} more causes; `--verbose` lists every job")
    return lines


def _word(decision: Decision) -> str:
    match decision.status:
        case Status.RUN:
            return "waits" if decision.waits_for else "ready"
        case Status.MAYBE:
            return "may run"
        case Status.SKIP:
            return "current"
        case _:
            return decision.status.value


def _reason(decision: Decision) -> str:
    if decision.status is Status.RUN and decision.waits_for:
        waits = decision.waits_for
        return "waits for " + (", ".join(map(str, waits)) if len(waits) <= 3 else f"{len(waits)} jobs")
    return decision.reason


def job_line(decision: Decision, work: dict[int, str], width: int) -> str:
    job = decision.job
    return f"[{job.id:>{width}}] {_word(decision):7} {name(job, work[job.id])}: {_reason(decision)}"


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
        files = (*result.job.inputs, *result.job.outputs)
        tail = log_tail(result.log) if result.log is not None else []
        groups.setdefault((result.error, _normalised(files, root, tail)), []).append(result)
    return list(groups.values())


def ids(results: list[Result]) -> str:
    shown = ", ".join(str(result.job.id) for result in results[:LISTED])
    return shown + (f", and {len(results) - LISTED} more" if len(results) > LISTED else "")
