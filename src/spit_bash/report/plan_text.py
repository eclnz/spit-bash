"""What `plan` prints: counts, the causes that need action, each job's line, and what `adopt` and `--move-files` did."""

from __future__ import annotations

from collections import Counter

from ..display import id_width, labels, name
from ..model import Dag
from ..plan import Adoption, Decision, Status
from ..reuse.relocate import Migration
from . import LISTED
from .causes import adoption, cause, move


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
            causes.setdefault((decision.job.operation, cause(decision.cause)), []).append(decision)
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
        case Status.CHECK | Status.BLOCKED:
            return decision.status.value


def _reason(decision: Decision) -> str:
    if decision.status is Status.RUN and decision.waits_for:
        waits = decision.waits_for
        return "waits for " + (", ".join(map(str, waits)) if len(waits) <= 3 else f"{len(waits)} jobs")
    return cause(decision.cause)


def job_line(decision: Decision, work: dict[int, str], width: int) -> str:
    job = decision.job
    return f"[{job.id:>{width}}] {_word(decision):7} {name(job, work[job.id])}: {_reason(decision)}"


def listing(dag: Dag, decisions: tuple[Decision, ...], verbose: bool) -> list[str]:
    """What `plan` and `run` print of a plan: the counts, a line for each cause that blocks jobs, and with `verbose` every job."""
    lines = [counts(decisions), *blocked_lines(decisions)]
    if verbose:
        work, width = labels(dag.jobs), id_width(dag.jobs)
        lines += [job_line(decision, work, width) for decision in decisions]
    return lines


def nothing_run(decisions: tuple[Decision, ...]) -> str:
    """Why `run` without `--partial` ran nothing: how many jobs are blocked."""
    held = sum(decision.status is Status.BLOCKED for decision in decisions)
    return (f"nothing was run: {jobs(held)} {'is' if held == 1 else 'are'} blocked. Fix the cause, or pass "
            "--partial to run the jobs that are not.")


def adoption_line(adopted: Adoption) -> str:
    return f"[{adopted.job.id}] {adopted.status.value:7} {adopted.job.operation}: {adoption(adopted.cause)}"


def move_lines(migrations: tuple[Migration, ...], made: bool) -> list[str]:
    """For stdout: each move, made or that would be made as `made` says, and each job not moved with why."""
    lines = []
    for migration in migrations:
        for old, new in migration.moves:
            lines.append(f"[{migration.job.id}] {'moved' if made else 'would move'} {old} -> {new}")
        if not migration.moves:
            lines.append(f"[{migration.job.id}] not moved: {move(migration)}")
    return lines


def move_notes(migrations: tuple[Migration, ...], made: bool) -> list[str]:
    """For stderr: how many jobs reuse moved outputs, and once moves are made, what that leaves unchecked."""
    moved = sum(1 for migration in migrations if migration.moves)
    if not moved:
        return []
    notes = [f"{moved} job(s) reuse outputs by moving them to their new paths"]
    if made:
        notes.append("outputs were moved to the paths the pipeline now gives them; a tool that wrote those paths "
                     "into its output is not rerun")
    return notes
