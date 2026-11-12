"""What `plan` prints: counts, the causes that need action, each job's line, and what `adopt` did."""

from __future__ import annotations

from collections import Counter

from ..display import id_width, labels, name
from ..model import Dag
from ..plan import Adoption, Decision, Status
from . import LISTED
from .causes import adoption, cause, decided, move


def jobs(count: int) -> str:
    return f"{count} job" + ("" if count == 1 else "s")


def counts(decisions: tuple[Decision, ...]) -> str:
    """One line: how many jobs can start, will wait, may run, will move their outputs, only need checks, are blocked
    or are current."""
    kinds: Counter[str] = Counter()
    for decision in decisions:
        match decision.status:
            case Status.RUN:
                kinds["wait" if decision.waits_for else "start"] += 1
            case Status.MAYBE:
                kinds["maybe"] += 1
            case Status.PLACE:
                kinds["place"] += 1
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
    if kinds["place"]:
        one = kinds["place"] == 1
        parts.append(f"{jobs(kinds['place'])} will move {'its' if one else 'their'} outputs")
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


def replacing_lines(decisions: tuple[Decision, ...]) -> list[str]:
    """For the jobs that will run over outputs already on disk, a line for each operation and reason, under a count."""
    causes: dict[tuple[str, str], int] = {}
    for decision in decisions:
        if decision.replaces:
            key = (decision.job.operation, decided(decision))
            causes[key] = causes.get(key, 0) + 1
    if not causes:
        return []
    total = sum(causes.values())
    lines = [f"{jobs(total)} will replace existing outputs:"]
    for (operation, reason), count in list(causes.items())[:LISTED]:
        lines.append(f"  {operation}" + (f" x{count}" if count > 1 else "") + f": {reason}")
    if len(causes) > LISTED:
        lines.append(f"  ... and {len(causes) - LISTED} more causes; `--verbose` lists every job")
    return lines


def _word(decision: Decision) -> str:
    match decision.status:
        case Status.RUN:
            return "waits" if decision.waits_for else "ready"
        case Status.MAYBE:
            return "may run"
        case Status.SKIP:
            return "current"
        case Status.CHECK | Status.BLOCKED | Status.PLACE:
            return decision.status.value


def _reason(decision: Decision) -> str:
    if decision.status is Status.RUN and decision.waits_for:
        waits = decision.waits_for
        return "waits for " + (", ".join(map(str, waits)) if len(waits) <= 3 else f"{len(waits)} jobs")
    return decided(decision)


def job_line(decision: Decision, work: dict[int, str], width: int) -> str:
    job = decision.job
    return f"[{job.id:>{width}}] {_word(decision):7} {name(job, work[job.id])}: {_reason(decision)}"


def not_moved_lines(decisions: tuple[Decision, ...]) -> list[str]:
    """Under `--move-files`, a line for each job whose outputs are recorded at other paths but are not moved, with why."""
    refused = [(decision.job.id, decision.placement) for decision in decisions
               if decision.status is not Status.PLACE and decision.placement is not None]
    lines = [f"[{job_id}] not moved: {move(placement)}" for job_id, placement in refused[:LISTED]]
    if len(refused) > LISTED:
        lines.append(f"... and {len(refused) - LISTED} more not moved; `--verbose` lists every job")
    return lines


def listing(dag: Dag, decisions: tuple[Decision, ...], verbose: bool) -> list[str]:
    """What `plan` and `run` print of a plan: the counts, a line for each cause that blocks jobs, and with `verbose`
    every job, or without it why jobs will replace outputs on disk and each job whose outputs are not moved."""
    lines = [counts(decisions), *blocked_lines(decisions)]
    if verbose:
        work, width = labels(dag.jobs), id_width(dag.jobs)
        lines += [job_line(decision, work, width) for decision in decisions]
    else:
        lines += replacing_lines(decisions)
        lines += not_moved_lines(decisions)
    return lines


def nothing_run(decisions: tuple[Decision, ...]) -> str:
    """Why `run` without `--partial` ran nothing: how many jobs are blocked."""
    held = sum(decision.status is Status.BLOCKED for decision in decisions)
    return (f"nothing was run: {jobs(held)} {'is' if held == 1 else 'are'} blocked. Fix the cause, or pass "
            "--partial to run the jobs that are not.")


def adoption_line(adopted: Adoption) -> str:
    return f"[{adopted.job.id}] {adopted.status.value:7} {adopted.job.operation}: {adoption(adopted.cause)}"
