"""The evidence behind one job's planning decision, as `explain` prints it."""

from __future__ import annotations

from ..display import labels, name
from ..files import Stamp
from ..model import Dag
from ..plan import Decision, UPSTREAM
from ..reuse.evidence import Content, Evidence, FileDiff
from .causes import cause, decided, parts

CONTENT = {
    Content.NOT_COMPARED: "content not compared (--compare stat)",
    Content.NO_HASH: "no content hash recorded, so it counts as changed",
    Content.UNREADABLE: "content cannot be read",
    Content.SAME: "same content, so only touched",
    Content.DIFFERS: "content differs",
}


def _stamp(value: Stamp | None) -> str:
    return "missing" if value is None else value.describe()


def _files(paths: tuple[str, ...], diffs: tuple[FileDiff, ...]) -> list[str]:
    """Each of `paths` that differs from the record, in order; a path read at two places is shown at both."""
    by_path = {diff.path: diff for diff in diffs}
    lines = []
    for path in paths:
        diff = by_path.get(path)
        if diff is None:
            continue
        lines.append(f"    {path}")
        lines.append(f"      recorded: {_stamp(diff.recorded)}")
        lines.append(f"      now:      {_stamp(diff.now)}")
        if diff.now is not None and diff.recorded is not None:
            lines.append(f"      {CONTENT[diff.content]}")
    return lines or ["    none differs from the record"]


def explain(dag: Dag, decisions: tuple[Decision, ...], decision: Decision, found: Evidence) -> list[str]:
    """Describe why the planner made `decision`, one of `decisions`, from the decision and the evidence `found`."""
    by_id = {each.job.id: each for each in decisions}
    job = decision.job
    lines = [f"Job {job.id}: {name(job, labels(dag.jobs)[job.id])}",
             f"  decision: {decision.status.value}, {decided(decision)}"]
    if decision.caused_by is not None:
        lines.append(f"  blocked by job {decision.caused_by}: {cause(by_id[decision.caused_by].cause)}")
    record = found.record
    if record is None:
        lines.append("  record: none; no successful run is recorded for what this job makes")
    else:
        same = "changed" if found.work_changed else "same"
        lines.append(f"  fingerprint: {same} (recorded {record.fingerprint}, now {job.work.fingerprint})")
        if found.work_changed:
            lines.append(f"  changed: {parts(found.parts)}")
        lines.append(f"  checks: {'differ' if found.checks_changed else 'same'} "
                     f"(recorded {record.checks or 'none'}, now {job.work.checks_key or 'none'})")
        lines.append("  inputs that differ from the record:")
        lines += _files(job.input_paths, found.inputs)
        lines.append("  outputs that differ from the record:")
        lines += _files(job.output_paths, found.outputs)
    upstream = [dep for dep in job.depends_on if by_id[dep].status in UPSTREAM]
    if upstream:
        lines.append("  upstream jobs that will or may rewrite its inputs: "
                     + ", ".join(f"{dep} ({by_id[dep].status.value})" for dep in upstream))
    return lines
