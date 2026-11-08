"""The evidence behind one job's planning decision, for `plan --job`."""

from __future__ import annotations

from .dag import Dag, DagError, Job
from .display import labels, name
from .files import Stamp, content_hash, stamps, stored
from .plan import changed_parts, Decision, State, Status, UPSTREAM, job_key


def _stamp(value: Stamp | None) -> str:
    return "missing" if value is None else value.describe()


def _content(dag: Dag, path: str, key: str, recorded: dict[str, str] | None, hashing: bool) -> str:
    """Whether a file whose stamp differs still holds what the record saw."""
    if not hashing:
        return "content not compared (--compare stat)"
    if not recorded or key not in recorded:
        return "no content hash recorded, so it counts as changed"
    now = content_hash(str(dag.path(path)), path in dag.folders)
    if now is None:
        return "content cannot be read"
    return "same content, so only touched" if now == recorded[key] else "content differs"


def _files(dag: Dag, job: Job, paths: tuple[str, ...], recorded: dict[str, list[int]],
           recorded_hashes: dict[str, str] | None, hashing: bool, seen: dict) -> list[str]:
    lines = []
    current = stamps(dag, paths, seen)
    for path in paths:
        key = job.identities[path]
        was, now = recorded.get(key), current[path]
        if was == stored(now):
            continue
        lines.append(f"    {path}")
        lines.append(f"      recorded: {_stamp(None if was is None else Stamp.from_stored(was))}")
        lines.append(f"      now:      {_stamp(now)}")
        if now is not None and was is not None:
            lines.append(f"      {_content(dag, path, key, recorded_hashes, hashing)}")
    return lines or ["    none differs from the record"]


def explain(dag: Dag, state: State, decisions: tuple[Decision, ...], job_id: int, hashing: bool) -> list[str]:
    """Describe why the planner decided as it did about job `job_id`, from its own decision and the record."""
    by_id = {decision.job.id: decision for decision in decisions}
    if job_id not in by_id:
        raise DagError(f"no job {job_id} among the jobs planned")
    decision = by_id[job_id]
    job = decision.job
    lines = [f"Job {job.id}: {name(job, labels(dag.jobs)[job.id])}",
             f"  decision: {decision.status.value}, {decision.reason}"]
    if decision.caused_by is not None:
        lines.append(f"  blocked by job {decision.caused_by}: {by_id[decision.caused_by].reason}")
    record = state.jobs.get(job_key(job))
    if record is None:
        lines.append("  record: none; no successful run is recorded for what this job makes")
    else:
        same = "same" if record.fingerprint == job.fingerprint else "changed"
        lines.append(f"  fingerprint: {same} (recorded {record.fingerprint}, now {job.fingerprint})")
        if same == "changed":
            lines.append(f"  changed: {changed_parts(record, job)}")
        lines.append(f"  checks: {'same' if record.checks == job.checks_key else 'differ'} "
                     f"(recorded {record.checks or 'none'}, now {job.checks_key or 'none'})")
        seen: dict = {}
        lines.append("  inputs that differ from the record:")
        lines += _files(dag, job, job.inputs, record.inputs, record.input_hashes, hashing, seen)
        lines.append("  outputs that differ from the record:")
        lines += _files(dag, job, job.outputs, record.outputs, record.output_hashes, hashing, seen)
    upstream = [dep for dep in job.depends_on if by_id[dep].status in UPSTREAM]
    if upstream:
        lines.append("  upstream jobs that will or may rewrite its inputs: "
                     + ", ".join(f"{dep} ({by_id[dep].status.value})" for dep in upstream))
    return lines
