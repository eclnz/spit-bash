"""What changed since the last run, in the researcher's terms: sources added and changed, work gone, and changed
operations. `plan` and `run` give it before the counts, so a plan reads by its causes before its jobs."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Collection

from ..cause import Why
from ..model import Artifact, Dag
from ..plan import Decision, Status
from .causes import parts

# How many sources a line names one by one before it describes them by their new values.
NAMED = 3


def _entities(artifact: Artifact) -> str:
    return " ".join(f"{dimension}={value}" for dimension, value in artifact.entities)


def _described(chosen: list[Artifact], others: list[Artifact]) -> str:
    """Sources of one product, as a reader would name them: each, when there are few; else the values of a dimension
    that no other source of the product has, as `take=3, take=4`; else nothing more than the count."""
    if len(chosen) <= NAMED:
        # A source with no entities is named by its product alone, which the line gives already.
        return ", ".join(named for named in map(_entities, chosen) if named)
    seen = {pair for artifact in others for pair in artifact.entities}
    new = sorted({pair for artifact in chosen for pair in artifact.entities if pair not in seen})
    return ", ".join(f"{dimension}={value}" for dimension, value in new)


def _sources(dag: Dag, chosen: list[Decision], exclude: set[str]) -> dict[str, list[Artifact]]:
    """The sources the `chosen` jobs read, by product, leaving out the paths in `exclude`."""
    external = set(dag.external_inputs)
    found: dict[str, dict[str, Artifact]] = {}
    for decision in chosen:
        for artifact in decision.job.input_artifacts:
            if artifact.path in external and artifact.path not in exclude:
                found.setdefault(artifact.product, {})[artifact.path] = artifact
    return {product: sorted(by_path.values(), key=lambda a: a.path) for product, by_path in sorted(found.items())}


def change_lines(dag: Dag, everything: Dag, decisions: tuple[Decision, ...], records: Collection[str]) -> list[str]:
    """One line of what changed since the last run, or none when nothing did or no job has run before.

    `records` holds the keys the state has records under. A source is new when only jobs with no record read it,
    and changed when a job whose input changed reads it. Records that no job of `everything`, the whole DAG,
    claims are work the pipeline or the data no longer has.
    """
    if not records:
        return []
    external = set(dag.external_inputs)
    fresh = [decision for decision in decisions if decision.job.work.key not in records]
    recorded = {artifact.path for decision in decisions if decision.job.work.key in records
                for artifact in decision.job.input_artifacts if artifact.path in external}
    every: dict[str, list[Artifact]] = {}
    for decision in decisions:
        for artifact in decision.job.input_artifacts:
            if artifact.path in external:
                every.setdefault(artifact.product, []).append(artifact)
    parts_of: list[str] = []
    new = _sources(dag, fresh, recorded)
    for product, chosen in new.items():
        paths = {artifact.path for artifact in chosen}
        others = [artifact for artifact in every[product] if artifact.path not in paths]
        described = _described(chosen, others)
        parts_of.append(f"+{len(chosen)} {product}" + (f" ({described})" if described else ""))
    inputs_changed = [decision for decision in decisions
                      if decision.status is Status.RUN and decision.cause.why is Why.INPUT_CHANGED]
    changed = _sources(dag, inputs_changed, {a.path for chosen in new.values() for a in chosen})
    for product, chosen in changed.items():
        paths = {artifact.path for artifact in chosen}
        others = [artifact for artifact in every[product] if artifact.path not in paths]
        described = _described(chosen, others)
        parts_of.append(f"{len(chosen)} {product} changed" + (f" ({described})" if described else ""))
    claimed = {job.work.key for job in everything.jobs}
    gone = [key for key in records if key not in claimed]
    if gone:
        # A key names what its job made, so the values only gone jobs made say what went, as `voice=yweweler`.
        kept = {pair for job in everything.jobs for artifact in job.output_artifacts for pair in artifact.entities}
        went = sorted({(dimension, value) for key in gone for _, entities in json.loads(key)
                       for dimension, value in entities} - kept)
        named = f" ({', '.join(f'{d}={v}' for d, v in went[:NAMED])}{', ...' if len(went) > NAMED else ''})" if went else ""
        parts_of.append(f"{len(gone)} {'job' if len(gone) == 1 else 'jobs'} from earlier runs no longer in the DAG{named}")
    edited = Counter((decision.job.operation, parts(decision.cause.parts)) for decision in decisions
                     if decision.status is Status.RUN and decision.cause.why is Why.WORK_CHANGED)
    for (operation, what), count in sorted(edited.items()):
        parts_of.append(f"{operation}: {what} ({count} {'job' if count == 1 else 'jobs'})")
    return [f"since the last run: {'; '.join(parts_of)}"] if parts_of else []
