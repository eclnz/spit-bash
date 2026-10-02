"""Choose part of a DAG: the jobs that make the chosen outputs, and every job they need."""

from __future__ import annotations

from dataclasses import dataclass, replace

from .dag import Dag, DagError, Job

LISTED_VALUES = 10

Entities = tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Selection:
    # A job is chosen when one of its outputs matches every kind of criterion
    # given, and within a kind matches any one of the values given.
    only: tuple[Entities, ...] = ()
    products: tuple[str, ...] = ()
    stages: tuple[tuple[str, ...], ...] = ()

    def __bool__(self) -> bool:
        return bool(self.only or self.products or self.stages)


def parse_only(text: str) -> Entities:
    """`sub=01,ses=02` as pairs; every pair must hold for an output to match."""
    pairs: dict[str, str] = {}
    for item in text.split(","):
        dimension, equals, value = item.partition("=")
        if not equals or not dimension or not value:
            raise ValueError(f"expected DIMENSION=VALUE, got `{item}`")
        if dimension in pairs:
            raise ValueError(f"dimension `{dimension}` given twice")
        pairs[dimension] = value
    return tuple(sorted(pairs.items()))


def parse_stage(text: str) -> tuple[str, ...]:
    """`preprocess/combine` as the stage path, outermost first."""
    parts = tuple(text.split("/"))
    if not all(parts):
        raise ValueError(f"expected a stage such as `preprocess` or `preprocess/combine`, got `{text}`")
    return parts


def _listing(names: set[str]) -> str:
    shown = sorted(names)
    more = len(shown) - LISTED_VALUES
    return ", ".join(shown[:LISTED_VALUES]) + (f" and {more} more" if more > 0 else "")


def _check_names(dag: Dag, selection: Selection) -> None:
    """Name a criterion the DAG cannot match at all, so a typo is not just "no job matches"."""
    products = {made.product for job in dag.jobs for made in job.made}
    values: dict[str, set[str]] = {}
    for job in dag.jobs:
        for made in job.made:
            for dimension, value in made.entities:
                values.setdefault(dimension, set()).add(value)
    stages = {job.stage[:depth] for job in dag.jobs for depth in range(1, len(job.stage) + 1)}
    for product in selection.products:
        if product not in products:
            raise DagError(f"no output is of product `{product}`; products: {_listing(products)}")
    for group in selection.only:
        for dimension, value in group:
            if dimension not in values:
                raise DagError(f"no output has dimension `{dimension}`; dimensions: {_listing(set(values)) or 'none'}")
            if value not in values[dimension]:
                raise DagError(f"no output has {dimension}={value}; {dimension} takes {_listing(values[dimension])}")
    for stage in selection.stages:
        if stage not in stages:
            if not stages:
                raise DagError(f"no job is in stage `{'/'.join(stage)}`: this DAG has no stages")
            listed = {"/".join(path) for path in stages}
            raise DagError(f"no job is in stage `{'/'.join(stage)}`; stages: {_listing(listed)}")


def _matches(job: Job, selection: Selection) -> bool:
    if selection.stages and not any(job.stage[:len(stage)] == stage for stage in selection.stages):
        return False
    for made in job.made:
        if selection.products and made.product not in selection.products:
            continue
        if selection.only and not any(set(group) <= set(made.entities) for group in selection.only):
            continue
        return True
    return False


@dataclass(frozen=True)
class Selected:
    dag: Dag
    matched: int
    needed: int


def select(dag: Dag, selection: Selection) -> Selected:
    """Keep the matching jobs and the jobs upstream of them; drop everything downstream."""
    if not selection:
        return Selected(dag, len(dag.jobs), 0)
    _check_names(dag, selection)
    matched = {job.id for job in dag.jobs if _matches(job, selection)}
    if not matched:
        raise DagError("no job's outputs match every criterion together")
    by_id = {job.id: job for job in dag.jobs}
    chosen = set(matched)
    stack = list(matched)
    while stack:
        for dependency in by_id[stack.pop()].depends_on:
            if dependency not in chosen:
                chosen.add(dependency)
                stack.append(dependency)
    jobs = tuple(job for job in dag.jobs if job.id in chosen)
    return Selected(replace(dag, jobs=jobs), len(matched), len(chosen) - len(matched))
