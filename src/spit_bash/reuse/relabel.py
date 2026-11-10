"""Give a record left by a renamed product, dimension, source or entity to the job that now does its work."""

from __future__ import annotations

import json
from itertools import chain

from ..files import Stamps, stamps
from ..model import Dag
from ..state.record import Record
from ..state.store import StateWrite
from .evidence import Compare, changed, compare_files


def _likeness(key: str) -> tuple[tuple, tuple]:
    """What a renamed job keeps of a key: its outputs' dimension and value pairs, which a renamed product
    keeps, and their values alone, which a renamed dimension keeps. Keep in step with `identity.key`."""
    made = json.loads(key)
    pairs = tuple(sorted((dimension, value) for _, entities in made for dimension, value in entities))
    return pairs, tuple(sorted(value for _, value in pairs))


def relabel(dag: Dag, records: dict[str, Record], everything: Dag) -> tuple[StateWrite, ...]:
    """The writes that hand each unclaimed record over to the job that now does its work.

    A job with no record under its key takes an unclaimed record of the same
    work (the same fingerprint) when each input holds what the record's did, so
    the same work on the same bytes is reused whatever things are called.
    Records of jobs whose inputs hold the same bytes all qualify, so a job
    first tries the records that kept its dimension and value pairs, then
    those that kept its values, and only then the rest: each job takes its own
    record under its old name, and the outputs it moves are its own.
    `everything` is the whole DAG, whose jobs claim their keys even when `dag`
    holds only some. Each handover forgets the old key, then records the new.
    """
    claimed = {job.work.key for job in everything.jobs}
    orphans: dict[str, list[str]] = {}
    alike: dict[tuple[str, tuple], list[str]] = {}
    for key, record in sorted(records.items()):
        if key not in claimed and record.input_hashes is not None and record.output_hashes is not None:
            orphans.setdefault(record.fingerprint, []).append(key)
            for likeness in _likeness(key):
                alike.setdefault((record.fingerprint, likeness), []).append(key)
    if not orphans:
        return ()
    # What each file holds: as read, or, for a file a handed-over job has yet to make
    # at its new path, as that job's record says.
    held: dict[str, str | None] = {}
    seen: Stamps = {}
    writes: list[StateWrite] = []
    taken: set[str] = set()
    handed: set[str] = set()
    for job in dag.jobs:
        key = job.work.key
        fingerprint = job.work.fingerprint
        if key in records or key in taken or not job.commands or not orphans.get(fingerprint):
            continue
        nearest = (old for likeness in _likeness(key) for old in alike.get((fingerprint, likeness), ()))
        for old in dict.fromkeys(chain(nearest, orphans[fingerprint])):
            if old in handed:
                continue
            record = records[old]
            assert record.input_hashes is not None and record.output_hashes is not None
            slots = job.input_paths_by_slot
            if set(slots) != set(record.input_hashes) or changed(
                    compare_files(dag, slots, stamps(dag, job.input_paths, seen), record.inputs, record.input_hashes, Compare.FULL, held)):
                continue
            handed.add(old)
            writes += [StateWrite(old, None), StateWrite(key, record)]
            taken.add(key)
            for slot, path in job.output_paths_by_slot.items():
                held[path] = record.output_hashes.get(slot)
            break
    return tuple(writes)
