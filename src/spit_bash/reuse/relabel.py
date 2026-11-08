"""Give a record left by a renamed product, dimension, source or entity to the job that now does its work."""

from __future__ import annotations

from ..files import Stamps, stamps
from ..model import Dag
from ..state.record import Record
from ..state.store import StateWrite
from .evidence import Compare, changed, compare_files


def relabel(dag: Dag, records: dict[str, Record], everything: Dag) -> tuple[StateWrite, ...]:
    """The writes that hand each unclaimed record over to the job that now does its work.

    A job with no record under its key takes an unclaimed record of the same
    work (the same fingerprint) when each input holds what the record's did, so
    the same work on the same bytes is reused whatever things are called.
    `everything` is the whole DAG, whose jobs claim their keys even when `dag`
    holds only some. Each handover forgets the old key, then records the new.
    """
    claimed = {job.work.key for job in everything.jobs}
    orphans: dict[str, list[str]] = {}
    for key, record in sorted(records.items()):
        if key not in claimed and record.input_hashes is not None and record.output_hashes is not None:
            orphans.setdefault(record.fingerprint, []).append(key)
    if not orphans:
        return ()
    # What each file holds: as read, or, for a file a handed-over job has yet to make
    # at its new path, as that job's record says.
    held: dict[str, str | None] = {}
    seen: Stamps = {}
    writes: list[StateWrite] = []
    taken: set[str] = set()
    for job in dag.jobs:
        key = job.work.key
        if key in records or key in taken or not job.commands or not orphans.get(job.work.fingerprint):
            continue
        for old in orphans[job.work.fingerprint]:
            record = records[old]
            assert record.input_hashes is not None and record.output_hashes is not None
            slots = job.input_paths_by_slot
            if set(slots) != set(record.input_hashes) or changed(
                    compare_files(dag, slots, stamps(dag, job.input_paths, seen), record.inputs, record.input_hashes, Compare.FULL, held)):
                continue
            orphans[job.work.fingerprint].remove(old)
            writes += [StateWrite(old, None), StateWrite(key, record)]
            taken.add(key)
            for slot, path in job.output_paths_by_slot.items():
                held[path] = record.output_hashes.get(slot)
            break
    return tuple(writes)
