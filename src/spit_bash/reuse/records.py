"""Every state record the runner makes, from a run, a check, an adoption, a refresh or a move."""

from __future__ import annotations

from ..files import Stamps, stored_each
from ..model import Job
from ..state.record import Record


def after_run(job: Job, inputs: Stamps, outputs: Stamps, input_hashes: dict[str, str] | None,
              output_hashes: dict[str, str] | None, seconds: float) -> Record:
    """What a successful run leaves: the files' stamps by path, their hashes by slot, or None when not hashed."""
    return Record(fingerprint=job.work.fingerprint, parts=job.work.parts,
                  inputs=stored_each(job.keyed(inputs)), outputs=stored_each(job.keyed(outputs)),
                  checks=job.work.checks_key, input_hashes=input_hashes, output_hashes=output_hashes,
                  seconds=round(seconds, 3), output_paths=job.output_paths_by_slot)


def adopted(job: Job, inputs: Stamps, outputs: Stamps) -> Record:
    """A job taken as current without running: its files' stamps, and no checks, hashes or time."""
    return Record(fingerprint=job.work.fingerprint, parts=job.work.parts,
                  inputs=stored_each(job.keyed(inputs)), outputs=stored_each(job.keyed(outputs)),
                  output_paths=job.output_paths_by_slot)


def refreshed(record: Record, job: Job, now: Stamps) -> Record | None:
    """The record with the stamps `now` and the job's output paths, or None when it already has them.

    For a job whose files were touched but hold what they held, so that the
    next plan need not hash them.
    """
    inputs = stored_each(job.keyed({path: now[path] for path in job.input_paths}))
    outputs = stored_each(job.keyed({path: now[path] for path in job.output_paths}))
    paths = job.output_paths_by_slot
    if (inputs, outputs, paths) == (record.inputs, record.outputs, record.output_paths):
        return None
    return record.model_copy(update={"inputs": inputs, "outputs": outputs, "output_paths": paths})


def checked(record: Record, job: Job) -> Record:
    """The record of a current job whose checks have now passed on the files it records."""
    return record.model_copy(update={"checks": job.work.checks_key})


def relocated(record: Record, job: Job) -> Record:
    """The record of a job whose outputs were moved to the paths the job now gives them."""
    return record.model_copy(update={"output_paths": job.output_paths_by_slot})
