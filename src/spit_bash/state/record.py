"""What one successful run left, as the state file keeps it."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

# A file's size and time, or a folder's, with how many files it holds. Both
# start with a size and a time, so either can be compared by time.
FileStamp = Annotated[list[int], Field(min_length=2, max_length=3)]


class Record(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    fingerprint: str
    # The fingerprint's parts as `Job.parts` gives them, to say which changed;
    # None for a record made before they were kept.
    parts: dict[str, str] | None = None
    inputs: dict[str, FileStamp]
    outputs: dict[str, FileStamp]
    # The checks the job passed with on these files, as `Job.checks_key`
    # gives them; None for no checks, and for a job adopted without running any.
    checks: str | None = None
    # What each file or folder held, as `files.content_hash` gives it, so a
    # file that was only touched is not a change; None when the run did not hash.
    input_hashes: dict[str, str] | None = None
    output_hashes: dict[str, str] | None = None
    # How long the job took, from its first check to its last, for estimating
    # how long a run will take; None for an adopted job.
    seconds: float | None = None
    # Where each output was made, by identity, so a run can find files
    # that a change of path rules left behind; None before the runner kept it.
    output_paths: dict[str, str] | None = None
    # The run that made the outputs, by its folder's name under `runs`, where its log is; None for a job
    # adopted without running.
    run: str | None = None
