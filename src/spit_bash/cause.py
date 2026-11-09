"""Why the planner decided as it did about a job, as data; `report.causes` gives it words."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Why(Enum):
    """The rule that settled a job's decision.

    Each value is the code `plan --json` gives the reason, which scripts may match: rename a value
    only as a breaking change.
    """

    # A command job: what its files and record say.
    OUTPUT_MISSING = "output_missing"
    INPUT_MISSING = "input_missing"
    FORCED = "forced"
    NO_RECORD = "no_record"
    WORK_CHANGED = "work_changed"
    INPUT_CHANGED = "input_changed"
    OUTPUT_CHANGED = "output_changed"
    CHECKS_CHANGED = "checks_changed"
    CHECKS_NOT_RUN = "checks_not_run"
    CURRENT = "current"
    # A command job below one that will or may run.
    DEPENDENCY_WILL_RUN = "dependency_will_run"
    UPSTREAM_MAY_RUN = "upstream_may_run"
    # A job with no command, which can only be current or blocked.
    NO_COMMAND_OUTPUT_MISSING = "no_command_output_missing"
    NO_COMMAND_DEPENDENCY_WILL_RUN = "no_command_dependency_will_run"
    NO_COMMAND_DEPENDENCY_MAY_RUN = "no_command_dependency_may_run"
    NO_COMMAND_OUTPUT_OLDER = "no_command_output_older"
    NO_COMMAND_CHECKING = "no_command_checking"
    NO_COMMAND_OUTPUTS_EXIST = "no_command_outputs_exist"
    # What blocks any job before its files are looked at.
    MISSING_EXTERNAL = "missing_external"
    PROGRAM_NOT_FOUND = "program_not_found"
    DEPENDENCY_BLOCKED = "dependency_blocked"


@dataclass(frozen=True)
class Cause:
    why: Why
    # For `Why.OUTPUT_MISSING`, where an earlier run left the output, if a change of path rules stranded it;
    # for `Why.MISSING_EXTERNAL`, the external inputs missing;
    # for `Why.PROGRAM_NOT_FOUND`, the programs, sorted.
    paths: tuple[str, ...] = ()
    # For `Why.DEPENDENCY_BLOCKED`, the blocked jobs it needs, in the order it depends on them.
    jobs: tuple[int, ...] = ()
    # For `Why.WORK_CHANGED`, the names in `reuse.identity.PARTS` that changed, or None when the
    # record keeps no parts to say which.
    parts: tuple[str, ...] | None = ()
