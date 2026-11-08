"""Why the planner decided as it did about a job, as data; `report.causes` gives it words."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Why(Enum):
    """The rule that settled a job's decision."""

    # A command job: what its files and record say.
    OUTPUT_MISSING = "output missing"
    INPUT_MISSING = "input missing"
    FORCED = "forced"
    NO_RECORD = "no record"
    WORK_CHANGED = "work changed"
    INPUT_CHANGED = "input changed"
    OUTPUT_CHANGED = "output changed"
    CHECKS_CHANGED = "checks changed"
    CHECKS_NOT_RUN = "checks not run"
    CURRENT = "current"
    # A command job below one that will or may run.
    DEPENDENCY_WILL_RUN = "dependency will run"
    UPSTREAM_MAY_RUN = "upstream may run"
    # A job with no command, which can only be current or blocked.
    NO_COMMAND_OUTPUT_MISSING = "no command, output missing"
    NO_COMMAND_DEPENDENCY_WILL_RUN = "no command, dependency will run"
    NO_COMMAND_DEPENDENCY_MAY_RUN = "no command, dependency may run"
    NO_COMMAND_OUTPUT_OLDER = "no command, output older"
    NO_COMMAND_CHECKING = "no command, checking"
    NO_COMMAND_OUTPUTS_EXIST = "no command, outputs exist"
    # What blocks any job before its files are looked at.
    MISSING_EXTERNAL = "missing external"
    PROGRAM_NOT_FOUND = "program not found"
    DEPENDENCY_BLOCKED = "dependency blocked"


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
