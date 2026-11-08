"""The one place a cause becomes words: of a decision, an adoption and a move."""

from __future__ import annotations

from ..cause import Cause, Why
from ..plan import AdoptCause, Decision, Status
from ..reuse.relocate import Migration, MoveCause

# The most moves a reason names before it counts them instead.
MOVES_NAMED = 3

# The words for each of `reuse.identity.PARTS`, in the order they are named.
PART_NAMES = {"commands": "command", "verify": "verify commands",
              "inputs": "the artifacts it reads", "outputs": "the artifacts it makes"}


def parts(changed: tuple[str, ...] | None) -> str:
    """Why a job's work differs from its record's, naming the parts that changed when it can."""
    names = [text for part, text in PART_NAMES.items() if part in (changed or ())]
    return "; ".join(names) + " changed" if names else "job fingerprint changed"


def cause(found: Cause) -> str:
    """A decision's cause, as `plan` prints it and `plan --json` gives it as `reason`."""
    match found.why:
        case Why.OUTPUT_MISSING:
            return f"output missing; an earlier run left it at {found.paths[0]}" if found.paths else "output missing"
        case Why.INPUT_MISSING:
            return "input is missing"
        case Why.FORCED:
            return "forced"
        case Why.NO_RECORD:
            return "no successful run recorded"
        case Why.WORK_CHANGED:
            return parts(found.parts)
        case Why.INPUT_CHANGED:
            return "input changed"
        case Why.OUTPUT_CHANGED:
            return "output changed"
        case Why.CHECKS_CHANGED:
            return "checks changed"
        case Why.CHECKS_NOT_RUN:
            return "checks not yet run on these files"
        case Why.CURRENT:
            return "current"
        case Why.DEPENDENCY_WILL_RUN:
            return "dependency will run"
        case Why.UPSTREAM_MAY_RUN:
            return "runs if an upstream output changes"
        case Why.NO_COMMAND_OUTPUT_MISSING:
            return "no command and output is missing"
        case Why.NO_COMMAND_DEPENDENCY_WILL_RUN:
            return "no command and dependency will run"
        case Why.NO_COMMAND_DEPENDENCY_MAY_RUN:
            return "no command and dependency may run"
        case Why.NO_COMMAND_OUTPUT_OLDER:
            return "no command and output is older than an input"
        case Why.NO_COMMAND_CHECKING:
            return "no command; outputs exist, checking them"
        case Why.NO_COMMAND_OUTPUTS_EXIST:
            return "no command; outputs exist"
        case Why.MISSING_EXTERNAL:
            return "missing external input: " + ", ".join(found.paths)
        case Why.PROGRAM_NOT_FOUND:
            return "program not found: " + ", ".join(found.paths)
        case Why.DEPENDENCY_BLOCKED:
            return "dependency blocked: " + ", ".join(map(str, found.jobs))


def moves(placement: Migration) -> str:
    """The moves of a job planned `place`, as `moves a -> b`."""
    if len(placement.moves) > MOVES_NAMED:
        return f"moves {len(placement.moves)} outputs to the paths the pipeline now gives them"
    return "moves " + ", ".join(f"{old} -> {new}" for old, new in placement.moves)


def decided(decision: Decision) -> str:
    """A decision's reason: its cause, with a placed job's moves, or why a job's outputs are not moved."""
    placement = decision.placement
    if decision.status is Status.PLACE:
        assert placement is not None, "a job planned `place` has its moves"
        return moves(placement) + ("" if decision.cause.why is Why.CURRENT else f"; {cause(decision.cause)}")
    if placement is not None:
        return f"{cause(decision.cause)}; not moved: {move(placement)}"
    return cause(decision.cause)


def adoption(found: AdoptCause) -> str:
    """Why `adopt` recorded a job or left it."""
    match found:
        case AdoptCause.NO_COMMAND:
            return "no command"
        case AdoptCause.OUTPUT_MISSING:
            return "output missing"
        case AdoptCause.INPUT_MISSING:
            return "input missing"
        case AdoptCause.OUTPUTS_EXIST:
            return "outputs exist"


def move(migration: Migration) -> str:
    """Why a job's recorded outputs can be moved, or why not."""
    match migration.cause:
        case MoveCause.MOVABLE:
            return "outputs recorded at other paths"
        case MoveCause.WORK_CHANGED:
            return f"the job changed since the run ({parts(migration.parts)}), so its outputs are stale"
        case MoveCause.INPUT_CHANGED:
            return "an input is missing or changed since the run"
        case MoveCause.PATH_UNRECORDED:
            return "the record does not say where an output was made"
        case MoveCause.PARTLY_IN_PLACE:
            return "only some outputs moved; the rest are not moved"
        case MoveCause.BAD_PATH:
            return migration.detail
        case MoveCause.DESTINATION_EXISTS:
            return f"{migration.detail} already exists"
        case MoveCause.STILL_USED:
            return f"{migration.detail} is a path the pipeline still uses"
        case MoveCause.OUTPUT_CHANGED:
            return f"{migration.detail} is missing or changed since the run"
