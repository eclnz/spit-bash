"""The plan as `plan --json` prints it."""

from __future__ import annotations

import json
from collections import Counter

from ..model import Dag
from ..plan import Decision
from .causes import cause


def document(dag: Dag, decisions: tuple[Decision, ...]) -> str:
    """The outputs the DAG left out, and each job's decision with its cause in words and how many jobs it holds up."""
    held = Counter(d.caused_by for d in decisions if d.caused_by is not None)
    return json.dumps({
        "left_out": list(dag.left_out),
        "jobs": [
            {"id": d.job.id, "operation": d.job.operation, "status": d.status.value, "reason": cause(d.cause),
             "waits_for": list(d.waits_for), "caused_by": d.caused_by, "holds_up": held[d.job.id]}
            for d in decisions
        ],
    }, indent=2)
