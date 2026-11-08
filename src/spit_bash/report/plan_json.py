"""The plan as `plan --json` prints it."""

from __future__ import annotations

import json
from collections import Counter

from ..model import Dag
from ..plan import Decision, Status
from .causes import decided


def document(dag: Dag, decisions: tuple[Decision, ...]) -> str:
    """The outputs the DAG left out, and each job's decision with its reason in words and how many jobs it holds up.

    A job planned `place` also has its `moves`, each as `from` and `to`.
    """
    held = Counter(d.caused_by for d in decisions if d.caused_by is not None)
    jobs = []
    for d in decisions:
        item = {"id": d.job.id, "operation": d.job.operation, "status": d.status.value, "reason": decided(d),
                "waits_for": list(d.waits_for), "caused_by": d.caused_by, "holds_up": held[d.job.id]}
        if d.status is Status.PLACE and d.placement is not None:
            item["moves"] = [{"from": old, "to": new} for old, new in d.placement.moves]
        jobs.append(item)
    return json.dumps({"left_out": list(dag.left_out), "jobs": jobs}, indent=2)
