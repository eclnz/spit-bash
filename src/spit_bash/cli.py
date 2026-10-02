"""Command line interface for planning and running a .spitdag."""

from __future__ import annotations

import argparse
import asyncio
import sys
from enum import Enum
from pathlib import Path

from .dag import DagError, load_dag
from .plan import State, plan
from .run import execute


class Action(str, Enum):
    PLAN = "plan"
    RUN = "run"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="spit-bash", description="Plan and run SPIT .spitdag jobs")
    subcommands = parser.add_subparsers(required=True)
    for action in Action:
        command = subcommands.add_parser(action.value)
        command.set_defaults(action=action)
        command.add_argument("dag", help=".spitdag JSON file, or - for stdin")
        command.add_argument("--root", help="dataset root, required when the DAG has no root")
        command.add_argument("--state", help="state file (default: ROOT/.spit-bash/state.json)")
        command.add_argument("--force", action="store_true", help="rerun command jobs even if current")
        if action is Action.RUN:
            command.add_argument("-j", "--jobs", type=int, default=1, help="maximum concurrent jobs")
    args = parser.parse_args(argv)
    if args.action is Action.RUN and args.jobs < 1:
        parser.error("--jobs must be at least 1")
    try:
        if args.dag == "-":
            dag = load_dag(sys.stdin, args.root)
        else:
            with open(args.dag, encoding="utf-8") as source:
                dag = load_dag(source, args.root)
        state = State(Path(args.state).expanduser().resolve() if args.state else dag.root / ".spit-bash" / "state.json")
        decisions = plan(dag, state, args.force)
        for decision in decisions:
            print(f"[{decision.job.id}] {decision.status:7} {decision.job.operation}: {decision.reason}")
        if dag.left_out:
            print(f"partial DAG: {len(dag.left_out)} output(s) left out", file=sys.stderr)
        blocked = [decision for decision in decisions if decision.status == "blocked"]
        if blocked:
            return 1
        if args.action is Action.PLAN:
            return 0
        results = asyncio.run(execute(dag, decisions, state, args.jobs))
        for result in results:
            if result.error:
                print(f"[{result.job.id}] failed: {result.error}", file=sys.stderr)
        return 1 if any(result.error for result in results) else 0
    except (DagError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
