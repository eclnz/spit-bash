"""Command line interface for planning and running a .spitdag."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
from enum import Enum
from pathlib import Path

from .dag import Dag, DagError, load_dag
from .plan import State, adopt, plan
from .run import execute, log_tail


class Action(str, Enum):
    PLAN = "plan"
    RUN = "run"
    ADOPT = "adopt"


HELP = {
    Action.PLAN: "report which jobs would run, skip or be blocked",
    Action.RUN: "run every job that is not current or blocked",
    Action.ADOPT: "record jobs whose files already exist as current, without running them",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="spit-bash", description="Plan and run SPIT .spitdag jobs")
    subcommands = parser.add_subparsers(required=True)
    for action in Action:
        command = subcommands.add_parser(action.value, help=HELP[action], description=HELP[action])
        command.set_defaults(action=action)
        command.add_argument("dag", help=".spitdag JSON file, or - for stdin")
        command.add_argument("--root", help="dataset root, required when the DAG has no root")
        command.add_argument("--state", help="state file (default: ROOT/.spit-bash/state.jsonl)")
        if action is not Action.ADOPT:
            command.add_argument("--force", action="store_true", help="rerun command jobs even if current")
        if action is Action.PLAN:
            command.add_argument("--json", action="store_true", help="print the plan as JSON")
        if action is Action.RUN:
            command.add_argument("-j", "--jobs", type=int, default=os.cpu_count() or 1,
                                 help="maximum concurrent jobs (default: the number of CPUs)")
            command.add_argument("--logs", help="folder for job logs (default: ROOT/.spit-bash/logs)")
    return parser


def _load(args: argparse.Namespace) -> Dag:
    if args.dag == "-":
        return load_dag(sys.stdin, args.root)
    with open(args.dag, encoding="utf-8") as source:
        return load_dag(source, args.root)


def _report_failure(job_id: int, error: str, log: Path | None) -> None:
    print(f"[{job_id}] failed: {error}", file=sys.stderr)
    if log is not None:
        tail = log_tail(log)
        if tail:
            print(f"[{job_id}] last lines of {log}:", file=sys.stderr)
            for line in tail:
                print(f"    {line}", file=sys.stderr)
        else:
            print(f"[{job_id}] log: {log}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.action is Action.RUN and args.jobs < 1:
        parser.error("--jobs must be at least 1")
    state: State | None = None
    try:
        dag = _load(args)
        state = State(Path(args.state).expanduser().resolve() if args.state else dag.root / ".spit-bash" / "state.jsonl")
        if args.action is not Action.PLAN:
            state.lock()
        if dag.left_out:
            print(f"partial DAG: {len(dag.left_out)} output(s) left out", file=sys.stderr)

        if args.action is Action.ADOPT:
            adoptions = adopt(dag, state)
            for adoption in adoptions:
                print(f"[{adoption.job.id}] {adoption.status:7} {adoption.job.operation}: {adoption.reason}")
            return 0

        decisions = plan(dag, state, args.force)
        blocked = any(decision.status == "blocked" for decision in decisions)
        if args.action is Action.PLAN and args.json:
            json.dump({
                "left_out": list(dag.left_out),
                "jobs": [
                    {"id": d.job.id, "operation": d.job.operation, "status": d.status, "reason": d.reason}
                    for d in decisions
                ],
            }, sys.stdout, indent=2)
            print()
        else:
            for decision in decisions:
                print(f"[{decision.job.id}] {decision.status:7} {decision.job.operation}: {decision.reason}")
        if args.action is Action.PLAN:
            return 1 if blocked else 0

        logs = Path(args.logs).expanduser().resolve() if args.logs else dag.root / ".spit-bash" / "logs"
        try:
            results = asyncio.run(execute(dag, decisions, state, args.jobs, logs))
        except asyncio.CancelledError:
            print("terminated; running jobs were stopped", file=sys.stderr)
            return 128 + signal.SIGTERM
        except KeyboardInterrupt:
            print("interrupted; running jobs were stopped", file=sys.stderr)
            return 128 + signal.SIGINT
        failed = [result for result in results if result.error]
        for result in failed:
            _report_failure(result.job.id, result.error or "", result.log)
        ran = sum(1 for decision in decisions if decision.status == "run")
        skipped = sum(1 for decision in decisions if decision.status == "skip")
        print(
            f"{ran - len(failed)} done, {len(failed)} failed, {skipped} current, "
            f"{len(decisions) - ran - skipped} blocked",
            file=sys.stderr,
        )
        return 1 if failed or blocked else 0
    except (DagError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        if state is not None:
            state.close()


if __name__ == "__main__":
    raise SystemExit(main())
