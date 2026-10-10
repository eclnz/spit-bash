"""Command line interface for planning and running a .spitdag."""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import signal
import subprocess
import sys
from enum import Enum
from pathlib import Path

from .dag import Dag, DagError, load_dag
from .plan import State, Status, adopt, plan
from .resources import Pool, parse_size
from .run import execute, log_tail
from .selection import Selection, parse_only, parse_stage, select

SPIT_SUFFIXES = (".spitin", ".spit", ".spitout")

FILES_HELP = """\
what to run: a .spitdag (or - for one on stdin), or what `spit dag` takes,
which spit-bash then runs for you: a .spitin recipe, a .spit pipeline and
its .spitout (or - for one on stdin), or a .spit pipeline with --root"""


class Action(str, Enum):
    PLAN = "plan"
    RUN = "run"
    ADOPT = "adopt"


HELP = {
    Action.PLAN: "report which jobs would run, be checked, skip or be blocked",
    Action.RUN: "run every job that is not current or blocked, and check current jobs whose checks changed",
    Action.ADOPT: "record jobs whose files already exist as current, without running them",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="spit-bash", description="Plan and run SPIT .spitdag jobs")
    subcommands = parser.add_subparsers(required=True)
    for action in Action:
        command = subcommands.add_parser(action.value, help=HELP[action], description=HELP[action],
                                         formatter_class=argparse.RawTextHelpFormatter)
        command.set_defaults(action=action)
        command.add_argument("files", nargs="+", metavar="FILE", help=FILES_HELP)
        command.add_argument("--root", help="dataset root: required when the DAG has no root,\n"
                                            "and the folder `spit dag` scans for a .spit alone")
        command.add_argument("--spit", default=os.environ.get("SPIT", "spit"),
                             help="the spit program, for a recipe or pipeline (default: $SPIT, else spit)")
        command.add_argument("--partial", action="store_true",
                             help="pass --partial to `spit dag`: plan complete jobs, leave out the rest")
        choose = command.add_argument_group("choosing jobs", "Keep the jobs whose outputs match, and every job upstream of them.\n"
                                            "Repeat an option to match any of its values; different options must all match.")
        choose.add_argument("--only", action="append", default=[], metavar="DIM=VALUE[,...]",
                            help="outputs with these entities, such as sub=01 or sub=01,ses=02")
        choose.add_argument("--product", action="append", default=[], metavar="NAME", help="outputs of this product")
        choose.add_argument("--stage", action="append", default=[], metavar="NAME[/NAME...]",
                            help="jobs in this stage, or a stage inside it, such as preprocess/combine")
        command.add_argument("--state", help="state file (default: ROOT/.spit-bash/state.jsonl)")
        if action is not Action.ADOPT:
            command.add_argument("--force", action="store_true", help="rerun command jobs even if current")
        if action is Action.PLAN:
            command.add_argument("--json", action="store_true", help="print the plan as JSON")
        if action is Action.RUN:
            command.add_argument("-j", "--jobs", type=int, default=os.cpu_count() or 1,
                                 help="processors to share between jobs: a job takes its `cpus` prop of them, one if it has none\n"
                                      "(default: the number of CPUs)")
            command.add_argument("--mem", metavar="SIZE",
                                 help="memory to share between jobs, such as 64G: a job takes its `mem` prop of it\n"
                                      "(default: no limit)")
            command.add_argument("--logs", help="folder for job logs (default: ROOT/.spit-bash/logs)")
    return parser


def _load(args: argparse.Namespace) -> Dag:
    if not args.files[0].endswith(SPIT_SUFFIXES):
        if len(args.files) > 1:
            raise DagError(f"expected one .spitdag, got {len(args.files)} files")
        if args.partial:
            raise DagError("--partial is for `spit dag`; give a recipe or pipeline, not a .spitdag")
        if args.files[0] == "-":
            return load_dag(sys.stdin, args.root)
        with open(args.files[0], encoding="utf-8") as source:
            return load_dag(source, args.root)

    # `spit dag` finds a recipe's root itself; --root names the folder to scan
    # only for a pipeline given alone.
    scan = args.root is not None and args.files[0].endswith(".spit") and len(args.files) == 1
    command = [args.spit, "dag", *args.files, "--json"]
    if scan:
        command += ["--root", args.root]
    if args.partial:
        command.append("--partial")
    try:
        # spit's notes and errors go straight to the terminal.
        result = subprocess.run(command, stdout=subprocess.PIPE, text=True)
    except OSError as exc:
        raise DagError(f"cannot run `{args.spit}`: {exc.strerror}; put spit on PATH, or pass --spit or set SPIT") from exc
    if result.returncode != 0:
        raise DagError(f"`spit dag` exited with status {result.returncode}")
    return load_dag(io.StringIO(result.stdout), None if scan else args.root)


def _selection(args: argparse.Namespace) -> Selection:
    try:
        return Selection(
            only=tuple(parse_only(text) for text in args.only),
            products=tuple(args.product),
            stages=tuple(parse_stage(text) for text in args.stage),
        )
    except ValueError as exc:
        raise DagError(str(exc)) from exc


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
    try:
        memory = parse_size(args.mem, "--mem") if args.action is Action.RUN and args.mem else None
    except ValueError as exc:
        parser.error(str(exc))
    state: State | None = None
    try:
        selection = _selection(args)
        dag = _load(args)
        total = len(dag.jobs)
        selected = select(dag, selection)
        dag = selected.dag
        if selection:
            needed = f", and {selected.needed} they need" if selected.needed else ""
            print(f"chose {selected.matched} of {total} jobs{needed}", file=sys.stderr)
        state = State(Path(args.state).expanduser().resolve() if args.state else dag.root / ".spit-bash" / "state.jsonl")
        if args.action is not Action.PLAN:
            state.lock()
        if dag.left_out:
            print(f"partial DAG: {len(dag.left_out)} output(s) left out", file=sys.stderr)

        if args.action is Action.ADOPT:
            adoptions = adopt(dag, state)
            for adoption in adoptions:
                print(f"[{adoption.job.id}] {adoption.status.value:7} {adoption.job.operation}: {adoption.reason}")
            return 0

        decisions = plan(dag, state, args.force)
        blocked = any(decision.status is Status.BLOCKED for decision in decisions)
        if args.action is Action.PLAN and args.json:
            json.dump({
                "left_out": list(dag.left_out),
                "jobs": [
                    {"id": d.job.id, "operation": d.job.operation, "status": d.status.value, "reason": d.reason}
                    for d in decisions
                ],
            }, sys.stdout, indent=2)
            print()
        else:
            for decision in decisions:
                print(f"[{decision.job.id}] {decision.status.value:7} {decision.job.operation}: {decision.reason}")
        if args.action is Action.PLAN:
            return 1 if blocked else 0

        logs = Path(args.logs).expanduser().resolve() if args.logs else dag.root / ".spit-bash" / "logs"
        try:
            results = asyncio.run(execute(dag, decisions, state, Pool(args.jobs, memory), logs))
        except asyncio.CancelledError:
            print("terminated; running jobs were stopped", file=sys.stderr)
            return 128 + signal.SIGTERM
        except KeyboardInterrupt:
            print("interrupted; running jobs were stopped", file=sys.stderr)
            return 128 + signal.SIGINT
        failed = [result for result in results if result.error]
        for result in failed:
            _report_failure(result.job.id, result.error or "", result.log)
        count = {status: 0 for status in Status}
        for decision in decisions:
            count[decision.status] += 1
        failed_ids = {result.job.id for result in failed}
        failed_checks = sum(1 for d in decisions if d.status is Status.CHECK and d.job.id in failed_ids)
        print(
            f"{count[Status.RUN] - len(failed) + failed_checks} done, "
            f"{count[Status.CHECK] - failed_checks} checked, {len(failed)} failed, "
            f"{count[Status.SKIP]} current, {count[Status.BLOCKED]} blocked",
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
