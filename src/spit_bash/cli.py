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
from collections import Counter
from enum import Enum
from pathlib import Path

from .load import load_dag
from .model import Dag, DagError
from .display import id_width, labels
from .explain import explain
from .files import stamp
from .plan import Status, adopt, plan, writes
from .reuse.evidence import Compare
from .reuse.relabel import relabel
from .reuse.relocate import apply, find
from .state.store import Store
from .execute.options import Options
from .execute.scheduler import execute
from .report import run_text
from .report.plan_text import blocked_lines, counts, job_line, jobs
from .report.summary import Summary, stale
from .schedule import over_memory
from .selection import Selection, parse_only, parse_stage, select

SPIT_SUFFIXES = (".spitin", ".spit", ".spitout")
# The units of --memory, as SPIT's `resources` line takes them.
UNITS = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}

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
                             help="do what can be done: pass --partial to `spit dag`, which leaves out jobs it cannot\n"
                                  "complete, and with `run`, run the jobs that are not blocked (default: run nothing\n"
                                  "while any job is blocked)")
        choose = command.add_argument_group("choosing jobs", "Keep the jobs whose outputs match, and every job upstream of them.\n"
                                            "Repeat an option to match any of its values; different options must all match.")
        choose.add_argument("--only", action="append", default=[], metavar="DIM=VALUE[,...]",
                            help="outputs with these entities, such as sub=01 or sub=01,ses=02")
        choose.add_argument("--product", action="append", default=[], metavar="NAME", help="outputs of this product")
        choose.add_argument("--stage", action="append", default=[], metavar="NAME[/NAME...]",
                            help="jobs in this stage, or a stage inside it, such as preprocess/combine")
        command.add_argument("--state", help="state file (default: ROOT/.spit-bash/state.jsonl)")
        if action is not Action.ADOPT:
            command.add_argument("--force", action="store_true",
                                 help="rerun selected command jobs even if current")
        if action is not Action.ADOPT:
            command.add_argument("--compare", choices=[compare.value for compare in Compare], default="content",
                                 help="how a file counts as unchanged since its record: stat, by size and modification\n"
                                      "time alone, with no stop at an unchanged output and no reuse after a rename;\n"
                                      "content, by size and time, then by content where those differ (default);\n"
                                      "full, by content always, trusting no size or time")
        if action is not Action.ADOPT:
            command.add_argument("--move-files", action="store_true",
                                 help="move outputs the state records at other paths to the paths the pipeline now gives,\n"
                                      "instead of rerunning their jobs; `plan` lists the moves. A tool that writes its own\n"
                                      "paths into its output is not rerun, so use this only if yours do not")
        if action is not Action.ADOPT:
            command.add_argument("--verbose", action="store_true",
                                 help="list every job, current ones too, and with `run` each job that did not run\n"
                                      "(default: counts, and a line for each cause that blocks jobs)")
        if action is Action.PLAN:
            command.add_argument("--job", type=int, metavar="ID",
                                 help="explain one job's decision: recorded and current fingerprint and checks,\n"
                                      "the files that differ from the record, and the upstream jobs that will rewrite its inputs")
            command.add_argument("--json", action="store_true", help="print the plan as JSON")
        if action is Action.RUN:
            command.add_argument("-j", "--cores", type=int, default=os.cpu_count() or 1, metavar="CORES",
                                 help="cores to use: run jobs at once while the cores\n"
                                      "they declare fit (default: the number of CPUs)")
            command.add_argument("--memory", type=_memory, default=_machine_memory(), metavar="SIZE",
                                 help="memory to use, as 512M or 32G: run jobs at once while\n"
                                      "the memory they declare fits (default: the machine's)")
            command.add_argument("--logs", help="folder for job logs (default: ROOT/.spit-bash/logs)")
            command.add_argument("--no-enforce-memory", dest="enforce_memory", action="store_false",
                                 help="let a job hold more memory than it declares, and run a job\n"
                                      "declaring more than --memory alone, rather than refusing it")
            command.add_argument("--timestamp-logs", action="store_true",
                                 help="start each line of a job's log with the date, time and UTC offset at which\n"
                                      "the runner received it; a tool that buffers its output may have written it earlier")
            command.add_argument("--keep-failed", action="store_true",
                                 help="keep what a failed job's command wrote at its outputs, to debug it")
    return parser


def _memory(text: str) -> int:
    """A size as `--memory` takes it: a whole number, and a unit K, M, G or T, each 1024 times the one before."""
    unit = text[-1:].upper()
    number, scale = (text[:-1], UNITS[unit]) if unit in UNITS else (text, 1)
    if not (number.isascii() and number.isdigit()) or int(number) < 1:
        raise argparse.ArgumentTypeError(f"expected a size such as 512M or 32G, not {text!r}")
    return int(number) * scale


def _machine_memory() -> int | None:
    """The machine's physical memory in bytes, or None where the system does not say."""
    try:
        memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, ValueError, OSError):
        return None
    return memory if memory > 0 else None


def _load(args: argparse.Namespace) -> Dag:
    if not args.files[0].endswith(SPIT_SUFFIXES):
        if len(args.files) > 1:
            raise DagError(f"expected one .spitdag, got {len(args.files)} files")
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


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.action is Action.RUN and args.cores < 1:
        parser.error("--cores must be at least 1")
    state: Store | None = None
    try:
        selection = _selection(args)
        dag = _load(args)
        everything = dag
        total = len(dag.jobs)
        selected = select(dag, selection)
        dag = selected.dag
        if selection:
            needed = f", and {selected.needed} they need" if selected.needed else ""
            print(f"chose {selected.matched} of {total} jobs{needed}", file=sys.stderr)
        state = Store(Path(args.state).expanduser().resolve() if args.state else dag.layout.state,
                      args.action is not Action.PLAN)
        if args.action is not Action.PLAN:
            state.lock()
        if dag.left_out:
            print(f"partial DAG: {len(dag.left_out)} output(s) left out", file=sys.stderr)

        if args.action is Action.ADOPT:
            adoptions = adopt(dag, state)
            for adoption in adoptions:
                print(f"[{adoption.job.id}] {adoption.status.value:7} {adoption.job.operation}: {adoption.reason}")
            return 0

        forced = selected.matched_ids if args.force else False
        compare = Compare(args.compare)
        if compare is not Compare.STAT:
            handed = relabel(dag, state.jobs, everything)
            # A plan sees the records handed over, but only a run stores them.
            if args.action is Action.RUN:
                state.apply(handed)
            else:
                state.assume(handed)
        decisions = plan(dag, state, forced, compare)
        if args.move_files and not args.force:
            # A job that is current as it stands has nothing to move; `find` leaves out one whose
            # inputs changed, which a moved output would not save from running.
            migrations = find(dag, state.jobs, {d.job.id for d in decisions if d.status in (Status.RUN, Status.MAYBE)})
            if args.action is Action.RUN:
                migrations = apply(dag, migrations, state.apply)
            else:
                # Judge the jobs as if the files were already where the pipeline wants them.
                pending = {new: stamp(dag.checked(old), new in dag.folders)
                           for m in migrations for old, new in m.moves}
                if pending:
                    decisions = plan(dag, state, forced, compare, pending)
            for migration in migrations:
                for old, new in migration.moves:
                    print(f"[{migration.job.id}] {'moved' if args.action is Action.RUN else 'would move'} {old} -> {new}")
                if not migration.moves:
                    print(f"[{migration.job.id}] not moved: {migration.reason}")
            moved = sum(1 for m in migrations if m.moves)
            if moved:
                print(f"{moved} job(s) reuse outputs by moving them to their new paths", file=sys.stderr)
            if args.action is Action.RUN and any(m.moves for m in migrations):
                print("outputs were moved to the paths the pipeline now gives them; a tool that wrote those paths "
                      "into its output is not rerun", file=sys.stderr)
                decisions = plan(dag, state, forced, compare)
        if args.action is Action.RUN:
            state.apply(writes(decisions))
        blocked = any(decision.status is Status.BLOCKED for decision in decisions)
        work, width = labels(dag.jobs), id_width(dag.jobs)
        if args.action is Action.PLAN and args.job is not None:
            print("\n".join(explain(dag, state, decisions, args.job, compare)))
            return 1 if any(d.job.id == args.job and d.status is Status.BLOCKED for d in decisions) else 0
        if args.action is Action.PLAN and args.json:
            held = Counter(d.caused_by for d in decisions if d.caused_by is not None)
            json.dump({
                "left_out": list(dag.left_out),
                "jobs": [
                    {"id": d.job.id, "operation": d.job.operation, "status": d.status.value, "reason": d.reason,
                     "waits_for": list(d.waits_for), "caused_by": d.caused_by, "holds_up": held[d.job.id]}
                    for d in decisions
                ],
            }, sys.stdout, indent=2)
            print()
        else:
            print(counts(decisions))
            for line in blocked_lines(decisions):
                print(line)
            if args.verbose:
                for decision in decisions:
                    print(job_line(decision, work, width))
        if args.action is Action.PLAN:
            return 1 if blocked else 0
        if blocked and not args.partial:
            held = sum(decision.status is Status.BLOCKED for decision in decisions)
            print(f"nothing was run: {jobs(held)} {'is' if held == 1 else 'are'} blocked. Fix the cause, or pass "
                  "--partial to run the jobs that are not.", file=sys.stderr)
            return 1

        if args.enforce_memory:
            running = [d.job for d in decisions if d.status in (Status.RUN, Status.MAYBE)]
            over = over_memory(running, args.memory)
            if over:
                for line in over:
                    print(line, file=sys.stderr)
                print("lower the job's `resources` line, give more with --memory, "
                      "or pass --no-enforce-memory to run it alone", file=sys.stderr)
                return 2

        logs = Path(args.logs).expanduser().resolve() if args.logs else dag.layout.logs
        options = Options(logs, args.cores, args.memory, compare, args.keep_failed, args.enforce_memory,
                          args.timestamp_logs)
        try:
            results = asyncio.run(execute(dag, decisions, state, options, run_text.printer(dag)))
        except asyncio.CancelledError:
            print("terminated; running jobs were stopped", file=sys.stderr)
            return 128 + signal.SIGTERM
        except KeyboardInterrupt:
            print("interrupted; running jobs were stopped", file=sys.stderr)
            return 128 + signal.SIGINT
        summary = Summary.of(decisions, results)
        run_text.report_failures(dag, summary, args.verbose)
        print(run_text.summary_line(summary), file=sys.stderr)
        left = stale(everything, dag, decisions, results) if selection else 0
        if left:
            print(run_text.stale_line(left), file=sys.stderr)
        failed = summary.failed or summary.not_run
        return 1 if failed or blocked else 0
    except (DagError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        if state is not None:
            state.close()


if __name__ == "__main__":
    raise SystemExit(main())
