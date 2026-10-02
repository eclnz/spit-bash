# spit-bash

`spit-bash` runs the jobs in a [SPIT](https://github.com/eclnz/spit) `dag --json` result or saved `.spitdag`. It reads the resolved jobs directly; it does not need the original `.spit` pipeline or `.spitout` inventory. This first version supports SPIT DAG format 4.

See the [runnable examples](examples/README.md) for complete recipes, input files, commands, and expected output.

## Install and use

Requires Python 3.10 or newer. From this repository:

```sh
python3 -m pip install .
spit dag dataset.spitin -o jobs.spitdag
spit-bash plan jobs.spitdag
spit-bash run jobs.spitdag -j 4
```

For `dag --json`, pipe directly into the planner or runner:

```sh
spit dag dataset.spitin --json | spit-bash plan -
spit dag dataset.spitin --json | spit-bash run - -j 4
```

SPIT writes the absolute dataset `root` into a DAG when it knows it. If `root` is `null`, pass `--root /path/to/dataset`. Relative paths in artifacts and commands are interpreted from this root.

`plan` reports `run`, `skip`, or `blocked` for each job, and exits 1 if any job is blocked. `plan --json` prints the same as JSON. `run` makes the same plan and runs every job that is not current or blocked. It runs each job's `verify` commands in order, then its main command, and only starts a dependent after its producers finish successfully. Independent jobs run together, up to `-j` processes, which defaults to the number of CPUs. Commands are passed as argument arrays directly to the operating system; no shell parses them. Output parent directories are created when a job starts. A command must create every declared output file to count as successful.

A blocked job blocks the jobs that depend on it, but not the rest of the plan: `run` still runs every other job, then exits 1. A job is blocked when an external input is missing, when the program its command or a `verify` command starts with cannot be found, or when a job it depends on is blocked. Programs are looked up on `PATH`, from SPIT's `executables` list; a program named by a path, such as `bin/check`, is looked up from the root.

## Output and logs

Each job's output, from its `verify` commands and its command, goes to `ROOT/.spit-bash/logs/ID-OPERATION.log`, which each run of the job replaces. The terminal shows only when each job starts and finishes, so parallel jobs do not mix their output. When a job fails, `run` prints the last lines of its log. `--logs DIR` puts the logs elsewhere.

## Stopping a run

Ctrl-C (SIGINT) or SIGTERM, as a cluster scheduler sends at a time limit, stops every running job's process group and exits with 130 or 143. Each job runs in a session of its own, so a tool's own child processes are stopped with it. A stopped job has no successful record, so the next run starts it again.

## Existing paths and reruns

The runner records successful jobs in `ROOT/.spit-bash/state.jsonl`, keyed by their output paths relative to the root, so a dataset keeps its state when it moves. A job runs if an output is missing, its SPIT fingerprint changed, an input or output file changed, or a producer will run. It skips only when its recorded fingerprint and file sizes and modification times still match. `--force` reruns all jobs that have commands.

Existing outputs with no successful record are run again; use `plan` to inspect this before `run`. For a dataset whose outputs were made before spit-bash was used, `spit-bash adopt` records every job whose inputs and outputs all exist as current, without running it. It trusts the files as they are, so adopt only outputs you know were made by the commands in the DAG.

Jobs with no command can only be skipped when all their outputs already exist and are at least as new as their inputs; a missing or stale output blocks them. Failed verification, failed commands, and missing outputs fail the job and its dependents. A partial DAG reports its `left_out` count while still planning and running its complete jobs.

The file checks use size and modification time, not content hashes. If another tool changes a file while preserving both, the runner will not notice. The state path can be changed with `--state PATH`; keep one state file per dataset root.

The state file is a log of JSON lines: a header, then one line each time a job is recorded or forgotten, and the last line for a job wins. Finishing a job appends a line rather than rewriting the file, and the end of a run compacts it to one line per job. `run` and `adopt` lock the state, so a second run on the same dataset stops with an error instead of overwriting the first one's records. Locking uses `flock`, so spit-bash runs on Linux and macOS, not Windows.

SPIT DAG version 4 is validated with Pydantic before planning. Invalid field types, unknown fields, malformed argument parts and paths outside the dataset root are rejected before any command runs.

## Development

```sh
python3 -m pip install -e '.[dev]'
messie -af .
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

The test fixture under `tests/fixtures/` is actual version 4 output from SPIT's `command_demo` example. `tests/test_examples.py` runs the [examples](examples/README.md) with a real `spit`: set `SPIT` to the binary or put it on `PATH`, or the tests are skipped. CI builds `spit` from SPIT's main branch for them, on every push and weekly, so a change to SPIT's DAG format fails here.
