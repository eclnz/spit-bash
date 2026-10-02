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

`plan` only reports `run`, `skip`, or `blocked` for each job. `run` applies the same plan. It checks external input files before starting, runs each job's `verify` commands in order, then its main command, and only starts a dependent after its producers finish successfully. Independent jobs may run together, up to `-j` processes. Commands are passed as argument arrays directly to the operating system; no shell parses them. Output parent directories are created when a job starts. A command must create every declared output file to count as successful.

## Existing paths and reruns

The runner stores successful jobs in `ROOT/.spit-bash/state.json`, keyed by their output paths. A job runs if an output is missing, its SPIT fingerprint changed, an input or output file changed, or a producer will run. It skips only when its recorded fingerprint and file sizes and modification times still match. Existing outputs with no successful state record are run again; use `plan` to inspect this before `run`. `--force` reruns all jobs that have commands.

Jobs with no command can only be skipped when all their outputs already exist and are at least as new as their inputs; a missing or stale output blocks the plan. Missing external inputs, failed verification, failed commands, and missing outputs block dependent jobs. A partial DAG reports its `left_out` count while still planning and running its complete jobs.

The file checks use size and modification time, not content hashes. If another tool changes a file while preserving both, the runner will not notice. The state path can be changed with `--state PATH`; keep one state file per dataset root.

SPIT DAG version 4 is validated with Pydantic before planning. Invalid field types, unknown fields, malformed argument parts and paths outside the dataset root are rejected before any command runs.

## Development

```sh
python3 -m pip install -e '.[dev]'
messie -af .
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

The test fixture under `tests/fixtures/` is actual version 4 output from SPIT's `command_demo` example.
