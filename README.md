# spit-bash

`spit-bash` runs the jobs of a [SPIT](https://github.com/eclnz/spit) pipeline on one machine. Give it a recipe and it runs `spit dag` for you; give it a saved `.spitdag` or `dag --json` output and it needs nothing else, not the original `.spit` pipeline or `.spitout` inventory. It reads SPIT DAG formats 4 to 7; format 5 added folder artifacts, format 6 [checks](#checks), and format 7 says which call each job came from, which `spit-bash` checks but does not use.

See the [runnable examples](examples/README.md) for complete recipes, input files, commands, and expected output.

## Install and use

Requires Python 3.10 or newer, and `spit` on `PATH` to run a recipe or pipeline. From this repository:

```sh
python3 -m pip install .
spit-bash plan dataset.spitin
spit-bash run dataset.spitin -j 4
```

`spit-bash` takes what `spit dag` takes, and runs `spit dag --json` on it:

| Files | Runs |
| --- | --- |
| `dataset.spitin` | `spit dag dataset.spitin --json` |
| `analysis.spit dataset.spitout` | `spit dag analysis.spit dataset.spitout --json` (`-` for a `.spitout` on stdin) |
| `analysis.spit --root data` | `spit dag analysis.spit --root data --json`, scanning `data` |

`--partial` is passed on to `spit dag`. `--spit PATH`, or the `SPIT` variable, names the `spit` program when it is not on `PATH`. SPIT's notes and errors appear as it prints them, and if `spit dag` fails, `spit-bash` stops with status 2 before planning anything.

A saved DAG, or one on stdin, is read directly:

```sh
spit dag dataset.spitin -o jobs.spitdag
spit-bash run jobs.spitdag -j 4
spit dag dataset.spitin --json | spit-bash run - -j 4
```

## Choosing jobs

`--only`, `--product` and `--stage` choose part of the DAG: the jobs whose outputs match, and every job upstream that they need. Nothing downstream is chosen. Try a pipeline on one subject before running every subject:

```sh
spit-bash run dataset.spitin --only sub=01
spit-bash run dataset.spitin --only sub=01,ses=02 --product denoised_dwi
spit-bash plan dataset.spitin --stage preprocess
```

| Option | Chooses jobs with |
| --- | --- |
| `--only DIM=VALUE[,DIM=VALUE...]` | an output with every one of these entities |
| `--product NAME` | an output of this product |
| `--stage NAME[/NAME...]` | this stage, or a stage inside it, such as `preprocess/combine` |

Repeat an option to match any of its values, as in `--only sub=01 --only sub=02`. Different options must all hold for the same output, so `--only sub=01 --product clean` chooses the jobs that make `clean` for subject 01. A job with no `sub` dimension, such as one making a group template, is not chosen by `--only sub=01`, but it runs if a chosen job needs it; a group average over every subject is not chosen, so it does not run. `plan` and `run` print how many jobs were chosen and how many more they need. A name that matches no output is an error that lists the names that would, so a misspelt dimension or product does not just choose nothing.

Choosing jobs does not change how a job is planned: a chosen job is still skipped when it is current, and the state records it as any run does.

## Planning and running

SPIT writes the absolute dataset `root` into a DAG when it knows it. If `root` is `null`, pass `--root /path/to/dataset`. Relative paths in artifacts and commands are interpreted from this root.

`plan` reports `run`, `check`, `skip`, `maybe` or `blocked` for each job, and exits 1 if any job is blocked. A `maybe` job is current as it stands, but a job it reads from will run, so whether it must is not known until that job has finished: see [Stopping at an unchanged output](#stopping-at-an-unchanged-output). `plan --json` prints the same as JSON. `run` makes the same plan and runs every job that is not current or blocked. It runs each job's input checks and `verify` commands in order, then its main command, then its output checks, and only starts a dependent after its producers finish successfully. Independent jobs run together, up to `-j` processes, which defaults to the number of CPUs. Commands are passed as argument arrays directly to the operating system; no shell parses them. Output parent directories are created when a job starts. A command must create every declared output file, or [folder](#folders), to count as successful.

A blocked job blocks the jobs that depend on it, but not the rest of the plan: `run` still runs every other job, then exits 1. A job is blocked when an external input is missing, when the program its command, a `verify` command or a check starts with cannot be found, or when a job it depends on is blocked. Programs are looked up on `PATH`, from SPIT's `executables` list; a program named by a path, such as `bin/check`, is looked up from the root.

## Folders

A DAG of format 5 marks each artifact as a `file` or a `folder`, for a product SPIT declares with a `/` after its type, such as `source dicom : Dicom / [sub]`. The runner treats a folder as one artifact:

- A source folder must exist as a folder, or the jobs that read it are blocked.
- Before a job's command runs, and after its `verify` commands pass, the runner removes whatever an earlier run left at each of its output folders. The job owns the folder, since SPIT puts no other artifact inside one, and old files would otherwise mix with the new run's. Then it makes the folder's parent, but not the folder: the tool makes it, as `cp -R` does, and some tools refuse to write into a folder that exists. A tool that needs the folder to exist first, such as `dcm2niix -o`, is wrapped in a script that makes it.
- After the command, each output folder must exist. It may be empty.
- A folder counts as changed when anything under it does. Its record is the total size of the files under it, the latest modification time of any of them or of any folder under it, and how many files it holds. A folder's own modification time is not enough, as it changes only when an entry directly in it is added, removed or renamed.

## Checks

A DAG of format 6 gives each job its `checks`: commands that each test one artifact, which the pipeline declares once with SPIT's `check` and attaches to ports and sources. A `before` check tests an input before the job's `verify` commands; an `after` check tests an output once the command has made it. A failed check fails the job, even when its command exited 0, and the jobs that depend on it do not run:

```text
[2] failed: check contains(today) on output output output/copied/day=tuesday.txt failed: exited with status 1
```

A successful run records the checks the job passed. SPIT leaves checks out of a job's fingerprint, so a changed or new check does not rerun the job: `plan` marks it `check`, and `run` runs its checks alone on the files it already has. If they pass, the record takes the new checks; if one fails, the job fails and its record is dropped, so the next run reruns the job. A job with no command whose outputs exist is checked on every run. A dependent that runs or is checked in the same run waits for those checks, and fails if they do; a current dependent stays current. `adopt` records no checks, so the next `run` checks the adopted files.

## Output and logs

Each job's output, from its checks, its `verify` commands and its command, goes to `ROOT/.spit-bash/logs/ID-OPERATION.log`, which each run of the job replaces. The terminal shows only when each job starts and finishes, so parallel jobs do not mix their output. When a job fails, `run` prints the last lines of its log. `--logs DIR` puts the logs elsewhere.

## Programs

SPIT's fingerprint covers a command's text, not the program it starts, so upgrading `sort`, or editing a script a command runs, would leave a job looking current. Each run hashes the file every command, `verify` command and check starts with: found on `PATH`, or for a name with a `/` under the dataset root. A job records the hashes of its programs when it succeeds, and `plan` marks it `run`, with the reason `program changed: sort`, when one of them differs. Each program is hashed once per command, however many jobs use it. A job recorded before programs were tracked has none, which counts as unknown, not changed, so upgrading spit-bash reruns nothing. `adopt` records the programs it finds now.

A program's hash covers its file and, for a script, the interpreter its `#!` line names. It also covers every file under the root that a command names as an argument, or after an `=` in one as in `--config=run.yaml`, unless the job declares it as an input or output, which are judged by content already: `python3 lib/script.py` reruns when `script.py` changes. What the runner cannot see is what a program loads by itself, such as an interpreter's packages. Name it with `--track PROGRAM=PATH`, a file or folder under the root, such as `--track python3=env/lib/site-packages`: the jobs that start with `PROGRAM` rerun when it changes. Tracked folders are hashed in full on every `plan` and `run`. A program that cannot be found blocks the jobs that must run, as before, and does not rerun one that is current. `--ignore-programs` on `plan`, `run` and `adopt` neither compares nor records programs, for a machine where programs are updated often and outputs are not meant to change.

## Stopping at an unchanged output

A job that reads the output of a job that will run used to run too. Now it is `maybe` when its own record is current: the same fingerprint, files and checks as the last successful run. `plan` says how many jobs will run and how many more may. `run` waits for the jobs a `maybe` job reads from, then compares the files it now reads with its record, by size and time and, where those differ, by content. If the producer wrote the same bytes, the job is skipped, its new stamps are stored, and its own dependents are settled the same way. If they differ, it runs.

A job with a reason of its own, such as a changed input, a missing output or a changed fingerprint, is `run`, not `maybe`. So is a job whose input does not exist yet, since there is nothing to compare. `--stat-only` turns the early stop off, as it has no hashes to compare: every job below a rerun is `run`, as before. A job with no command that reads a `maybe` job is blocked, as it is when it reads a `run` job. A `maybe` job whose program is missing is blocked, as a `run` job is, even if it would be skipped.

## Stopping a run

Ctrl-C (SIGINT) or SIGTERM, as a cluster scheduler sends at a time limit, stops every running job's process group and exits with 130 or 143. Each job runs in a session of its own, so a tool's own child processes are stopped with it. A stopped job has no successful record, so the next run starts it again.

## Existing paths and reruns

The runner records successful jobs in `ROOT/.spit-bash/state.jsonl`, keyed by their output paths relative to the root, so a dataset keeps its state when it moves. A job runs if an output is missing, its SPIT fingerprint changed, a program its commands start with changed, an input or output file changed, or a producer will run. It skips only when its recorded fingerprint still matches and its files are as recorded: the same size and modification time, or, where those differ, the same content. A `touch`, a `git checkout` or a copy that leaves the bytes alone does not rerun a job. `--force` reruns all jobs that have commands.

Existing outputs with no successful record are run again; use `plan` to inspect this before `run`. For a dataset whose outputs were made before spit-bash was used, `spit-bash adopt` records every job whose inputs and outputs all exist as current, without running it. It trusts the files as they are, so adopt only outputs you know were made by the commands in the DAG.

Jobs with no command can only be skipped, or checked, when all their outputs already exist and are at least as new as their inputs; a missing or stale output blocks them. Failed verification, failed commands, failed checks and missing outputs fail the job and its dependents. A partial DAG reports its `left_out` count while still planning and running its complete jobs.

A file is first compared by size and modification time, which costs one `stat`, and only a file whose stamp differs from the record is read and hashed. A folder's stamp sums these over the files under it, and its hash covers the names of everything under it and the bytes of its files. When the hash matches, `run` stores the new stamp, so the next plan need not hash the file again; `plan` itself changes nothing. If another tool changes a file while preserving both its size and its modification time, the runner will not notice. After a job runs, the hashes of its inputs and outputs are recorded; `adopt` records none, so a touched file of an adopted job counts as changed once. `--stat-only` on `plan` and `run` judges by size and time alone, as before, and records no hashes. The hash is xxh3_128 from the `xxhash` package, or BLAKE2b where that has no wheel, and each recorded hash names its algorithm, so one made with another is never taken as equal. The state path can be changed with `--state PATH`; keep one state file per dataset root.

The state file is a log of JSON lines: a header, version 4 since records hold the hash of each file as well as the checks each job passed, then one line each time a job is recorded or forgotten, and the last line for a job wins. Finishing a job appends a line rather than rewriting the file, and the end of a run compacts it to one line per job. `run` and `adopt` lock the state, so a second run on the same dataset stops with an error instead of overwriting the first one's records. Locking uses `flock`, so spit-bash runs on Linux and macOS, not Windows. A version 2 or 3 state is still read; its records hold no checks or no hashes, and the next run that records a job adds them.

A SPIT DAG is validated with Pydantic before planning. Invalid field types, unknown fields, malformed argument parts and paths outside the dataset root are rejected before any command runs.

## Development

```sh
python3 -m pip install -e '.[dev]'
messie -af .
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

The test fixture under `tests/fixtures/` is actual version 4 output from SPIT's `command_demo` example. `tests/test_examples.py` runs the [examples](examples/README.md) with a real `spit`: set `SPIT` to the binary or put it on `PATH`, or the tests are skipped. CI builds `spit` from SPIT's `dev` branch, where its work merges, for them, on every push and weekly, so a change to SPIT's DAG format fails here. A branch whose name SPIT also has, other than `main` and `dev`, builds SPIT's branch of that name instead, so a change made in both repositories is tested together before SPIT's half merges.
