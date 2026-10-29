# spit-bash

`spit-bash` runs the jobs of a [SPIT](https://github.com/eclnz/spit) pipeline on one machine. Give it a recipe and it runs `spit dag` for you; give it a saved `.spitdag` or `dag --json` output and it needs nothing else, not the original `.spit` pipeline or `.spitout` inventory. It reads version 8 of SPIT's DAG format, and no other: a DAG of another version is rejected before anything runs, so use a `spit` and a `spit-bash` that match.

See the [runnable examples](examples/README.md) for the recipes in SPIT's repository, their commands, and expected output.

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
| `analysis.spit` with a `root` line | `spit dag analysis.spit --json`: a small pipeline names its dataset root itself, so it needs no `.spitin` |
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

`plan` reports `run`, `check`, `skip`, `maybe` or `blocked` for each job, and exits 1 if any job is blocked. A `maybe` job is current as it stands, but a job it reads from will run, so whether it must is not known until that job has finished: see [Stopping at an unchanged output](#stopping-at-an-unchanged-output). `plan --json` prints the same as JSON. `run` makes the same plan and runs every job that is not current or blocked. It runs each job's input checks and `verify` commands in order, then its main command, then its output checks, and only starts a dependent after its producers finish successfully. Independent jobs run together, as many as the run's cores and memory hold: see [Cores and memory](#cores-and-memory). When more jobs are ready than can start, the one at the head of the longest chain of work still to come starts first: each job is ranked by its own expected time, as in [the estimate](#output-and-logs), plus the longest of the ranks of the jobs that depend on it, a job with no time counting as the median of the times the state holds, or 1 second when it holds none. Jobs of equal rank start in DAG order. So a long chain, such as a subject's preprocessing followed by its model fit, starts early, rather than finishing alone at the end of the run while the other cores sit idle. Commands are passed as argument arrays directly to the operating system; no shell parses them. Output parent directories are created when a job starts. A command must create every declared output file, or [folder](#folders), to count as successful. A declared output is never optional: a job whose command exits 0 but leaves one missing fails, records no success, runs no output checks and holds back its dependents, and the failure names each missing output's port and path, as `command succeeded but did not create output meta derivatives/image/sub=01.json`. A tool run with a flag that turns a file off, such as `dcm2niix -b n` and its `.json`, needs an operation that does not declare that file.

A blocked job blocks the jobs that depend on it, but not the rest of the plan: `run` still runs every other job, then exits 1. A job is blocked when an external input is missing, when the program its command, a `verify` command or a check starts with cannot be found, or when a job it depends on is blocked. Programs are looked up on `PATH`, from SPIT's `executables` list; a program named by a path, such as `bin/check`, is looked up from the root.

## Folders

A DAG marks each artifact as a `file` or a `folder`, for a product SPIT declares with a `/` after its type, such as `source dicom : Dicom / [sub]`. The runner treats a folder as one artifact:

- A source folder must exist as a folder, or the jobs that read it are blocked.
- Before a job's command runs, and after its `verify` commands pass, the runner removes whatever an earlier run left at each of its output folders. The job owns the folder, since SPIT puts no other artifact inside one, and old files would otherwise mix with the new run's. Then it makes the folder again, empty, for the tool to write into, as tools given a folder to fill, such as `bwa index -p` or `dcm2niix -o`, expect. Copy a folder's contents into it with `cp -R {notes}/. {@output}`: `cp -R {notes} {@output}` puts a copy of `notes` inside the folder. A tool that refuses to write into a folder that exists is wrapped in a command that removes it first.
- After the command, each output folder must exist. It may be empty. If the job fails once its command has started, the folder is [removed](#when-a-job-fails) with everything in it.
- A folder counts as changed when anything under it does. Its record is the total size of the files under it, the latest modification time of any of them or of any folder under it, and how many files it holds. A folder's own modification time is not enough, as it changes only when an entry directly in it is added, removed or renamed.

## Checks

A DAG gives each job its `checks`: commands that each test one artifact, which the pipeline declares once with SPIT's `check` and attaches to ports and sources. A `before` check tests an input before the job's `verify` commands; an `after` check tests an output once the command has made it. A failed check fails the job, even when its command exited 0, and the jobs that depend on it do not run:

```text
[2] failed: check contains(today) on output output output/copied/day=tuesday.txt failed: exited with status 1
```

A successful run records the checks the job passed. SPIT leaves checks out of a job's fingerprint, so a changed or new check does not rerun the job: `plan` marks it `check`, and `run` runs its checks alone on the files it already has. If they pass, the record takes the new checks; if one fails, the job fails and its record is dropped, so the next run reruns the job. A job with no command whose outputs exist is checked on every run. A dependent that runs or is checked in the same run waits for those checks, and fails if they do; a current dependent stays current. `adopt` records no checks, so the next `run` checks the adopted files.

## Where jobs come from

In SPIT, an operation can be carried out by a body of steps, often declared in a library, and a call to it becomes ordinary jobs. The DAG says where each such job came from: the files the pipeline was read from, each call, and the line of the body's step. When one of these jobs fails, `run` prints the step and every call it is nested in, innermost first:

```text
[1] failed: command exited with status 3
[1] step at libs/lib.spit line 10
[1] in `m::cleaned = L::tidy(...)` at libs/lib.spit line 13
[1] in `m = L::summarise(...)` at main.spit line 8
```

None of this is part of a job's fingerprint, so an edit to a library that leaves a job's command as it was reruns nothing.

## Standard input and output

A SPIT command written `wc -l < {table} > {@output}` reads an input file on its standard input and writes an output file on its standard output. The DAG gives the job's command as `wc -l` alone, and names the two files in the job's `stdin` and `stdout`:

- The runner opens the `stdin` file for reading as the command's standard input. A command without one reads nothing: its standard input is `/dev/null`.
- The runner writes the command's standard output to a temporary file beside the `stdout` output, named like `.x.txt.spit-bash-partial` for `x.txt`, and renames it onto the output only once the command exits 0. So a failed or stopped command never leaves a partial output at its path, and the temporary file is removed with the job's other outputs. After the rename the output is like any other: it must exist, which an empty file does, and its output checks run on it.
- The command's standard error goes to the job's log in either case. A command without `stdout` has its standard output logged as well.

The log shows the redirections on the command's line, as a shell would write them:

```text
$ tr a-z A-Z < input/allotment.txt > output/loud/plot=allotment.txt
```

`verify` commands and checks never redirect. When the DAG is loaded, a job's `stdin` must be one of its input files and its `stdout` one of its output files, neither a folder, and a job with no command has neither; a DAG that breaks one of these is rejected.

## Cores and memory

A job's `cores` and `memory` in the DAG come from its operation's `resources` line, as in `resources merge: cores=2, memory=64M`: a job needs one core and claims no memory when its operation declares neither. `run` has a number of cores and an amount of memory to share among the jobs it runs at once:

```sh
spit-bash run dataset.spitin -j 16 --memory 64G
```

- `-j CORES` is the cores to use, by default the number of CPUs. A pipeline that declares no `resources` runs `-j` jobs at once.
- `--memory SIZE` is the memory to use, a whole number with a unit `K`, `M`, `G` or `T`, each 1024 times the one before, as in a `resources` line, or a number of bytes. By default it is the machine's physical memory; where the system does not report it, memory sets no limit.
- A job starts only when its cores and memory are free beside those of the jobs already running, and holds them until its output checks finish. A job with no memory claim needs none.
- A job's declared memory is a limit. While its `verify` commands and its main command run, `run` reads twice a second how much memory the command's process group holds, from `/proc` on Linux and `ps` elsewhere, and kills the group at once when it holds more than the job declares. The job fails, as `command used 212M of memory, more than the 64M its job declares, and was killed`, and loses its outputs as any failed job does. Raise the job's `resources` line and run again. A job with no memory claim is not limited, and checks are not. The reading counts each process's resident memory, so pages that forked workers share count once for each worker, and a group of them can read higher than it holds.
- A job that declares more memory than `--memory` is refused before anything runs, and `run` exits 2 naming it, as `[3] align needs 64G of memory where the run has 32G`.
- Ready jobs start in rank order and no other. While the job at the head waits for cores or memory, the jobs behind it wait too, even ones that would fit: the head is on the longest chain of work, and letting smaller jobs past it could delay the run or keep a big job waiting for good. Some cores may sit idle while a big job waits for enough to free.
- A job that needs more cores than `-j` takes all of the run's cores and memory: it starts once no other job is running, and runs alone. `run` says so as it starts, as `[3] needs 16 cores where the run has 8; it will run alone, once no other job is running`.
- `--no-enforce-memory` turns both memory rules off: a job may hold more than it declares, and one that declares more than `--memory` runs alone, as a job needing more cores does.
- A job planned `check` runs only its checks, but waits for the job's cores and memory as a run of it does.

## When a job fails

A command writes its outputs where they belong, so one that fails can leave a truncated file that another tool, a person or `spit discover` would take for a finished one. Once a job's command has been started, any failure removes everything at the job's declared outputs, files and folders alike: a non-zero exit, a command that could not start, a missing output, an input that changed while it ran, a failed output check, or a run that was stopped. An output that passed the command but failed a check is complete but rejected, and is removed too. The runner removes them only after the command's process group has stopped, so nothing is still writing, and the job's log ends with a line for each output it removed:

```text
spit-bash: removed failed output work/upper.txt
```

A job that fails before its command starts, at a missing input, an input check or a `verify` command, keeps what is at its outputs: the command never touched them, so they are still an earlier run's, whole. The job's record is still dropped, so the next run runs it again. To debug a tool, `run --keep-failed` leaves what a failed command wrote where it is, and the log says `spit-bash: kept failed output work/upper.txt` instead. An output the command writes on its standard output stays in its temporary file, such as `work/.upper.txt.spit-bash-partial`, and is never renamed onto the output. The job's record is dropped all the same, so a kept output is never taken for a finished one: the next run runs the job again. The log also holds what the command printed, other than an output it writes on its standard output, and each command it ran, after `$ `, to run again by hand.

## Output and logs

Each job's output, from its checks, its `verify` commands and its command, goes to `ROOT/.spit-bash/logs/ID-OPERATION.log`, except a command's standard output when it is [one of the job's outputs](#standard-input-and-output), which each run of the job replaces. The terminal shows only when each job starts and finishes, so parallel jobs do not mix their output. When a job fails, `run` prints the last lines of its log. `--logs DIR` puts the logs elsewhere.

`run` also says how long it expects to take, and as each job finishes, how long it took and how many jobs are left:

```text
6 jobs to run, about 4s
[1] start slow
[2] start slow
[1] done in 2s; 5 left, about 2s
```

A job is expected to take as long as its last successful run did, which the state records. A job that has not run before is expected to take the mean time of its operation's other jobs, from earlier runs and from this one. Until every job left has an estimate, `run` gives the count alone. A `maybe` job counts as one that runs until it is skipped, so the estimate is the longest the run can take. The estimate is the longer of two times the run cannot beat: the work left, each job's time multiplied by its cores, divided among the `-j` cores, and the longest chain of jobs left that each wait for the one before, such as a group template made after every subject and then a statistic made from it, which more cores do not shorten. A running job counts for the time it has left. The longest chain is worked out again at most every 5 seconds, as times come in. Neither allows for how the jobs will actually fit together on the cores, nor for jobs waiting for memory, so the run may take longer than either.

## Stopping at an unchanged output

A job that reads the output of a job that will run used to run too. Now it is `maybe` when its own record is current: the same fingerprint, files and checks as the last successful run. `plan` says how many jobs will run and how many more may. `run` waits for the jobs a `maybe` job reads from, then compares the files it now reads with its record, by size and time and, where those differ, by content. If the producer wrote the same bytes, the job is skipped, its new stamps are stored, and its own dependents are settled the same way. If they differ, it runs.

A job with a reason of its own, such as a changed input, a missing output or a changed fingerprint, is `run`, not `maybe`. So is a job whose input does not exist yet, since there is nothing to compare. `--stat-only` turns the early stop off, as it has no hashes to compare: every job below a rerun is `run`, as before. A job with no command that reads a `maybe` job is blocked, as it is when it reads a `run` job. A `maybe` job whose program is missing is blocked, as a `run` job is, even if it would be skipped.

## Stopping a run

Ctrl-C (SIGINT) or SIGTERM, as a cluster scheduler sends at a time limit, stops every running job's process group and exits with 130 or 143. Each job's group gets SIGTERM, then SIGKILL once the command has exited or five seconds have passed, whichever is first, so a tool that ignores SIGTERM or hangs while cleaning up cannot keep the run from stopping. Each job runs in a session of its own, so a tool's own child processes are stopped with it. A stopped job has no successful record, so the next run starts it again, and what its command had written at its outputs is [removed](#when-a-job-fails).

## Existing paths and reruns

The runner records successful jobs in `ROOT/.spit-bash/state.jsonl`, keyed by their output paths relative to the root, so a dataset keeps its state when it moves. A job runs if an output is missing, its SPIT fingerprint changed, an input or output file changed, or a producer will run. It skips only when its recorded fingerprint still matches and its files are as recorded: the same size and modification time, or, where those differ, the same content. A `touch`, a `git checkout` or a copy that leaves the bytes alone does not rerun a job. `--force` reruns all jobs that have commands.

Existing outputs with no successful record are run again; use `plan` to inspect this before `run`. For a dataset whose outputs were made before spit-bash was used, `spit-bash adopt` records every job whose inputs and outputs all exist as current, without running it. It trusts the files as they are, so adopt only outputs you know were made by the commands in the DAG.

Jobs with no command can only be skipped, or checked, when all their outputs already exist and are at least as new as their inputs; a missing or stale output blocks them. Failed verification, failed commands, failed checks and missing outputs fail the job and its dependents, and a failure once the command has started [removes the job's outputs](#when-a-job-fails). A partial DAG reports its `left_out` count while still planning and running its complete jobs.

A file is first compared by size and modification time, which costs one `stat`, and only a file whose stamp differs from the record is read and hashed. A folder's stamp sums these over the files under it, and its hash covers the names of everything under it and the bytes of its files. When the hash matches, `run` stores the new stamp, so the next plan need not hash the file again; `plan` itself changes nothing. If another tool changes a file while preserving both its size and its modification time, the runner will not notice. After a job runs, the hashes of its inputs and outputs are recorded; `adopt` records none, so a touched file of an adopted job counts as changed once. `--stat-only` on `plan` and `run` judges by size and time alone, as before, and records no hashes. The hash is xxh3_128 from the `xxhash` package, or BLAKE2b where that has no wheel, and each recorded hash names its algorithm, so one made with another is never taken as equal. The state path can be changed with `--state PATH`; keep one state file per dataset root.

The state file is a log of JSON lines: a header, version 5 since records hold how long each job took, as well as the hash of each file and the checks each job passed, then one line each time a job is recorded or forgotten, and the last line for a job wins. Finishing a job appends a line rather than rewriting the file, and the end of a run compacts it to one line per job. `run` and `adopt` lock the state, so a second run on the same dataset stops with an error instead of overwriting the first one's records. Locking uses `flock`, so spit-bash runs on Linux and macOS, not Windows. A version 2, 3 or 4 state is still read; its records hold no checks, no hashes or no times, and the next run that records a job adds them.

A SPIT DAG is validated with Pydantic before planning. Invalid field types, unknown fields, malformed argument parts and paths outside the dataset root are rejected before any command runs.

## Development

```sh
python3 -m pip install -e '.[dev]'
messie -af .
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

The test fixture under `tests/fixtures/` is actual version 8 output from SPIT's `command_demo` example, written without a root. `tests/test_examples.py` runs the [examples](examples/README.md), which live in SPIT's repository under `examples/runnable` so that SPIT's own tests keep them in step with its syntax, with a real `spit`: set `SPIT` to the binary or put it on `PATH`, and `SPIT_REPOSITORY` to the checkout it was built from (a sibling `../spit` by default), or the tests are skipped. CI builds `spit` from SPIT's `dev` branch, where its work merges, for them, on every push and weekly, so a change to SPIT's DAG format fails here. A branch whose name SPIT also has, other than `main` and `dev`, builds SPIT's branch of that name instead, so a change made in both repositories is tested together before SPIT's half merges. Once SPIT's branch has merged into `dev`, CI builds `dev` again, so a branch left behind after its merge never stands in for `dev`.
