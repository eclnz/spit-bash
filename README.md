# spit-bash

`spit-bash` runs the jobs of a [SPIT](https://github.com/eclnz/spit) pipeline on one machine. Give it a recipe and it runs `spit dag` for you; give it a saved `.spitdag` or `dag --json` output and it needs nothing else, not the original `.spit` pipeline or `.spitout` inventory. It reads version 10 of SPIT's DAG format, and no other: a DAG of another version is rejected before anything runs, so use a `spit` and a `spit-bash` that match.

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

Choosing jobs does not change how a job is planned: a chosen job is still skipped when it is current, and the state records it as any run does. Jobs outside the choice are not run, so one that reads an output a chosen job rewrote is out of date afterwards. `run` counts them in a line after its summary, as `10 jobs outside the selection read outputs this run changed; run without the selection to bring them up to date`.

`--force` with a selection reruns only the jobs whose outputs matched it. Their prerequisites still run when stale and skip when current:

```sh
spit-bash run analysis.spitin --only sub=01 --force
```

Without `--only`, `--product` or `--stage`, `--force` reruns every command job. A downstream job that reads an output rewritten by a forced job is outside a partial selection, and the usual stale-job count reports it.

## Scripts as inputs

The runner identifies a job by its command text, and the DAG lists files a job declares as inputs. To make edits to a script invalidate just the operation that reads it, declare the script as a source and pass that source to the operation:

```spit
source script : Script .py
source lines : Lines .txt

default path: out/{@product}
path script: scripts/transform.py
path lines: input.txt

operation transform(script: Script, lines: Lines) -> Lines .txt
command transform: python3 {script} {lines} > {@output}

result = transform(script, lines)
```

With `scripts/transform.py` and `input.txt` under the dataset root, run the pipeline with `spit-bash run analysis.spit --root DATASET`. The compiler puts `scripts/transform.py` in the job's input list. When it changes, that operation reruns; a command argument that is not declared as an input is not inferred as one.

## Planning and running

SPIT writes the absolute dataset `root` into a DAG when it knows it. If `root` is `null`, pass `--root /path/to/dataset`. Relative paths in artifacts and commands are interpreted from this root.

`plan` reports `run`, `check`, `skip`, `maybe` or `blocked` for each job, and exits 1 if any job is blocked. A `maybe` job is current as it stands, but a job it reads from will run, so whether it must is not known until that job has finished: see [Stopping at an unchanged output](#stopping-at-an-unchanged-output). `plan --json` prints the same as JSON. `run` makes the same plan, but prints it as counts (`3 run, 1270 skip`) and a line for each job that will do something, the first 20 of them, since a rerun of a large DAG would otherwise print a line for every current job; `plan` and `run --verbose` list them all. It runs every job that is not current or blocked. It runs each job's input checks and `verify` commands in order, then its [commands](#several-commands), then its output checks, and only starts a dependent after its producers finish successfully. Independent jobs run together, as many as the run's cores and memory hold: see [Cores and memory](#cores-and-memory). When more jobs are ready than can start, the one at the head of the longest chain of work still to come starts first: each job is ranked by its own expected time, as in [the estimate](#output-and-logs), plus the longest of the ranks of the jobs that depend on it, a job with no time counting as the median of the times the state holds, or 1 second when it holds none. Jobs of equal rank start in DAG order. So a long chain, such as a subject's preprocessing followed by its model fit, starts early, rather than finishing alone at the end of the run while the other cores sit idle. Commands are passed as argument arrays directly to the operating system; no shell parses them. Output parent directories are created when a job starts. A command must create every declared output file, or [folder](#folders), to count as successful. A declared output is never optional: a job whose commands exit 0 but leave one missing fails, records no success, runs no output checks and holds back its dependents, and the failure names each missing output's port and path, as `command succeeded but did not create output meta derivatives/image/sub=01.json`. A tool run with a flag that turns a file off, such as `dcm2niix -b n` and its `.json`, needs an operation that does not declare that file.

`plan --job ID` explains one job's decision from the same plan: the status and reason, the job's recorded and current fingerprint and checks, the input and output files that differ from the record with their recorded and current sizes and times (a folder's total size, file count and latest time), whether a file whose stamp differs has the same content, different content, or no recorded hash to compare, and the upstream jobs that will or may rewrite its inputs. It runs nothing and writes nothing, not even to a state log with a truncated last line. An unknown job id is an error.

```text
Job 1: op1
  decision: run, input changed
  fingerprint: same (recorded 620b9a143dd25648, now 620b9a143dd25648)
  checks: same (recorded none, now none)
  inputs that differ from the record:
    source.txt
      recorded: 5 bytes, mtime_ns 1794032177977430844
      now:      8 bytes, mtime_ns 1794032178028297347
      content differs
```

A command that names a script kept beside the pipeline with `{@here}` gets the pipeline's absolute folder from the DAG's `pipeline_folder`, so such a DAG runs on the machine whose folder it names; run it elsewhere by planning from the recipe there. A blocked job blocks the jobs that depend on it, but not the rest of the plan: `run` still runs every other job, then exits 1. A job is blocked when an external input is missing, when the program its command, a `verify` command or a check starts with cannot be found, or when a job it depends on is blocked. Programs are looked up on `PATH`, from SPIT's `executables` list; a program named by a path, such as `bin/check`, is looked up from the root.

## Folders

A DAG marks each artifact as a `file` or a `folder`, for a product SPIT declares with a `/` after its type, such as `source dicom : Dicom / [sub]`. The runner treats a folder as one artifact:

- A source folder must exist as a folder, or the jobs that read it are blocked.
- Before a job's first command runs, and after its `verify` commands pass, the runner moves whatever an earlier run left at each of its output folders [aside](#when-a-job-fails). The job owns the folder, since SPIT puts no other artifact inside one, and old files would otherwise mix with the new run's. Then it makes the folder again, empty, for the tool to write into, as tools given a folder to fill, such as `bwa index -p` or `dcm2niix -o`, expect. Copy a folder's contents into it with `cp -R {notes}/. {@output}`: `cp -R {notes} {@output}` puts a copy of `notes` inside the folder. A tool that refuses to write into a folder that exists is wrapped in a command that removes it first.
- After the last command, each output folder must exist. It may be empty. If the job fails once its first command has started, the folder is [removed](#when-a-job-fails) with everything in it.
- A folder counts as changed when anything under it does. Its record is the total size of the files under it, the latest modification time of any of them or of any folder under it, and how many files it holds. A folder's own modification time is not enough, as it changes only when an entry directly in it is added, removed or renamed.

## Checks

A DAG gives each job its `checks`: commands that each test one artifact, which the pipeline declares once with SPIT's `check` and attaches to ports and sources. A `before` check tests an input before the job's `verify` commands; an `after` check tests an output once the command has made it. A failed check fails the job, even when its command exited 0, and the jobs that depend on it do not run:

```text
[2] failed: check contains(today) on output output output/copied/day=tuesday.txt failed: exited with status 1
```

A successful run records the checks the job passed. The runner leaves checks out of a job's fingerprint, so a changed or new check does not rerun the job: `plan` marks it `check`, and `run` runs its checks alone on the files it already has. If they pass, the record takes the new checks; if one fails, the job fails and its record is dropped, so the next run reruns the job. A job with no command whose outputs exist is checked on every run. A dependent that runs or is checked in the same run waits for those checks, and fails if they do; a current dependent stays current. `adopt` records no checks, so the next `run` checks the adopted files.

## Where jobs come from

In SPIT, an operation can be carried out by a body of steps, often declared in a library, and a call to it becomes ordinary jobs. The DAG says where each such job came from: the files the pipeline was read from, each call, and the line of the body's step. When one of these jobs fails, `run` prints the step and every call it is nested in, innermost first:

```text
[1] failed: command exited with status 3
[1] step at libs/lib.spit line 10
[1] in `m::cleaned = L::tidy(...)` at libs/lib.spit line 13
[1] in `m = L::summarise(...)` at main.spit line 8
```

None of this is part of a job's fingerprint, so an edit to a library that leaves a job's command as it was reruns nothing.

## Several commands

A job's `commands` in the DAG run in order, each once the one before it exits 0. A SPIT command written as a block runs a line each, as an aligner's output is sorted and then indexed in one job:

```text
command sort:
    samtools sort --threads {@cores} -o {bam} {sam}
    samtools index {bam}
```

- Each command runs as a job's only command does: in a process group and session of its own, stopped with the job when the run is, held to the job's memory, with the standard input and output described [below](#standard-input-and-output).
- The first command that fails stops the job, and the commands after it do not run. The failure names it by its place, from 1, as `command 2 exited with status 1`; a job of one command says `command`, as `command exited with status 1`.
- Once the first command has started, a failure in any of them [removes the job's outputs](#when-a-job-fails), what an earlier command wrote included, and `--keep-failed` keeps them.
- The log shows each command after `$ `, in the order they ran:

```text
$ samtools sort --threads 4 -o out/bam/sample=A.bam out/sam/sample=A.sam
$ samtools index out/bam/sample=A.bam
```

## Standard input and output

A SPIT command written `wc -l < {table} > {@output}` reads an input file on its standard input and writes an output file on its standard output. The DAG gives the command as `wc -l` alone, and names the two files in its `stdin` and `stdout`:

- The runner opens the `stdin` file for reading as the command's standard input. A command without one reads nothing: its standard input is `/dev/null`.
- The runner writes the command's standard output to a temporary file beside the `stdout` output, named like `.x.txt.spit-bash-partial` for `x.txt`, and renames it onto the output only once the command exits 0, before the next command runs, which may read it. So a failed or stopped command never leaves a partial output at its path, and the temporary file is removed with the job's other outputs. After the rename the output is like any other: it must exist, which an empty file does, and its output checks run on it.
- The command's standard error goes to the job's log in either case. A command without `stdout` has its standard output logged as well.

The log shows the redirections on the command's line, as a shell would write them:

```text
$ tr a-z A-Z < input/allotment.txt > output/loud/plot=allotment.txt
```

`verify` commands and checks never redirect. When the DAG is loaded, each command's `stdin` must be one of the job's input files and its `stdout` one of its output files, neither a folder, and one of a job's commands at most has a `stdout`; a DAG that breaks one of these is rejected.

## Cores and memory

A job's `cores` and `memory` in the DAG come from its operation's `resources` line, as in `resources merge: cores=2, memory=64M`: a job needs one core and claims no memory when its operation declares neither. `run` has a number of cores and an amount of memory to share among the jobs it runs at once:

```sh
spit-bash run dataset.spitin -j 16 --memory 64G
```

- `-j CORES` is the cores to use, by default the number of CPUs. A pipeline that declares no `resources` runs `-j` jobs at once.
- `--memory SIZE` is the memory to use, a whole number with a unit `K`, `M`, `G` or `T`, each 1024 times the one before, as in a `resources` line, or a number of bytes. By default it is the machine's physical memory; where the system does not report it, memory sets no limit.
- A job starts only when its cores and memory are free beside those of the jobs already running, and holds them until its output checks finish. A job with no memory claim needs none.
- A job's declared memory is a limit. While each of its `verify` commands and commands runs, `run` reads twice a second how much memory the command's process group holds, from `/proc` on Linux and `ps` elsewhere, and kills the group at once when it holds more than the job declares. The job fails, as `command used 212M of memory, more than the 64M its job declares, and was killed`, and loses its outputs as any failed job does. Raise the job's `resources` line and run again. A job with no memory claim is not limited, and checks are not. The reading counts each process's resident memory, so pages that forked workers share count once for each worker, and a group of them can read higher than it holds.
- A job that declares more memory than `--memory` is refused before anything runs, and `run` exits 2 naming it, as `[3] align needs 64G of memory where the run has 32G`.
- Ready jobs start in rank order and no other. While the job at the head waits for cores or memory, the jobs behind it wait too, even ones that would fit: the head is on the longest chain of work, and letting smaller jobs past it could delay the run or keep a big job waiting for good. Some cores may sit idle while a big job waits for enough to free.
- A job that needs more cores than `-j` takes all of the run's cores and memory: it starts once no other job is running, and runs alone. `run` says so as it starts, as `[3] needs 16 cores where the run has 8; it will run alone, once no other job is running`.
- `--no-enforce-memory` turns both memory rules off: a job may hold more than it declares, and one that declares more than `--memory` runs alone, as a job needing more cores does.
- A job planned `check` runs only its checks, but waits for the job's cores and memory as a run of it does.

## When a job fails

A command writes its outputs where they belong, so one that fails can leave a truncated file that another tool, a person or `spit discover` would take for a finished one. Once a job's first command has been started, any failure removes everything at the job's declared outputs, files and folders alike: a non-zero exit, a command that could not start, a missing output, an input that changed while it ran, a failed output check, or a run that was stopped. An output that passed the command but failed a check is complete but rejected, and is removed too. The runner removes them only after the failed command's process group has stopped, so nothing is still writing, and the job's log ends with a line for each output it removed:

```text
spit-bash: removed failed output work/upper.txt
```

Before its first command starts, a job moves what an earlier run left at its outputs aside, to a name such as `work/.upper.txt.spit-bash-previous`, so that it starts from the state a first run does and a tool that refuses to write over a file, such as `gdalwarp`, works on a rerun. When the job succeeds, the earlier version is deleted. When it fails, the failed output is removed as above and the earlier version is put back, with a line in the log: `spit-bash: restored the earlier work/upper.txt`. With `--keep-failed`, the failed output stays and the earlier version is deleted.

A job that fails before its first command starts, at a missing input, an input check or a `verify` command, keeps what is at its outputs: no command touched them, so they are still an earlier run's, whole. The job's record is still dropped, so the next run runs it again. To debug a tool, `run --keep-failed` leaves what a failed job's commands wrote where it is, and the log says `spit-bash: kept failed output work/upper.txt` instead. An output the command writes on its standard output stays in its temporary file, such as `work/.upper.txt.spit-bash-partial`, and is never renamed onto the output. The job's record is dropped all the same, so a kept output is never taken for a finished one: the next run runs the job again. The log also holds what the command printed, other than an output it writes on its standard output, and each command it ran, after `$ `, to run again by hand.

## Output and logs

Each job's output, from its checks, its `verify` commands and its commands, goes to `ROOT/.spit-bash/logs/ID-OPERATION.log`, except a command's standard output when it is [one of the job's outputs](#standard-input-and-output), which each run of the job replaces. The terminal shows only when each job starts and finishes, so parallel jobs do not mix their output. When a job fails, `run` prints the last lines of its log. `--logs DIR` puts the logs elsewhere.

`--timestamp-logs` starts every line of a job's log with the date, time and UTC offset at which the runner received it, to the millisecond, such as `2026-11-07T15:12:03.142+13:00 $ bet ...`. The runner's own `$` command lines, its notes, and every line of a command's output are stamped; a command's output written to a declared artifact is not touched. A tool that buffers its output may have written a line earlier than its stamp, and lines that arrive in one read share a time. A last line with no newline is ended with one. Without the flag the logs are as before.

```sh
spit-bash run pipeline.spit --timestamp-logs
```

`run` prints its plan under `Plan`, then a blank line and `Running`, with how many jobs will run and how long it expects to take, then a line as each job starts and finishes, how long it took and how many jobs are left. A job is named by its operation and by what it works on: the entities of its first output, such as `sub=02 task=rest run=01`, in name order, or the group a group job makes. Where those leave two jobs of one operation looking alike, as for jobs with no entities, the product is shown too, and a long value is cut short with `…`. The job id stays the reference to its log and to a failure.

```text
Plan
  12 run
  [ 1] run     anat sub=01: output missing
  [ 4] run     motion run=01 sub=01 task=rest: output missing
  ...

Running 12 jobs, about 4s
  [ 1] start   anat sub=01
  [ 4] start   motion run=01 sub=01 task=rest
  [ 1] done    anat sub=01 in 2s; 11 left, about 2s
```

Lines come in the order the scheduler starts and finishes jobs, not by id. `plan` prints the same job lines without the headings, and `plan --json` is unchanged.

A job is expected to take as long as its last successful run did, which the state records. A job that has not run before is expected to take the mean time of its operation's other jobs, from earlier runs and from this one. Until every job left has an estimate, `run` gives the count alone. A `maybe` job counts as one that runs until it is skipped, so the estimate is the longest the run can take. The estimate is the longer of two times the run cannot beat: the work left, each job's time multiplied by its cores, divided among the `-j` cores, and the longest chain of jobs left that each wait for the one before, such as a group template made after every subject and then a statistic made from it, which more cores do not shorten. A running job counts for the time it has left. The longest chain is worked out again at most every 5 seconds, as times come in. Neither allows for how the jobs will actually fit together on the cores, nor for jobs waiting for memory, so the run may take longer than either.

## Stopping at an unchanged output

A job that reads the output of a job that will run used to run too. Now it is `maybe` when its own record is current: the same fingerprint, files and checks as the last successful run. `plan` says how many jobs will run and how many more may. `run` waits for the jobs a `maybe` job reads from, then compares the files it now reads with its record, by size and time and, where those differ, by content. If the producer wrote the same bytes, the job is skipped, its new stamps are stored, and its own dependents are settled the same way. If they differ, it runs.

A job with a reason of its own, such as a changed input, a missing output or a changed fingerprint, is `run`, not `maybe`. So is a job whose input does not exist yet, since there is nothing to compare. `--stat-only` turns the early stop off, as it has no hashes to compare: every job below a rerun is `run`, as before. A job with no command that reads a `maybe` job is blocked, as it is when it reads a `run` job. A `maybe` job whose program is missing is blocked, as a `run` job is, even if it would be skipped.

## Stopping a run

Ctrl-C (SIGINT) or SIGTERM, as a cluster scheduler sends at a time limit, stops every running job's process group and exits with 130 or 143. Each job's group gets SIGTERM, then SIGKILL once the command has exited or five seconds have passed, whichever is first, so a tool that ignores SIGTERM or hangs while cleaning up cannot keep the run from stopping. Each job runs in a session of its own, so a tool's own child processes are stopped with it. A stopped job has no successful record, so the next run starts it again, and what its command had written at its outputs is [removed](#when-a-job-fails).

## Existing paths and reruns

The runner records successful jobs in `ROOT/.spit-bash/state.jsonl`, keyed by the products and entities they make, not their paths, so a dataset keeps its state when it moves or its path rules change. The runner computes each job's fingerprint from its operation, its artifacts' products and entities, and its commands with every path replaced by the artifact it names; SPIT writes none. Stamps and hashes in a record are also kept by artifact identity, so files moved or copied to new paths are current when their size and time, or their content, match the record; files that are not at the new paths run the job, and when the pipeline's path rules change, a job whose files are not at its new paths runs again, as its output is missing. With `--move-files`, `run` instead moves the outputs the record says were made at other paths to the paths the pipeline now gives, and `plan --move-files` lists the moves as `would move` and judges the jobs as if they were made. A job is moved only if its inputs are as recorded, each output is still as the run left it (checked after the move too), no destination exists, and no job still uses the old path; otherwise it is reported as `not moved` and runs normally, and nothing is overwritten. If a move fails part way, the job's files are put back, and moving back is what changing the rule back does. A move is not rerun, so a tool that writes its own paths into its output keeps the old path in it: use `--move-files` only for pipelines whose tools do not. Files already at the new paths are reused with or without the flag, when they match the record. The state is version 6, and an earlier state is refused: delete it to start from fresh records. A job runs if an output is missing, its fingerprint changed, an input or output file changed, or a producer will run. It skips only when its recorded fingerprint still matches and its files are as recorded: the same size and modification time, or, where those differ, the same content. A `touch`, a `git checkout` or a copy that leaves the bytes alone does not rerun a job. `--force` reruns every command job, or only the jobs matched by `--only`, `--product` or `--stage` when a selection is given; see [Choosing jobs](#choosing-jobs).

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

The test fixture under `tests/fixtures/` is actual version 11 output from SPIT's `command_demo` example, written without a root. `tests/test_examples.py` runs the [examples](examples/README.md), which live in SPIT's repository under `examples/runnable` so that SPIT's own tests keep them in step with its syntax, with a real `spit`: set `SPIT` to the binary or put it on `PATH`, and `SPIT_REPOSITORY` to the checkout it was built from (a sibling `../spit` by default), or the tests are skipped. CI builds `spit` from SPIT's `dev` branch, where its work merges, for them, on every push and weekly, so a change to SPIT's DAG format fails here. The weekly run tests this repository's `dev`, not `main`, which holds the last release. A branch whose name SPIT also has, other than `main` and `dev`, builds SPIT's branch of that name instead, so a change made in both repositories is tested together before SPIT's half merges. Once SPIT's branch has merged into `dev`, CI builds `dev` again, so a branch left behind after its merge never stands in for `dev`.

