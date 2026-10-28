# Runnable examples

Install `spit-bash` from the repository root with `python3 -m pip install .`. You also need the `spit` executable from [SPIT](https://github.com/eclnz/spit) on your `PATH`, or named by the `SPIT` variable. With sibling checkouts, `export SPIT=../spit/target/release/spit` is enough.

The recipes and their input files are in SPIT's repository, in `examples/runnable`, where SPIT's tests check them against the language. With sibling checkouts, set `EXAMPLES=../spit/examples/runnable`; the commands below use it. Each recipe uses its own folder as the dataset root. Generated DAGs, output files, and runner state go in those folders, which SPIT's `.gitignore` leaves out.

## Sort and merge

Three input files produce three sort jobs and two merge jobs. The sort jobs can run together; each merge waits for its group's sorted files.

```sh
spit-bash plan $EXAMPLES/lines/lines.spitin
spit-bash run $EXAMPLES/lines/lines.spitin -j 3
cat $EXAMPLES/lines/output/merged/group=alpha.txt
cat $EXAMPLES/lines/output/merged/group=beta.txt
```

The alpha result is `apple`, `banana`, `pear` on separate lines. The beta result is `yak`, `zebra`.

To run one group first, choose its jobs. This runs the beta sort and merge only:

```sh
spit-bash run $EXAMPLES/lines/lines.spitin --only group=beta
```

Run the plan command again to see every job marked `skip`. Edit one of the files under `$EXAMPLES/lines/input/` and plan again to see which jobs will rerun.

After a run, SPIT may report generated files that match no source path rule. Those files are not used as inputs.

## Operations carried out by steps

`steps.spit` declares `tidy` with a body of two `sort_lines` steps, then calls it once. The call becomes two ordinary jobs, so the runner plans and runs them like any others; the DAG's `calls` and each job's `origin` say they came from `tidy`.

```sh
spit-bash run $EXAMPLES/steps/steps.spitin
cat $EXAMPLES/steps/output/tidied/name=fruit.txt
```

The result is `apple`, `pear`.

## Verify before copying

This job checks that its input is nonempty before copying it. Here the DAG is saved first, so you can inspect the JSON or reuse it:

```sh
spit dag $EXAMPLES/verify/checked_copy.spitin -o $EXAMPLES/verify/checked_copy.spitdag
spit-bash plan $EXAMPLES/verify/checked_copy.spitdag
spit-bash run $EXAMPLES/verify/checked_copy.spitdag
cat $EXAMPLES/verify/output/copied/part=one.txt
```

The result is `Verified input.`. If you empty `$EXAMPLES/verify/input/one.txt`, the job's `verify` command fails and the copy does not run. `run` prints the end of the job's log, `$EXAMPLES/verify/.spit-bash/logs/1-copy.log`, which shows the failing `test -s input/one.txt`. Restore the input before planning another run.

## Checks before and after

`notes.spit` declares two checks once and attaches them: each note must not be empty before it is read, and each copy must contain `today` after it is written. SPIT writes each job's checks into the DAG, and `spit dag --commands` shows them as `check:` lines around the command:

```sh
spit dag $EXAMPLES/checks/notes.spitin --commands
spit-bash run $EXAMPLES/checks/notes.spitin
```

Both copies pass. Write `Rest.` into `$EXAMPLES/checks/input/tuesday.txt` and run again: `cp` succeeds, but the copy fails `contains(today)`, so the job fails and its log shows the failing `grep`. Then change `contains(today)` to `contains(for)` in `notes.spit` and plan: Monday's job is marked `check`, since only its check changed, and `run` checks its existing copy without copying again; it passes. Tuesday's job, which failed, runs again and fails again, since `Rest.` holds no `for`. Restore the inputs and `notes.spit` before planning another run.

## Standard input and output

`streams.spit` runs `grep -v '^#' < {list} > {@output}` on each plot's list, then `sort {items} > {@output}` on the results. The runner feeds each list to `grep` on its standard input, and writes each command's standard output to its output file, which appears only once the command succeeds:

```sh
spit-bash run $EXAMPLES/streams/streams.spitin
cat $EXAMPLES/streams/output/merged.txt
```

The result is `apples`, `carrots`, `leeks`, `pears`, `plums`. The log of each `uncomment` job, such as `$EXAMPLES/streams/.spit-bash/logs/1-uncomment.log`, shows its command with the redirections; the lines `grep` keeps go to the output, not the log. Write only comment lines into `$EXAMPLES/streams/input/garden.txt` and run again: `grep` keeps nothing and exits with status 1, so the job fails and leaves no `plot=garden.txt`, not even an empty one. Restore the input before planning another run.

## Folders in and out

Each trip's notes are a folder, and each job copies a folder whole, then packs the copy into an archive. The source and the copy are declared with a `/` after their types, so SPIT finds `input/alpha` and `input/beta` as folders and the runner treats each copy as one output:

```sh
spit-bash run $EXAMPLES/folders/albums.spitin -j 2
tar -tf $EXAMPLES/folders/output/archive/trip=alpha.tar
```

The alpha archive holds `./day1/morning.txt` and `./evening.txt`. `cp -R` makes each copy itself. Before it runs, the runner removes any copy an earlier run left, so the command never copies into an existing folder. Edit a file anywhere under `$EXAMPLES/folders/input/alpha/` and plan again: the alpha copy and its archive rerun, because a folder counts as changed when any file under it does.

`tests/test_examples.py` runs these examples, and checks the results above.
