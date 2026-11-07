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

`streams.spit` runs `tr a-z A-Z < {list} > {@output}` on each plot's list, then `sort {items} > {@output}` on the results. `tr` reads only its standard input, so the runner feeds each list to it there, and writes each command's standard output to its output file, which appears only once the command succeeds:

```sh
spit-bash run $EXAMPLES/streams/streams.spitin
cat $EXAMPLES/streams/output/merged.txt
```

The result is `APPLES`, `CARROTS`, `LEEKS`, `PEARS`, `PLUMS`. The log of each `shout` job, such as `$EXAMPLES/streams/.spit-bash/logs/1-shout.log`, shows its command with the redirections; the capitals `tr` prints go to the output, not the log. Empty `$EXAMPLES/streams/input/garden.txt` and run again: `tr` prints nothing, so its output fails its `nonempty` check, the job fails, and it leaves no `plot=garden.txt`, not even an empty one. Restore the input before planning another run.

## Several commands in one job

`sequence.spit` runs two tools, one after the other, in each `tally` job: `sort -o {sorted} {list}` writes the sorted list, then `uniq -c {sorted} > {counts}` reads it and prints how often each line comes:

```sh
spit dag $EXAMPLES/sequence/sequence.spitin --commands
spit-bash run $EXAMPLES/sequence/sequence.spitin
cat $EXAMPLES/sequence/output/counts/plot=garden.txt
```

`spit dag --commands` shows a `run:` line for each. The garden counts are `2 bean`, `3 carrot` and `1 pea`, each count padded with spaces, as `uniq -c` writes it. The log of each job, such as `$EXAMPLES/sequence/.spit-bash/logs/1-tally.log`, shows both commands after `$ `.

## Folders in and out

Each trip's notes are a folder, and each job copies a folder whole, then packs the copy into an archive. The source and the copy are declared with a `/` after their types, so SPIT finds `input/alpha` and `input/beta` as folders and the runner treats each copy as one output:

```sh
spit-bash run $EXAMPLES/folders/albums.spitin -j 2
tar -tf $EXAMPLES/folders/output/archive/trip=alpha.tar
```

The alpha archive holds `./day1/morning.txt` and `./evening.txt`. Before each copy runs, the runner removes any copy an earlier run left and makes the folder again, empty, and `cp -R {notes}/. {@output}` copies the trip's contents into it. Edit a file anywhere under `$EXAMPLES/folders/input/alpha/` and plan again: the alpha copy and its archive rerun, because a folder counts as changed when any file under it does.

`tests/test_examples.py` runs these examples, and checks the results above.
