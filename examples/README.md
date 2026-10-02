# Runnable examples

Install `spit-bash` from the repository root with `python3 -m pip install .`. You also need the `spit` executable from [SPIT](https://github.com/eclnz/spit) on your `PATH`, or named by the `SPIT` variable. With sibling checkouts, `export SPIT=../spit/target/release/spit` is enough.

Run these commands from the `spit-bash` repository root. Each recipe uses its own folder as the dataset root. Generated DAGs, output files, and runner state are ignored by Git.

## Sort and merge

Three input files produce three sort jobs and two merge jobs. The sort jobs can run together; each merge waits for its group's sorted files.

```sh
spit-bash plan examples/lines/lines.spitin
spit-bash run examples/lines/lines.spitin -j 3
cat examples/lines/output/merged/group=alpha.txt
cat examples/lines/output/merged/group=beta.txt
```

The alpha result is `apple`, `banana`, `pear` on separate lines. The beta result is `yak`, `zebra`.

To run one group first, choose its jobs. This runs the beta sort and merge only:

```sh
spit-bash run examples/lines/lines.spitin --only group=beta
```

Run the plan command again to see every job marked `skip`. Edit one of the files under `examples/lines/input/` and plan again to see which jobs will rerun.

After a run, SPIT may report generated files that match no source path rule. Those files are not used as inputs.

## Verify before copying

This job checks that its input is nonempty before copying it. Here the DAG is saved first, so you can inspect the JSON or reuse it:

```sh
spit dag examples/verify/checked_copy.spitin -o examples/verify/checked_copy.spitdag
spit-bash plan examples/verify/checked_copy.spitdag
spit-bash run examples/verify/checked_copy.spitdag
cat examples/verify/output/copied/part=one.txt
```

The result is `Verified input.`. If you empty `examples/verify/input/one.txt`, the job's `verify` command fails and the copy does not run. `run` prints the end of the job's log, `examples/verify/.spit-bash/logs/1-copy.log`, which shows the failing `test -s input/one.txt`. Restore the input before planning another run.

## Folders in and out

Each trip's notes are a folder, and each job copies a folder whole, then packs the copy into an archive. The source and the copy are declared with a `/` after their types, so SPIT finds `input/alpha` and `input/beta` as folders and the runner treats each copy as one output:

```sh
spit-bash run examples/folders/albums.spitin -j 2
tar -tf examples/folders/output/archive/trip=alpha.tar
```

The alpha archive holds `./day1/morning.txt` and `./evening.txt`. `cp -R` makes each copy itself. Before it runs, the runner removes any copy an earlier run left, so the command never copies into an existing folder. Edit a file anywhere under `examples/folders/input/alpha/` and plan again: the alpha copy and its archive rerun, because a folder counts as changed when any file under it does.

`tests/test_examples.py` runs these examples, and checks the results above.
