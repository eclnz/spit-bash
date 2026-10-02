# Runnable examples

Install `spit-bash` from the repository root with `python3 -m pip install .`. You also need the `spit` executable from [SPIT](https://github.com/eclnz/spit) on your `PATH`. With sibling checkouts, `../spit/target/release/spit` can replace `spit` in the commands below.

Run these commands from the `spit-bash` repository root. Each recipe uses its own folder as the dataset root. Generated DAGs, output files, and runner state are ignored by Git.

## Sort and merge

Three input files produce three sort jobs and two merge jobs. The sort jobs can run together; each merge waits for its group's sorted files.

```sh
spit dag examples/lines/lines.spitin --json | spit-bash plan -
spit dag examples/lines/lines.spitin --json | spit-bash run - -j 3
cat examples/lines/output/merged/group=alpha.txt
cat examples/lines/output/merged/group=beta.txt
```

The alpha result is `apple`, `banana`, `pear` on separate lines. The beta result is `yak`, `zebra`.

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

`tests/test_examples.py` runs both examples, and checks the results above.
