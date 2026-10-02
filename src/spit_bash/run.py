"""Run independent ready jobs concurrently, without a shell."""

from __future__ import annotations

import asyncio
import shlex
from dataclasses import dataclass

from .dag import Dag, Job
from .plan import Decision, State, job_key, stamps


@dataclass(frozen=True)
class Result:
    job: Job
    error: str | None = None


async def _command(dag: Dag, job: Job, args: tuple[str, ...], kind: str) -> str | None:
    print(f"[{job.id}] {kind}: {shlex.join(args)}", flush=True)
    try:
        process = await asyncio.create_subprocess_exec(*args, cwd=dag.root)
        try:
            code = await process.wait()
        except asyncio.CancelledError:
            process.terminate()
            await process.wait()
            raise
    except OSError as exc:
        return f"{kind} could not start: {exc}"
    return None if code == 0 else f"{kind} exited with status {code}"


async def execute(dag: Dag, decisions: tuple[Decision, ...], state: State, workers: int) -> tuple[Result, ...]:
    semaphore = asyncio.Semaphore(workers)
    tasks: dict[int, asyncio.Task[Result]] = {}

    async def run_one(decision: Decision) -> Result:
        job = decision.job
        for dependency in job.depends_on:
            if dependency in tasks:
                result = await tasks[dependency]
                if result.error:
                    return Result(job, f"dependency {dependency} failed")
        async with semaphore:
            before = stamps(dag, job.inputs)
            if any(value is None for value in before.values()):
                return Result(job, "input disappeared before execution")
            print(f"[{job.id}] run {job.operation}", flush=True)
            state.jobs.pop(job_key(dag, job), None)
            state.save()
            for index, command in enumerate(job.verify, 1):
                error = await _command(dag, job, command, f"verify {index}")
                if error:
                    return Result(job, error)
            if job.command is None:
                return Result(job, "job has no command")
            for output in job.outputs:
                dag.path(output).parent.mkdir(parents=True, exist_ok=True)
            error = await _command(dag, job, job.command, "command")
            if error:
                return Result(job, error)
            after = stamps(dag, job.inputs)
            if after != before:
                return Result(job, "input changed during execution")
            outputs = stamps(dag, job.outputs)
            if any(value is None for value in outputs.values()):
                return Result(job, "command succeeded but did not create every output")
            state.jobs[job_key(dag, job)] = {
                "fingerprint": job.fingerprint,
                "inputs": after,
                "outputs": outputs,
            }
            state.save()
            print(f"[{job.id}] done", flush=True)
            return Result(job)

    for decision in decisions:
        if decision.status == "run":
            tasks[decision.job.id] = asyncio.create_task(run_one(decision))
    return tuple(await asyncio.gather(*tasks.values()))
