"""Read and validate the execution fields of a SPIT DAG."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any


class DagError(ValueError):
    """The DAG cannot be run as supplied."""


@dataclass(frozen=True)
class Job:
    id: int
    operation: str
    fingerprint: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    depends_on: tuple[int, ...]
    command: tuple[str, ...] | None
    verify: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class Dag:
    root: Path
    jobs: tuple[Job, ...]
    external_inputs: tuple[str, ...]
    left_out: tuple[str, ...]

    def path(self, relative: str) -> Path:
        path = self.root.joinpath(*PurePosixPath(relative).parts)
        if not path.resolve().is_relative_to(self.root):
            raise DagError(f"path escapes dataset root: {relative}")
        return path


def _relative(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise DagError(f"{label} must be a nonempty relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in (".", "..") for part in value.split("/")):
        raise DagError(f"{label} must stay within the dataset root: {value}")
    return value


def _artifact_path(value: Any, label: str) -> str:
    if not isinstance(value, dict):
        raise DagError(f"{label} must be an artifact")
    return _relative(value.get("path"), f"{label}.path")


def _command(value: Any, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise DagError(f"{label} must be a nonempty argument list")
    arguments = []
    for argument in value:
        if not isinstance(argument, list) or not argument:
            raise DagError(f"{label} contains an empty argument")
        parts = []
        for part in argument:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict) and set(part) == {"path"}:
                parts.append(_relative(part["path"], f"{label} path"))
            elif isinstance(part, dict) and set(part) in ({"dir", "of"}, {"stem", "of"}):
                _relative(part["of"], f"{label} source path")
                key = "dir" if "dir" in part else "stem"
                if not isinstance(part[key], str):
                    raise DagError(f"{label} {key} must be text")
                parts.append(part[key])
            else:
                raise DagError(f"{label} contains an unknown argument part: {part!r}")
        arguments.append("".join(parts))
    if not arguments[0]:
        raise DagError(f"{label} has no executable")
    return tuple(arguments)


def load_dag(source: Any, root_override: str | None = None) -> Dag:
    try:
        data = json.load(source)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise DagError(f"invalid DAG JSON: {exc}") from exc
    if not isinstance(data, dict) or data.get("version") != 4:
        raise DagError("expected a SPIT .spitdag with version 4")
    root_value = root_override if root_override is not None else data.get("root")
    if not isinstance(root_value, str) or not root_value:
        raise DagError("DAG has no root; pass --root DIRECTORY")
    root = Path(root_value).expanduser().resolve()
    if not root.is_dir():
        raise DagError(f"dataset root is not a directory: {root}")

    external = data.get("external_inputs")
    jobs_data = data.get("jobs")
    left_out = data.get("left_out", [])
    if not isinstance(external, list) or not isinstance(jobs_data, list) or not isinstance(left_out, list):
        raise DagError("external_inputs, jobs and left_out must be arrays")
    external_paths = tuple(_artifact_path(item, "external input") for item in external)
    jobs = []
    seen_ids: set[int] = set()
    producers: dict[str, int] = {}
    for item in jobs_data:
        if not isinstance(item, dict):
            raise DagError("each job must be an object")
        job_id = item.get("id")
        if type(job_id) is not int or job_id < 1 or job_id in seen_ids:
            raise DagError(f"job id must be a unique positive integer: {job_id!r}")
        inputs = item.get("inputs")
        outputs = item.get("outputs")
        depends = item.get("depends_on")
        verify = item.get("verify")
        if not isinstance(inputs, dict) or not isinstance(outputs, dict) or not isinstance(depends, list) or not isinstance(verify, list):
            raise DagError(f"job {job_id} has invalid inputs, outputs, dependencies or verify")
        input_paths = []
        for port, artifacts in inputs.items():
            if not isinstance(artifacts, list):
                raise DagError(f"job {job_id} input {port} must be an array")
            input_paths.extend(_artifact_path(a, f"job {job_id} input {port}") for a in artifacts)
        output_paths = tuple(_artifact_path(a, f"job {job_id} output {port}") for port, a in outputs.items())
        if not output_paths:
            raise DagError(f"job {job_id} has no outputs")
        if any(type(dep) is not int or dep not in seen_ids for dep in depends) or len(depends) != len(set(depends)):
            raise DagError(f"job {job_id} dependencies must name earlier jobs once each")
        for path in output_paths:
            if path in producers or path in external_paths:
                raise DagError(f"multiple jobs or sources claim path: {path}")
            producers[path] = job_id
        command_data = item.get("command")
        command = None if command_data is None else _command(command_data, f"job {job_id} command")
        fingerprint = item.get("fingerprint")
        if not isinstance(fingerprint, str) or re.fullmatch(r"[0-9a-f]{16}", fingerprint) is None:
            raise DagError(f"job {job_id} has an invalid fingerprint")
        operation = item.get("operation")
        if not isinstance(operation, str) or not operation:
            raise DagError(f"job {job_id} has no operation")
        jobs.append(Job(
            id=job_id,
            operation=operation,
            fingerprint=fingerprint,
            inputs=tuple(input_paths),
            outputs=output_paths,
            depends_on=tuple(depends),
            command=command,
            verify=tuple(_command(v, f"job {job_id} verify") for v in verify),
        ))
        seen_ids.add(job_id)

    for job in jobs:
        expected = {producers[path] for path in job.inputs if path in producers}
        if set(job.depends_on) != expected:
            raise DagError(f"job {job.id} dependencies do not match its input producers")
        for path in job.inputs:
            if path not in producers and path not in external_paths:
                raise DagError(f"job {job.id} input is neither produced nor external: {path}")
    if any(not isinstance(item, dict) for item in left_out):
        raise DagError("left_out entries must be objects")
    dag = Dag(root, tuple(jobs), external_paths, tuple(str(item.get("identity", "?")) for item in left_out))
    for path in (*external_paths, *(path for job in jobs for path in (*job.inputs, *job.outputs))):
        dag.path(path)
    return dag
