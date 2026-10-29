"""The version 8 SPIT DAG schema, before filesystem checks or execution."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

# The one version of the format spit-bash reads.
VERSION = 8


class SchemaModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


NonEmpty = Annotated[str, Field(min_length=1)]
JobId = Annotated[int, Field(ge=1)]


def relative_path(value: str) -> str:
    path = PurePosixPath(value)
    if not value or path.is_absolute() or any(part in (".", "..") for part in value.split("/")):
        raise ValueError(f"path must stay within the dataset root: {value}")
    return value


RelativePath = Annotated[str, AfterValidator(relative_path)]


class VariableType(SchemaModel):
    variable: NonEmpty


class NamedType(SchemaModel):
    name: NonEmpty
    args: list[NamedType | VariableType | None]


class Artifact(SchemaModel):
    product: NonEmpty
    entities: dict[NonEmpty, str]
    artifact_type: NamedType | VariableType | None = Field(alias="type")
    path: RelativePath
    kind: Literal["file", "folder"]


class PathPart(SchemaModel):
    path: RelativePath


class DirPart(SchemaModel):
    dir: str
    of_path: RelativePath = Field(alias="of")


class StemPart(SchemaModel):
    stem: str
    of_path: RelativePath = Field(alias="of")


CommandPart = str | PathPart | DirPart | StemPart
Argument = Annotated[list[CommandPart], Field(min_length=1)]
Command = Annotated[list[Argument], Field(min_length=1)]


class Generator(SchemaModel):
    name: NonEmpty
    version: NonEmpty


class Removal(SchemaModel):
    product: str | None
    entities: dict[str, str]
    rule: str
    origin: str | None
    reason: str | None
    found: Annotated[int, Field(ge=0)] | None


class LeftOut(SchemaModel):
    identity: NonEmpty
    reasons: list[str]


class Check(SchemaModel):
    """A check of one artifact, before the command on an input or after it on an output."""

    when: Literal["before", "after"]
    check: NonEmpty
    port: NonEmpty
    path: RelativePath
    command: Command


class PipelineFile(SchemaModel):
    """A file the pipeline was read from, by its git blob id."""

    path: NonEmpty
    blob: Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]


Index = Annotated[int, Field(ge=0)]
Line = Annotated[int, Field(ge=1)]


class CallSite(SchemaModel):
    file: Index | None
    line: Line


class Call(SchemaModel):
    """A call to an operation carried out by steps, which the DAG holds as its jobs."""

    operation: NonEmpty
    instance: NonEmpty
    parent: Index | None
    file: Index | None
    at: CallSite


class Origin(SchemaModel):
    """The call whose body made a job, and the line of the body's step."""

    call: Index
    line: Line


class JobCommand(SchemaModel):
    """One of a job's commands, which run in order, and the files its streams are connected to."""

    command: Command
    # The input file the command reads on its standard input, and the output
    # file its standard output is written to; null when it does not redirect.
    stdin: RelativePath | None
    stdout: RelativePath | None


class SpitJob(SchemaModel):
    id: JobId
    operation: NonEmpty
    stage: list[str]
    fingerprint: Annotated[str, Field(pattern=r"^[0-9a-f]{16}$")]
    inputs: dict[NonEmpty, list[Artifact]]
    outputs: Annotated[dict[NonEmpty, Artifact], Field(min_length=1)]
    depends_on: list[JobId]
    dependents: list[JobId]
    # Empty for an operation with no command.
    commands: list[JobCommand]
    verify: list[Command]
    checks: list[Check]
    # Null for a step written in the pipeline.
    origin: Origin | None
    # What the job needs while its commands run: cores, 1 when its operation
    # declares none, and memory in bytes, null when it claims none.
    cores: Annotated[int, Field(ge=1)]
    memory: Annotated[int, Field(ge=0, le=2**53)] | None


class SpitDag(SchemaModel):
    version: int
    generator: Generator
    root: str | None
    external_inputs: list[Artifact]
    targets: list[Artifact]
    executables: list[str]
    removed: list[Removal]
    left_out: list[LeftOut]
    pipeline_files: list[PipelineFile]
    calls: list[Call]
    jobs: list[SpitJob]

    @model_validator(mode="before")
    @classmethod
    def _version(cls, data: object) -> object:
        """Name the version first, since a DAG of another one fails on fields it lacks or adds."""
        version = data.get("version") if isinstance(data, dict) else None
        if version != VERSION:
            raise ValueError(f"spit-bash reads version {VERSION} of the DAG format, not {version}; "
                             f"use a spit and spit-bash that match")
        return data
