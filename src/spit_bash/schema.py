"""The version 4 to 8 SPIT DAG schema, before filesystem checks or execution."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field


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
    # Version 5 added `kind`; every artifact of a version 4 DAG is a file.
    kind: Literal["file", "folder"] = "file"


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
    """A file the pipeline was read from, with its git blob id (version 7)."""

    path: NonEmpty
    blob: NonEmpty


class CallAt(SchemaModel):
    file: Annotated[int, Field(ge=0)]
    line: Annotated[int, Field(ge=1)]


class DagCall(SchemaModel):
    """A call to an operation carried out by steps (version 7)."""

    operation: NonEmpty
    instance: NonEmpty
    parent: Annotated[int, Field(ge=0)] | None
    file: Annotated[int, Field(ge=0)]
    at: CallAt


class Origin(SchemaModel):
    call: Annotated[int, Field(ge=0)]
    line: Annotated[int, Field(ge=1)]


class SpitJob(SchemaModel):
    id: JobId
    operation: NonEmpty
    stage: list[str]
    fingerprint: Annotated[str, Field(pattern=r"^[0-9a-f]{16}$")]
    inputs: dict[NonEmpty, list[Artifact]]
    outputs: Annotated[dict[NonEmpty, Artifact], Field(min_length=1)]
    depends_on: list[JobId]
    dependents: list[JobId]
    command: Command | None
    verify: list[Command]
    # Version 6 added `checks`; a version 4 or 5 DAG has none.
    checks: list[Check] | None = None
    # Version 7 added `origin`, and version 8 added `props`.
    origin: Origin | None = None
    props: dict[NonEmpty, str] | None = None


class SpitDag(SchemaModel):
    version: Literal[4, 5, 6, 7, 8]
    generator: Generator
    root: str | None
    external_inputs: list[Artifact]
    targets: list[Artifact]
    executables: list[str]
    removed: list[Removal]
    left_out: list[LeftOut]
    # Version 7 added these two.
    pipeline_files: list[PipelineFile] | None = None
    calls: list[DagCall] | None = None
    jobs: list[SpitJob]
