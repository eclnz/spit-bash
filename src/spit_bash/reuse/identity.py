"""What identifies a job's work and its record, whatever its files and things are called."""

from __future__ import annotations

import hashlib
import json

from ..model import Artifact, Check, Work
from ..schema import Command, CommandPart, DirPart, PathPart, SpitJob, StemPart

# The work's parts, each hashed on its own. Keep in step with the keys `_parts` gives.
PARTS = ("commands", "verify", "inputs", "outputs")


def slots(item: SpitJob) -> dict[str, str]:
    """Each input and output path's slot, as `Artifact.slot` gives it.

    An artifact is the same work whatever it is called, so a record names a file
    by where the job holds it, not by product or entities.
    """
    found: dict[str, str] = {}
    for port, artifacts in enumerate(item.inputs.values()):
        for place, artifact in enumerate(artifacts):
            found.setdefault(artifact.path, f"in{port}.{place}")
    for port, artifact in enumerate(item.outputs.values()):
        found.setdefault(artifact.path, f"out{port}")
    return found


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, separators=(",", ":"), sort_keys=True).encode()).hexdigest()[:16]


def _parts(item: SpitJob, slots: dict[str, str]) -> dict[str, object]:
    """The job's work as the runner identifies it: the shape of what it reads and
    writes, and its commands, with every path replaced by its place in the job.
    Names, storage paths, checks, scheduling and where the job came from are left
    out, so renaming or moving things changes nothing here. What the inputs hold
    is checked against the record's hashes, not here."""

    def of(path: str) -> str:
        return slots.get(path, path)

    def part(value: CommandPart) -> list | str:
        if isinstance(value, str):
            return value
        if isinstance(value, PathPart):
            return ["path", of(value.path)]
        if isinstance(value, DirPart):
            # The tool is given this text, so a change to it is a change to the work.
            return ["dir", value.dir, of(value.of_path)]
        if isinstance(value, StemPart):
            return ["stem", value.stem, of(value.of_path)]
        return ["here", value.here]

    def command(arguments: Command) -> list:
        return [[part(value) for value in argument] for argument in arguments]

    # Ports are listed in order, without their names: a renamed operation or port is the same work.
    return {
        "inputs": [[[slots[a.path], a.kind] for a in artifacts] for artifacts in item.inputs.values()],
        "outputs": [[slots[a.path], a.kind] for a in item.outputs.values()],
        "commands": [
            [command(c.command), None if c.stdin is None else of(c.stdin), None if c.stdout is None else of(c.stdout)]
            for c in item.commands
        ],
        "verify": [command(c) for c in item.verify],
    }


def checks_key(checks: tuple[Check, ...], slots: dict[str, str], shown: dict[str, str] | None = None) -> str | None:
    """What a successful run records of the checks it passed; None without checks.

    `shown` gives the text a path has in a command when it is not the path itself,
    as `load._command` writes it.
    """
    if not checks:
        return None
    shown = shown or {}
    # The tested artifact stands as its slot and its path in the command as a
    # placeholder, so moving the file does not change what was checked.
    text = json.dumps([[c.side.value, c.name, slots[c.path],
                        [a.replace(shown.get(c.path, c.path), "{@path}") for a in c.command]]
                       for c in checks], separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def key(outputs: tuple[Artifact, ...]) -> str:
    """The state file's key for a job that makes `outputs`."""
    # Job IDs may change when SPIT resolves a different inventory, and paths
    # change when the pipeline's path rules do. What a job makes is its outputs'
    # products and entities, which neither changes.
    made = sorted([output.product, output.entities] for output in outputs)
    return json.dumps(made, separators=(",", ":"))


def work(item: SpitJob, slots: dict[str, str], checks: tuple[Check, ...], outputs: tuple[Artifact, ...],
         shown: dict[str, str] | None = None) -> Work:
    """The job's `Work`, from its wire form, its slots, and its checks and outputs as loaded, with
    `shown` as `checks_key` takes it."""
    parts = _parts(item, slots)
    # The parts are hashed apart so that a record can say which of them changed.
    return Work(_digest(parts), {name: _digest(value) for name, value in parts.items()},
                checks_key(checks, slots, shown), key(outputs))
