"""What is on disk: each file's or folder's stamp, its folder entries, and the hash of what it holds."""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path
from typing import Any, NamedTuple

from .dag import Dag, Job

try:
    import xxhash
except ImportError:  # no wheel for this platform
    NAME = "blake2b"

    def _new() -> Any:
        return hashlib.blake2b(digest_size=16)
else:
    NAME = "xxh3_128"
    _new = xxhash.xxh3_128

CHUNK = 1 << 20

_FILE_FIELDS = ("size", "mtime_ns")
_FOLDER_FIELDS = ("total size", "latest mtime_ns", "file count")


class Stamp(NamedTuple):
    """A file's size and modification time, or a folder's, with how many files it holds.

    A folder's size is the total of the files under it and its time the latest
    of theirs; `count` is None for a file. Either can be compared by time.
    """

    size: int
    mtime_ns: int
    count: int | None = None

    @classmethod
    def from_stored(cls, value: list[int]) -> Stamp:
        """The stamp a record holds, in the form `stored` gives."""
        return cls(*value)

    def describe(self) -> str:
        if self.count is None:
            return f"{self.size} bytes, mtime_ns {self.mtime_ns}"
        return f"{self.size} bytes in {self.count} files, latest mtime_ns {self.mtime_ns}"

    def changes(self, now: Stamp) -> list[str]:
        """Each field that differs in `now`, as `size 10 -> 12`."""
        fields = _FILE_FIELDS if self.count is None else _FOLDER_FIELDS
        return [f"{field} {old} -> {new}" for field, old, new in zip(fields, self, now) if old != new]


Stamps = dict[str, Stamp | None]


def stored(stamp: Stamp | None) -> list[int] | None:
    """A stamp as the state file holds it: `[size, mtime_ns]`, and the count after them for a folder.

    A live stamp is compared with a record's in this form, so a file's stamp
    never equals a folder's.
    """
    if stamp is None:
        return None
    return [stamp.size, stamp.mtime_ns] if stamp.count is None else [stamp.size, stamp.mtime_ns, stamp.count]


def stored_each(by_key: dict[str, Stamp | None]) -> dict[str, list[int] | None]:
    """Every stamp of `by_key` as `stored` gives it."""
    return {key: stored(value) for key, value in by_key.items()}


def stamp(path: Path, folder: bool = False) -> Stamp | None:
    """A file's stamp, or None when it is not a file; with `folder`, a folder's, or None when it is not one.

    Directory mtimes are left out of a folder's stamp: syncing software can
    touch them without changing the files under them.
    """
    if folder:
        return _folder_stamp(path)
    try:
        info = path.stat()
    except FileNotFoundError:
        return None
    return Stamp(info.st_size, info.st_mtime_ns) if stat.S_ISREG(info.st_mode) else None


def _folder_stamp(path: Path) -> Stamp | None:
    if not path.is_dir():
        return None
    size, latest, count = 0, 0, 0
    for directory, folders, files in os.walk(path):
        for name in files:
            try:
                info = os.stat(os.path.join(directory, name))
            except FileNotFoundError:
                # A link to nothing names no file.
                continue
            size += info.st_size
            latest = max(latest, info.st_mtime_ns)
            count += 1
    return Stamp(size, latest, count)


def stamps(dag: Dag, paths: tuple[str, ...], seen: Stamps | None = None) -> Stamps:
    """Each path's stamp; with `seen`, each path is read once and kept there.

    Planning reads a file as one job's output and again as each consumer's
    input, and nothing changes the files meanwhile, so it keeps every stamp it
    reads for the whole plan. A run reads them afresh: its jobs change them.
    """
    if seen is None:
        return {path: stamp(dag.path(path), path in dag.folders) for path in paths}
    found = {}
    for path in paths:
        if path not in seen:
            seen[path] = stamp(dag.path(path), path in dag.folders)
        found[path] = seen[path]
    return found


FolderEntries = tuple[tuple[str, int, int], ...]


def folder_entries(root: Path) -> FolderEntries:
    """Each folder's name and each file's name, size and mtime under `root`, in a fixed order.

    Directory mtimes are left out, as in `stamp`. Two listings differ when a
    file is added, removed or renamed, even where the folder's stamp does not.
    """
    entries = []
    for directory, folders, files in os.walk(root):
        folders.sort()
        for name in folders:
            entries.append((f"d:{(Path(directory) / name).relative_to(root)}", 0, 0))
        for name in sorted(files):
            path = Path(directory) / name
            try:
                info = path.stat()
            except FileNotFoundError:
                continue
            if path.is_file():
                entries.append((f"f:{path.relative_to(root)}", info.st_size, info.st_mtime_ns))
    return tuple(entries)


def folder_inputs(dag: Dag, job: Job) -> dict[str, FolderEntries]:
    """The entries of each of the job's inputs that is a folder, by path."""
    return {path: folder_entries(dag.path(path)) for path in job.inputs if path in dag.folders}


def _file_digest(path: str) -> bytes:
    digest = _new()
    with open(path, "rb") as stream:
        while chunk := stream.read(CHUNK):
            digest.update(chunk)
    return digest.digest()


def content_hash(path: str, folder: bool = False) -> str | None:
    """The hash of a file's bytes, or of a folder's entries and the bytes of its files.

    The text names the algorithm, so a record made with another is never equal
    to this one. None when there is no such file or folder. A link to nothing
    names no file, as in `stamp`.
    """
    try:
        if not folder:
            return f"{NAME}:{_file_digest(path).hex()}" if os.path.isfile(path) else None
        if not os.path.isdir(path):
            return None
        digest = _new()
        for directory, folders, files in os.walk(path):
            folders.sort()
            for name in folders:
                digest.update(b"d" + os.path.relpath(os.path.join(directory, name), path).encode() + b"\0")
            for name in sorted(files):
                full = os.path.join(directory, name)
                if not os.path.isfile(full):
                    continue
                digest.update(b"f" + os.path.relpath(full, path).encode() + b"\0" + _file_digest(full))
        return f"{NAME}:{digest.hexdigest()}"
    except FileNotFoundError:
        return None


def hashes(dag: Dag, paths: tuple[str, ...]) -> dict[str, str]:
    """Each path's `content_hash`, leaving out a path with nothing there."""
    return {path: h for path in paths if (h := content_hash(str(dag.path(path)), path in dag.folders)) is not None}
