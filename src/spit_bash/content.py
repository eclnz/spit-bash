"""Hash what a file or folder holds, to tell a changed file from one that was only touched."""

from __future__ import annotations

import hashlib
import os
from typing import Any

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
    names no file, as in `plan.stamp`.
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
