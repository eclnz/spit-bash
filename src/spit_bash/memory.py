"""How much memory a running command's process group holds."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

PROC = Path("/proc")


def group_memory(group: int) -> int:
    """The resident memory of every process in process group `group`, in bytes.

    Read from /proc where there is one, as on Linux, and from `ps` elsewhere,
    as on macOS. Pages that processes share, such as a forked worker's, are
    counted once for each process, so a group of forked workers can read
    higher than it holds.
    """
    if PROC.is_dir():
        return _from_proc(group)
    return _from_ps(group)


def _from_proc(group: int) -> int:
    page = os.sysconf("SC_PAGE_SIZE")
    total = 0
    for entry in os.scandir(PROC):
        if not entry.name.isdigit():
            continue
        try:
            stat = Path(entry.path, "stat").read_text()
        except OSError:
            # The process exited between listing and reading.
            continue
        # The command name, in parentheses, may hold spaces; the fields after it
        # start at the state, the third field, so the group, the fifth, is at 2
        # and the resident pages, the twenty-fourth, at 21.
        fields = stat.rsplit(")", 1)[1].split()
        if int(fields[2]) == group:
            total += int(fields[21]) * page
    return total


def _from_ps(group: int) -> int:
    listing = subprocess.run(["ps", "-A", "-o", "pgid=,rss="], capture_output=True, text=True, check=False).stdout
    total = 0
    for line in listing.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == str(group):
            # `ps` gives resident memory in KiB.
            total += int(parts[1]) * 1024
    return total
