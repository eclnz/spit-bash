"""How much memory a running command's process group holds."""

from __future__ import annotations

import os
import signal
import struct
import subprocess
from pathlib import Path

PROC = Path("/proc")


def group_memory(group: int, exclude: int | None = None) -> int:
    """The resident memory of every process in process group `group`, in bytes.

    Read from /proc where there is one, as on Linux, and from `ps` elsewhere,
    as on macOS. Pages that processes share, such as a forked worker's, are
    counted once for each process, so a group of forked workers can read
    higher than it holds.
    """
    if PROC.is_dir():
        return _from_proc(group, exclude)
    return _from_ps(group, exclude)


def _from_proc(group: int, exclude: int | None = None) -> int:
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
        if int(fields[2]) == group and int(entry.name) != exclude:
            total += int(fields[21]) * page
    return total


def _from_ps(group: int, exclude: int | None = None) -> int:
    listing = subprocess.run(["ps", "-A", "-o", "pgid=,pid=,rss="], capture_output=True, text=True, check=False).stdout
    total = 0
    for line in listing.splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[0] == str(group) and parts[1] != str(exclude):
            # `ps` gives resident memory in KiB.
            total += int(parts[2]) * 1024
    return total


def wait_peak(peak_fd: int, ack_fd: int) -> None:
    """Exec one command, report its peak RSS, then hold its process group until acknowledged."""
    import sys

    signal.signal(signal.SIGINT, signal.SIG_DFL)
    args = sys.argv[sys.argv.index("--") + 1:]
    child = os.fork()
    if child == 0:
        os.close(peak_fd)
        os.close(ack_fd)
        try:
            os.execvpe(args[0], args, os.environ)
        except OSError as exc:
            print(f"spit-bash: command could not start: {exc}", file=sys.stderr)
            os._exit(127)
    _, status, usage = os.wait4(child, 0)
    peak = usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024
    os.write(peak_fd, struct.pack("!Q", peak))
    os.close(peak_fd)
    if not os.read(ack_fd, 1):
        os.close(ack_fd)
        try:
            os.killpg(os.getpid(), signal.SIGTERM)
        except ProcessLookupError:
            pass
        raise SystemExit(1)
    os.close(ack_fd)
    if os.WIFEXITED(status):
        raise SystemExit(os.WEXITSTATUS(status))
    if os.WIFSIGNALED(status):
        os.kill(os.getpid(), os.WTERMSIG(status))
    raise SystemExit(1)


if __name__ == "__main__":
    import sys

    wait_peak(int(sys.argv[1]), int(sys.argv[2]))
