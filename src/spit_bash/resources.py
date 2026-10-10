"""What a job asks of the machine, read from its SPIT `props`, and the pool that grants it.

SPIT does not say what a prop means. This runner reads two: `cpus`, a whole
number of processors, and `mem`, memory as a number with an optional `K`,
`M`, `G` or `T` suffix (`8G`; no suffix means bytes). A job without them asks
for one processor and no memory. Other props are ignored.
"""

from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager
from typing import AsyncIterator

UNITS = {"": 1, "K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40}
SIZE = re.compile(r"^([0-9]+)([KMGT]?)(?:i?B)?$", re.IGNORECASE)


def parse_cpus(text: str) -> int:
    if not text.isascii() or not text.isdigit() or int(text) < 1:
        raise ValueError(f"`cpus` must be a whole number of at least 1, not {text!r}")
    return int(text)


def parse_size(text: str, name: str = "mem") -> int:
    match = SIZE.match(text.strip())
    if match is None or int(match.group(1)) < 1:
        raise ValueError(f"`{name}` must be a size such as 512M or 8G, not {text!r}")
    return int(match.group(1)) * UNITS[match.group(2).upper()]


class Pool:
    """Processors, and optionally memory, that running jobs hold between them.

    A job that asks for more than the whole pool runs alone with all of it,
    rather than never running.
    """

    def __init__(self, cpus: int, memory: int | None = None) -> None:
        self.cpus = cpus
        self.memory = memory
        self._free_cpus = cpus
        self._free_memory = memory
        self._changed = asyncio.Condition()

    def _want(self, cpus: int, memory: int | None) -> tuple[int, int]:
        return min(cpus, self.cpus), 0 if self.memory is None or memory is None else min(memory, self.memory)

    @asynccontextmanager
    async def hold(self, cpus: int, memory: int | None) -> AsyncIterator[None]:
        """Wait until the job's share is free, take it, and give it back on leaving."""
        want_cpus, want_memory = self._want(cpus, memory)
        async with self._changed:
            await self._changed.wait_for(
                lambda: self._free_cpus >= want_cpus and (self._free_memory is None or self._free_memory >= want_memory)
            )
            self._free_cpus -= want_cpus
            if self._free_memory is not None:
                self._free_memory -= want_memory
        try:
            yield
        finally:
            async with self._changed:
                self._free_cpus += want_cpus
                if self._free_memory is not None:
                    self._free_memory += want_memory
                self._changed.notify_all()
