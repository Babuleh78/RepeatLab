"""Open a fixed dataset member while allowing aliases for the selected dataset directory."""

import os
import stat
from io import BufferedReader
from pathlib import Path


def open_artifact(path: Path) -> BufferedReader:
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode):
        raise ValueError("expected a regular artifact file")

    def open_regular(name: str, flags: int) -> int:
        # Where supported, avoid following a replacement link or blocking on a replacement FIFO.
        flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        fd = os.open(name, flags)
        try:
            actual = os.fstat(fd)
            if not stat.S_ISREG(actual.st_mode) or not os.path.samestat(before, actual):
                raise ValueError("artifact changed while opening")
        except BaseException:
            os.close(fd)
            raise
        return fd

    return open(path, "rb", opener=open_regular)
