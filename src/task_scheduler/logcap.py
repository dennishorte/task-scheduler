"""Log-capping filter: copies stdin to a file up to a byte cap, then drains.

Usage: python -m task_scheduler.logcap <path> <max_bytes>

Writes at most max_bytes to <path>, appends a truncation marker, then keeps
reading stdin to /dev/null so the producing process is never SIGPIPE'd.
"""
import os
import sys

MARKER = b"\n[scheduler] log truncated at cap\n"


def main() -> None:
    path, cap = sys.argv[1], int(sys.argv[2])
    written = 0
    capped = False
    # os.read (not BufferedReader.read) returns as soon as any data is
    # available — .read(n) would block until n bytes or EOF, hiding running
    # tasks' output. Writes are unbuffered so each chunk lands on disk
    # immediately for the log endpoint to serve.
    fd_in = sys.stdin.fileno()
    with open(path, "ab", buffering=0) as out:
        while True:
            chunk = os.read(fd_in, 65536)
            if not chunk:
                break
            if not capped:
                room = cap - written
                if len(chunk) <= room:
                    out.write(chunk)
                    written += len(chunk)
                else:
                    out.write(chunk[:room])
                    out.write(MARKER)
                    capped = True
            # else: drain to /dev/null


if __name__ == "__main__":
    main()
