"""Log-capping filter: copies stdin to a file up to a byte cap, then drains.

Usage: python -m task_scheduler.logcap <path> <max_bytes>

Writes at most max_bytes to <path>, appends a truncation marker, then keeps
reading stdin to /dev/null so the producing process is never SIGPIPE'd.
"""
import sys

MARKER = b"\n[scheduler] log truncated at cap\n"


def main() -> None:
    path, cap = sys.argv[1], int(sys.argv[2])
    written = 0
    capped = False
    with open(path, "ab") as out:
        while True:
            chunk = sys.stdin.buffer.read(65536)
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
