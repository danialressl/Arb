import sys

from arbscan.main import main


def _require_python() -> None:
    if sys.version_info < (3, 11):
        raise SystemExit("arbscan requires Python 3.11+. Please upgrade your Python runtime.")


if __name__ == "__main__":
    _require_python()
    raise SystemExit(main())
