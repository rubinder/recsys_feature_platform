"""`make quality`: run every data-quality and freshness check, exit non-zero on failure."""

from __future__ import annotations

import sys

from recsys.quality import checks, freshness


def main() -> None:
    outcomes = [o for fn in checks.ALL.values() for o in fn()] + [fn() for fn in freshness.ALL.values()]
    for o in outcomes:
        print(o.summary())
    failed = [o for o in outcomes if not o.passed and o.severity == "ERROR"]
    print(f"\n{len(outcomes) - len(failed)}/{len(outcomes)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
