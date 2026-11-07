"""Choose the Terraform backend for envs/main.

    auto    remote S3 backend if backend.hcl exists (after `make terraform-bootstrap`),
            otherwise a gitignored backend_override.tf forcing local state, so
            `terraform plan` works before the state bucket exists
    remote  drop the override (used right after bootstrap)
"""

from __future__ import annotations

import sys
from pathlib import Path

MAIN = Path(__file__).resolve().parents[1] / "infra" / "terraform" / "envs" / "main"
OVERRIDE = MAIN / "backend_override.tf"
LOCAL = 'terraform {\n  backend "local" {}\n}\n'


def main(mode: str) -> None:
    if mode == "remote" or (MAIN / "backend.hcl").exists():
        OVERRIDE.unlink(missing_ok=True)
        print("terraform backend: S3 (backend.hcl)")
    else:
        OVERRIDE.write_text(LOCAL)
        print("terraform backend: local state (not bootstrapped yet; run `make terraform-bootstrap` before applying)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "auto")
