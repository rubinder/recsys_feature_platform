"""Print exports for a self-refreshing AWS profile (credential_process -> the CLI's login/SSO session).

    eval "$(uv run python scripts/aws_profile_env.py)"

Exported credentials from `aws login` last ~15 minutes; EKS bring-up and
teardown take longer. The profile asks the CLI for fresh credentials on every
call instead.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
cfg = ROOT / "data" / "eks" / "aws-config"
cfg.parent.mkdir(parents=True, exist_ok=True)
cfg.write_text(
    "[profile recsys-session]\nregion = us-east-1\ncredential_process = env -u AWS_PROFILE "
    f"AWS_CONFIG_FILE={Path.home() / '.aws' / 'config'} aws configure export-credentials --format process\n"
)
print("unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_CREDENTIAL_EXPIRATION")
print(f"export AWS_CONFIG_FILE={cfg} AWS_PROFILE=recsys-session")
