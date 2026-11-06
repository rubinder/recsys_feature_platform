"""Register Feast definitions (equivalent of `feast apply`)."""

from recsys.feature_repo.store import apply

if __name__ == "__main__":
    apply()
    print("feast definitions applied")
