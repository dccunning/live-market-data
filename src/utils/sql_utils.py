from pathlib import Path


def load_sql(path: Path) -> str:
    with open(path, "r") as f:
        return f.read().strip()
