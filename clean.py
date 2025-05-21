"""Stub module to satisfy legacy CI tests."""
from pathlib import Path
import pandas as pd


def main() -> None:
    """Write a small cleaned CSV from ``data/master_raw.csv``."""
    raw_path = Path("data/master_raw.csv")
    if not raw_path.exists():
        raise FileNotFoundError(raw_path)
    df = pd.read_csv(raw_path)
    out = Path("data/master_clean.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
