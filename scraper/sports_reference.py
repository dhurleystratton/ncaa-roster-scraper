"""Stub module for offline tests."""
from pathlib import Path
import pandas as pd


def fetch_rosters(seasons=None) -> None:
    """Create a tiny master_raw.csv for tests."""
    data = {
        "season": [2019],
        "sport": ["men"],
        "school_slug": ["uconn"],
        "school_name": ["UConn"],
        "conference": ["Big East"],
    }
    df = pd.DataFrame(data)
    out = Path("data/master_raw.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
