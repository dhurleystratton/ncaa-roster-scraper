import pandas as pd
from pathlib import Path


def main():
    """Clean ``data/master_raw.csv`` and output ``data/master_clean.csv``."""
    raw_path = Path("data/master_raw.csv")
    if not raw_path.exists():
        raise FileNotFoundError(f"{raw_path} not found")

    df = pd.read_csv(raw_path)

    if "school" in df.columns:
        df["school"] = df["school"].astype(str).str.strip().str.title()

    subset = [col for col in ["player", "school", "season", "sport", "conference"] if col in df.columns]
    if subset:
        df = df.drop_duplicates(subset=subset)

    out_path = Path("data/master_clean.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)


if __name__ == "__main__":
    main()
