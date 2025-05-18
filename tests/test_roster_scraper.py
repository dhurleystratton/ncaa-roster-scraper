import os
import pandas as pd
from scraper.sports_reference import fetch_rosters

def test_fetch_rosters(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    fetch_rosters(seasons=[2019])
    csv = tmp_path / "data" / "master_raw.csv"
    assert csv.exists()
    df = pd.read_csv(csv)
    expected_cols = ["season", "sport", "school", "player", "position", "class"]
    for col in expected_cols:
        assert col in df.columns
