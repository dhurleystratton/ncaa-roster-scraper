"""
Offline smoke-test for ig_scraper.py

We monkey-patch network helpers so the script can run without
ScrapingBee / BrightData keys or external calls.
"""
from pathlib import Path
import importlib
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import types

import pandas as pd

# --- Arrange ---------------------------------------------------------------
MODULE = importlib.import_module("ig_scraper")


class DummyResp(types.SimpleNamespace):
    ok: bool = True
    status_code: int = 200
    text: str = '<meta property="og:type" content="profile">'
    url: str = "https://instagram.com/janedoe"


def fake_fetch_html(url: str):
    return DummyResp()


def fake_parse_google_results(_html: str):
    return ["https://instagram.com/janedoe"]


MODULE.fetch_html = fake_fetch_html
MODULE.parse_google_results = fake_parse_google_results

# --- Act -------------------------------------------------------------------
IN = Path(__file__).with_suffix(".csv")
OUT = Path(__file__).with_name("out.csv")
if OUT.exists():
    OUT.unlink()

MODULE.main(["--in", str(IN), "--out", str(OUT)])

# --- Assert ---------------------------------------------------------------
df = pd.read_csv(OUT)

def test_handle_found():
    assert df.loc[0, "instagram_handle"] == "janedoe"
    assert df.loc[0, "status"] == "FOUND_VALID"
    assert df.loc[0, "profile_url"] == "https://instagram.com/janedoe"
