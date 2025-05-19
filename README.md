# NCAA Division-I Roster Scraper (2016-2021)

This repository will house Python code (generated via OpenAI Codex) that:

1. Scrapes Sports-Reference rosters for
   • Men's Division-I Basketball
   • Women's Division-I Basketball
   • FBS Division-I Football
   seasons 2016-17 → 2020-21.
2. Consolidates raw data into `data/master_raw.csv`.
3. Cleans / de-dupes into `data/master_clean.csv`.

All code is added via Codex pull-requests and manually reviewed.

## Selenium Scraper

The file `selenium_scraper.py` can be used to scrape rosters with
Selenium.  It expects three CSV files containing team information:

```
358.csv  - men's basketball schools
356.csv  - women's basketball schools
130.csv  - FBS football schools
```

Each file should include `school_slug`, `school_name` and `conference`
columns. The script writes output to `data/selenium_rosters.csv` and can
resume if interrupted.

### Installation

1. Install Python dependencies:

   ```bash
   pip install -r requirements.txt
   ```

   You will also need Google Chrome and the matching ChromeDriver
   available on your `PATH`.

2. Place the team CSV files in the working directory and run:

   ```bash
   python selenium_scraper.py
   ```
