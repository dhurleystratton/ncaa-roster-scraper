import pandas as pd
from clean import main


def test_clean(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    raw = pd.DataFrame(
        {
            "season": ["2019-20", "2019-20", "2019-20", "2020-21"],
            "sport": ["men", "men", "women", "football"],
            "school": ["Duke ", "duke ", "UCLA", "ucla"],
            "player": ["John Doe", "John Doe", "Jane Roe", "Jack Smith"],
        }
    )

    raw.to_csv(data_dir / "master_raw.csv", index=False)

    monkeypatch.chdir(tmp_path)

    main()

    clean_file = tmp_path / "data" / "master_clean.csv"
    assert clean_file.exists()

    raw_rows = len(raw)
    clean_rows = len(pd.read_csv(clean_file))
    assert clean_rows <= raw_rows * 1.05
