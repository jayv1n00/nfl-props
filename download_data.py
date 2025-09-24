import os
import pandas as pd

os.makedirs("data/raw", exist_ok=True)

PBP_BASE = "https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{y}.parquet"
WEEKLY_BASE = "https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_{y}.parquet"

# Include 2025 (script will skip years that 404)
YEARS = [2020, 2021, 2022, 2023, 2024, 2025]

def fetch_parquet(url: str) -> pd.DataFrame:
    # pandas can read parquet from https URLs directly
    return pd.read_parquet(url)

def safe_download(label: str, url: str, out_path: str):
    try:
        print(f"Downloading {label} from:\n  {url}")
        df = fetch_parquet(url)
        df.to_parquet(out_path)
        print(f"✓ Saved {out_path}  (rows={len(df):,})\n")
    except Exception as e:
        print(f"✗ Skipped {label}: {e}\n")

def main():
    for y in YEARS:
        # Play-by-play per year
        safe_download(f"PBP {y}", PBP_BASE.format(y=y), f"data/raw/play_by_play_{y}.parquet")
        # Weekly player stats per year
        safe_download(f"Weekly stats {y}", WEEKLY_BASE.format(y=y), f"data/raw/player_stats_{y}.parquet")

    print("✅ Done. Files are in data/raw/")

if __name__ == "__main__":
    main()