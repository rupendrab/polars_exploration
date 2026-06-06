from __future__ import annotations

import argparse
from pathlib import Path
from shutil import copyfileobj
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DATASETS = [
    "yellow",
    "green",
    "fhv",
    "fhvhv",
]


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download NYC taxi parquet files for a given year and month."
    )
    parser.add_argument("year", type=int, help="Four-digit year, for example 2026")
    parser.add_argument("month", type=int, help="Month number from 1 to 12")
    args = parser.parse_args()

    if args.month < 1 or args.month > 12:
        parser.error("month must be between 1 and 12")

    return args


def build_urls(year: int, month: int) -> list[str]:
    suffix = f"{year:04d}-{month:02d}.parquet"
    return [f"{BASE_URL}/{dataset}_tripdata_{suffix}" for dataset in DATASETS]


def download_file(url: str, data_dir: Path) -> None:
    destination = data_dir / Path(url).name
    if destination.exists():
        print(f"Skipping {destination.name} (already exists)")
        return

    print(f"Downloading {destination.name}...")
    try:
        with urlopen(url) as response, destination.open("wb") as output:
            copyfileobj(response, output)
    except HTTPError as exc:
        print(f"Skipping {destination.name}: URL returned HTTP {exc.code}")
    except URLError as exc:
        print(f"Skipping {destination.name}: could not reach URL ({exc.reason})")


def main() -> None:
    args = parse_args()
    data_dir = project_root() / "data"
    data_dir.mkdir(exist_ok=True)

    for url in build_urls(args.year, args.month):
        download_file(url, data_dir)
    download_file(LOOKUP_URL, data_dir)

    print(f"Downloaded files to {data_dir}")


if __name__ == "__main__":
    main()
