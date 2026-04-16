from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_FILE = PROJECT_ROOT / "data" / "processed" / "joao_pessoa_combustiveis.csv"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gasolina_jp.pipeline import update_dataset


def run_update(source_url: str | None = None) -> None:
    result = update_dataset(raw_dir=RAW_DIR, processed_file=PROCESSED_FILE, source_url=source_url, limit=3)

    print("Update complete")
    print(f"Source URL: {result['source_url']}")
    print(f"Raw rows: {result['raw_rows']}")
    print(f"Rows in top3 output: {result['result_rows']}")
    print(f"Output file: {result['processed_file']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download and process ANP data for Joao Pessoa.")
    parser.add_argument(
        "--source-url",
        help="Optional custom XLSX/CSV/ZIP URL for ANP-like dataset.",
        default=None,
    )
    args = parser.parse_args()
    run_update(source_url=args.source_url)


if __name__ == "__main__":
    main()
