from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "RUvideos.csv"
TARGET = ROOT / "RUvideos_ydb.csv"


def main() -> None:
    with SOURCE.open("r", encoding="utf-8", errors="replace", newline="") as source:
        reader = csv.reader(source)
        header = next(reader)

        with TARGET.open("w", encoding="utf-8", newline="") as target:
            writer = csv.writer(target)
            writer.writerow(["row_id", *header])

            for row_id, row in enumerate(reader, start=1):
                writer.writerow([row_id, *row])

    print(f"Prepared {TARGET}")


if __name__ == "__main__":
    main()
