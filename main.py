"""Entry point for the FAF5 hub-selection data pipeline.

Usage
-----
    uv run python main.py download    # fetch all external sources
    uv run python main.py preprocess  # build the modelling dataset
    uv run python main.py all         # download then preprocess
"""
from __future__ import annotations

import argparse

from src.data import download, preprocess


def main() -> None:
    parser = argparse.ArgumentParser(description="FAF5 hub-selection pipeline")
    parser.add_argument(
        "step",
        choices=["download", "preprocess", "all"],
        help="Pipeline step to run",
    )
    args = parser.parse_args()

    if args.step in {"download", "all"}:
        download.main()
    if args.step in {"preprocess", "all"}:
        preprocess.main()


if __name__ == "__main__":
    main()
