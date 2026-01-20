#!/usr/bin/env python3
"""
Refresh database in-place using dataset-specific configs.

Flow:
1) Import datasets from config (daily or monthly)
2) Run migrations (fail-fast, ordered)
3) Refresh stable views (migration 005)
"""

import argparse
import os
import sys
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Refresh DB in-place from dataset config")
    parser.add_argument("database", nargs="?", default=None, help="Database name (default: PGDATABASE or matrikkel)")
    parser.add_argument("--config-file", default="datasets.yaml", help="Datasets config file (default: datasets.yaml)")
    args = parser.parse_args()

    base_db = args.database or os.environ.get("PGDATABASE", "matrikkel")
    config_file = args.config_file

    env = os.environ.copy()
    print(f"==> Refresh starting for database '{base_db}'")
    print(f"==> Importing datasets using '{config_file}'...")
    update_cmd = [
        sys.executable,
        str(Path(__file__).parent / "update_datasets.py"),
        config_file,
        base_db,
    ]
    subprocess.run(update_cmd, check=True, env=env)

    print(f"✓ Refresh complete: '{base_db}' updated in-place")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print(f"✗ Command failed: {e}", file=sys.stderr)
        sys.exit(1)
