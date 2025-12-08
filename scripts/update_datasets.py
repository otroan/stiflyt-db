#!/usr/bin/env python3
"""
Cron-friendly script to update all datasets from configuration file.

This script:
1. Downloads updated datasets (only if newer versions are available)
2. Loads them into PostGIS database (replacing old data)
3. Logs everything for monitoring

Usage:
    python3 scripts/update_datasets.py [config_file] [database_name]

Environment variables:
    PGDATABASE - Database name (default: matrikkel)
    LOG_DIR    - Directory for logs (default: ./logs)

For cron, add to crontab:
    0 2 * * * /path/to/python3 /path/to/stiflyt-db/scripts/update_datasets.py /path/to/datasets.yaml >> /path/to/logs/cron.log 2>&1
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

try:
    import yaml
except ImportError:
    print("Feil: PyYAML ikke installert. Installer med: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

# Import functions directly instead of using subprocess
try:
    # Add scripts directory to path for imports
    scripts_dir = Path(__file__).parent
    sys.path.insert(0, str(scripts_dir))

    from download_kartverket import download_from_config
    from load_dataset import load_dataset
    from db_status import check_database_health
except ImportError as e:
    print(f"Feil: Kunne ikke importere nødvendige moduler: {e}", file=sys.stderr)
    sys.exit(1)


def setup_logging(log_dir: Path) -> Path:
    """Setup logging directory and return log file path."""
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"update_{timestamp}.log"
    return log_file


def log(message: str, log_file: Path, also_print: bool = True):
    """Log message to file and optionally print."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_line = f"[{timestamp}] {message}\n"
    with open(log_file, 'a') as f:
        f.write(log_line)
    if also_print:
        print(message)


def load_config(config_path: Path) -> List[Dict[str, Any]]:
    """Load YAML configuration file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Feil: Kunne ikke laste konfigurasjonsfil: {e}", file=sys.stderr)
        sys.exit(1)


def download_datasets(config_path: Path, log_file: Path) -> bool:
    """Download datasets using download function."""
    log("==> Downloading datasets...", log_file)

    try:
        # Capture stdout/stderr by redirecting temporarily
        import io
        from contextlib import redirect_stdout, redirect_stderr

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            download_from_config(config_path)

        # Write captured output to log
        stdout_output = stdout_capture.getvalue()
        stderr_output = stderr_capture.getvalue()

        with open(log_file, 'a') as f:
            if stdout_output:
                f.write(stdout_output)
            if stderr_output:
                f.write(stderr_output)

        log("✓ Download completed", log_file)
        return True
    except Exception as e:
        log(f"✗ Download failed: {e}", log_file)
        return False


def load_postgis_dataset(zip_file: Path, database: str, log_file: Path) -> bool:
    """Load PostGIS SQL dataset."""
    try:
        # Validate inputs
        if zip_file is None:
            log(f"✗ Feil: zip_file er None", log_file)
            return False

        if not isinstance(zip_file, Path):
            zip_file = Path(zip_file)

        zip_file = zip_file.resolve()

        if not zip_file.exists():
            log(f"✗ Feil: ZIP-fil eksisterer ikke: {zip_file}", log_file)
            return False

        import io
        from contextlib import redirect_stdout, redirect_stderr

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            success = load_dataset(zip_file, database, drop_tables=True)

        # Write captured output to log
        stdout_output = stdout_capture.getvalue()
        stderr_output = stderr_capture.getvalue()

        with open(log_file, 'a') as f:
            if stdout_output:
                f.write(stdout_output)
            if stderr_output:
                f.write(stderr_output)

        return success
    except Exception as e:
        log(f"✗ Failed to load PostGIS dataset: {e}", log_file)
        return False


def load_gml_dataset(zip_file: Path, database: str, table_name: str, srid: int, log_file: Path) -> bool:
    """Load GML dataset."""
    try:
        # Validate inputs
        if zip_file is None:
            log(f"✗ Feil: zip_file er None", log_file)
            return False

        if not isinstance(zip_file, Path):
            zip_file = Path(zip_file)

        zip_file = zip_file.resolve()

        if not zip_file.exists():
            log(f"✗ Feil: ZIP-fil eksisterer ikke: {zip_file}", log_file)
            return False

        import io
        from contextlib import redirect_stdout, redirect_stderr

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            success = load_dataset(zip_file, database, table_name=table_name, target_srid=srid)

        # Write captured output to log
        stdout_output = stdout_capture.getvalue()
        stderr_output = stderr_capture.getvalue()

        with open(log_file, 'a') as f:
            if stdout_output:
                f.write(stdout_output)
            if stderr_output:
                f.write(stderr_output)

        return success
    except Exception as e:
        log(f"✗ Failed to load GML dataset: {e}", log_file)
        return False


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(description='Update datasets from configuration file')
    parser.add_argument('config_file', nargs='?', default='datasets.yaml',
                       help='Path to YAML configuration file (default: datasets.yaml)')
    parser.add_argument('database', nargs='?', default=None,
                       help='Database name (default: from PGDATABASE env or matrikkel)')
    parser.add_argument('--log-dir', default='./logs',
                       help='Directory for log files (default: ./logs)')

    args = parser.parse_args()

    # Setup
    config_path = Path(args.config_file)
    database = args.database or os.environ.get('PGDATABASE', 'matrikkel')
    log_dir = Path(args.log_dir)

    if not config_path.exists():
        print(f"Feil: Konfigurasjonsfil ikke funnet: {config_path}", file=sys.stderr)
        sys.exit(1)

    # Setup logging
    log_file = setup_logging(log_dir)
    log("=== Starting dataset update ===", log_file)
    log(f"Config file: {config_path}", log_file)
    log(f"Database: {database}", log_file)
    log(f"Log file: {log_file}", log_file)

    # Load configuration
    configs = load_config(config_path)
    log(f"Found {len(configs)} datasets in configuration", log_file)

    # Download datasets
    if not download_datasets(config_path, log_file):
        log("ERROR: Download failed - aborting", log_file)
        sys.exit(1)

    # Load each dataset
    log("==> Loading datasets into database...", log_file)
    success_count = 0
    failed_count = 0

    for cfg in configs:
        name = cfg.get('name', 'unknown')
        dataset = cfg.get('dataset', '')
        format_type = cfg.get('format', '')
        output_dir_str = cfg.get('output_dir', './data')
        # Resolve relative paths to absolute
        output_dir = Path(output_dir_str).resolve()
        utm_zone = cfg.get('utm_zone', '25833')

        log(f"  -> Processing {name} ({format_type} format)...", log_file)

        # Find ZIP files in output directory
        zip_files = list(output_dir.glob('*.zip'))

        if not zip_files:
            log(f"    ⚠ No ZIP files found in {output_dir} - skipping", log_file)
            failed_count += 1
            continue

        # Filter ZIP files by dataset name if multiple exist (use most recent)
        # This prevents loading the same dataset multiple times
        if len(zip_files) > 1:
            # Sort by modification time, most recent first
            zip_files = sorted(zip_files, key=lambda p: p.stat().st_mtime, reverse=True)
            log(f"    ℹ Found {len(zip_files)} ZIP files, using most recent: {zip_files[0].name}", log_file)
            zip_files = zip_files[:1]  # Only process most recent

        # Process ZIP file (should be only one after filtering)
        zip_file = zip_files[0]

        # Ensure zip_file is a Path object
        if not isinstance(zip_file, Path):
            zip_file = Path(zip_file)
        zip_file = zip_file.resolve()  # Make absolute

        if format_type == 'PostGIS':
            if load_postgis_dataset(zip_file, database, log_file):
                log(f"    ✓ {name} loaded successfully", log_file)
                success_count += 1
            else:
                log(f"    ✗ Failed to load {name}", log_file)
                failed_count += 1

        elif format_type == 'GML':
            # Use dataset name as table name (sanitized)
            table_name = name.lower().replace('-', '_').replace(' ', '_')
            # Handle utm_zone - it might be int or str from YAML
            if isinstance(utm_zone, int):
                srid = utm_zone
            elif isinstance(utm_zone, str) and utm_zone.isdigit():
                srid = int(utm_zone)
            else:
                srid = 25833  # Default

            if load_gml_dataset(zip_file, database, table_name, srid, log_file):
                log(f"    ✓ {name} loaded successfully", log_file)
                success_count += 1
            else:
                log(f"    ✗ Failed to load {name}", log_file)
                failed_count += 1

        else:
            log(f"    ⚠ Unknown format '{format_type}' - skipping", log_file)
            failed_count += 1

    # Summary
    log("==> Update completed", log_file)
    log(f"  ✓ Successful: {success_count}", log_file)
    log(f"  ✗ Failed: {failed_count}", log_file)

    # Post-update health check
    log("==> Verifying database health...", log_file)
    try:
        from db_status import get_db_connection_params
        db_params = get_db_connection_params()
        db_params['database'] = database

        is_healthy, status = check_database_health(db_params, min_tables=len(configs))

        if is_healthy:
            log("  ✓ Database health check passed", log_file)
            log(f"  Tables: {status['table_count']}", log_file)
            if status['database_size']:
                log(f"  Database size: {status['database_size']}", log_file)
        else:
            log("  ✗ Database health check failed", log_file)
            for error in status.get('errors', []):
                log(f"    • {error}", log_file)
            log("  ⚠ Database may be in inconsistent state", log_file)
    except Exception as e:
        log(f"  ⚠ Health check failed: {e}", log_file)

    log("=== End of update ===", log_file)

    # Clean up old logs (keep last 30 days)
    try:
        from datetime import timedelta
        cutoff_date = datetime.now() - timedelta(days=30)
        for old_log in log_dir.glob('update_*.log'):
            if old_log.stat().st_mtime < cutoff_date.timestamp():
                old_log.unlink()
    except Exception:
        pass  # Ignore cleanup errors

    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    main()

