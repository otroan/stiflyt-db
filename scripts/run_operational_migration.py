#!/usr/bin/env python3
"""Run operational schema migration on operational database."""
import sys
from pathlib import Path
import subprocess
import os


def _env_truthy(value):
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def main():
    sql_file = Path("scripts/operational_schema.sql")
    if not sql_file.exists():
        print(f"Error: {sql_file} not found")
        sys.exit(1)

    cmd = ["psql"]
    database_url = os.getenv("OP_DATABASE_URL")
    if database_url:
        cmd.append(database_url)
    else:
        # Default to Unix socket (same behavior as other migration scripts)
        use_unix_socket = _env_truthy(os.getenv("OP_USE_UNIX_SOCKET"))
        if os.getenv("OP_USE_UNIX_SOCKET") is None:
            use_unix_socket = True

        socket_dir = os.getenv("OP_DB_SOCKET_DIR")
        if use_unix_socket:
            if socket_dir:
                cmd.extend(["-h", socket_dir])
        else:
            host = os.getenv("PGHOST", "localhost")
            port = os.getenv("PGPORT", "5432")
            cmd.extend(["-h", host, "-p", port])

        db_user = os.getenv("OP_DB_USER") or os.getenv("PGUSER") or os.getenv("USER", "postgres")
        cmd.extend(["-U", db_user])

        db_password = os.getenv("OP_DB_PASSWORD") or os.getenv("PGPASSWORD")
        if db_password:
            os.environ["PGPASSWORD"] = db_password

        db_name = os.getenv("OP_DB_NAME") or os.getenv("PGDATABASE", "matrikkel")
        cmd.extend(["-d", db_name])

    cmd.extend(["-f", str(sql_file)])

    if database_url:
        print("Running operational schema migration using OP_DATABASE_URL")
    else:
        print(f"Running operational schema migration on database: {db_name}")
        print(f"Using {'Unix socket' if use_unix_socket else 'TCP'} connection")
        if use_unix_socket and socket_dir:
            print(f"Socket directory: {socket_dir}")
        print(f"User: {db_user}")

    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
