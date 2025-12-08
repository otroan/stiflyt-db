# Database Inspection Guide

## Quick Start

### View All Tables
```bash
# Using Makefile
make inspect-db

# Or directly
venv/bin/python3 scripts/inspect_db.py matrikkel --tables
```

### View Schema for a Specific Table
```bash
venv/bin/python3 scripts/inspect_db.py matrikkel --schema "turogfriluftsruter_a76f986a1d204cc9a06ad5d87ce9f94b.fotruteinfo"
```

### View Sample Data from a Table
```bash
# Show 5 rows (default)
venv/bin/python3 scripts/inspect_db.py matrikkel --sample "turogfriluftsruter_a76f986a1d204cc9a06ad5d87ce9f94b.fotruteinfo"

# Show 10 rows
venv/bin/python3 scripts/inspect_db.py matrikkel --sample "turogfriluftsruter_a76f986a1d204cc9a06ad5d87ce9f94b.fotruteinfo" --rows 10
```

### View All Information
```bash
venv/bin/python3 scripts/inspect_db.py matrikkel --all
```

## Available Options

- `--tables` - List all tables with row counts and sizes
- `--schema TABLE` - Show detailed schema for a specific table (columns, geometry columns, indexes)
- `--indexes` - List all indexes for all tables
- `--srids` - List all spatial reference systems (SRIDs) in use
- `--sample TABLE` - Show sample data from a table (default: 5 rows)
- `--rows N` - Number of sample rows to show (use with --sample)
- `--all` - Show all available information

## Examples

### Example 1: Quick Overview
```bash
make inspect-db
```

Output shows:
- All tables
- Estimated row counts
- Table sizes

### Example 2: Detailed Schema
```bash
venv/bin/python3 scripts/inspect_db.py matrikkel \
  --schema "turogfriluftsruter_a76f986a1d204cc9a06ad5d87ce9f94b.fotruteinfo"
```

Shows:
- Column names and types
- Geometry columns with SRID
- Indexes

### Example 3: Sample Data
```bash
venv/bin/python3 scripts/inspect_db.py matrikkel \
  --sample "turogfriluftsruter_a76f986a1d204cc9a06ad5d87ce9f94b.fotruteinfo" \
  --rows 10
```

Shows:
- Total row count
- Column names
- Sample rows in a formatted table

### Example 4: All Information
```bash
venv/bin/python3 scripts/inspect_db.py matrikkel --all
```

Shows everything: tables, indexes, SRIDs, etc.

## Using psql Directly

You can also use `psql` directly for more advanced queries:

```bash
# Connect to database
psql -U otroan -d matrikkel

# Then run SQL queries:
\dt                    # List tables
\d+ schema.table       # Show table schema
SELECT * FROM schema.table LIMIT 10;  # Sample data
```

## Tips

1. **Table Names**: Some tables have long schema names. Use tab completion in bash or copy-paste from `--tables` output.

2. **Geometry Columns**: Geometry columns are automatically converted to text format (WKT) in sample data for readability.

3. **Large Tables**: For very large tables, use `--rows` to limit output.

4. **Schema Names**: If table name doesn't include schema, it defaults to `public`. Use `schema.table` format for other schemas.

