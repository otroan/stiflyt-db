#!/usr/bin/env python3
"""
Test script for finding distance between a ruteinfopunkt and nearest link endpoint.

This script finds a ruteinfopunkt (e.g., a DNT hut named "Fast") and calculates
the distance to the nearest link endpoint (node).

The script searches for names in the "opphav" field of the ruteinfopunkt table.

Usage:
    # List all fields/columns in ruteinfopunkt table
    python3 scripts/test_ruteinfopunkt_distance.py [database_name] --list-fields

    # Find DNT hytte named "Fast" (searches in opphav field, filters by vedlikeholdsansvarlig=DNT)
    python3 scripts/test_ruteinfopunkt_distance.py [database_name] --name Fast --vedlikeholdsansvarlig DNT

    # List all matches for debugging
    python3 scripts/test_ruteinfopunkt_distance.py [database_name] --name Fast --list-all

    # Search without vedlikeholdsansvarlig filter
    python3 scripts/test_ruteinfopunkt_distance.py [database_name] --name Fast --vedlikeholdsansvarlig ''

Environment variables:
    PGHOST       - PostgreSQL host (default: localhost)
    PGPORT       - PostgreSQL port (default: 5432)
    PGUSER       - PostgreSQL user (default: current user)
    PGPASSWORD   - PostgreSQL password (if needed)
"""

import os
import sys
import argparse
from typing import Optional, Dict, Any

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("Feil: psycopg2 ikke installert. Installer med: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)


def get_db_connection_params() -> dict:
    """Get database connection parameters from environment or defaults."""
    host = os.environ.get('PGHOST', 'localhost')
    if host == 'localhost' or host == '127.0.0.1':
        host = None

    return {
        'host': host,
        'port': os.environ.get('PGPORT', '5432') if host else None,
        'user': os.environ.get('PGUSER', os.environ.get('USER', 'postgres')),
        'password': os.environ.get('PGPASSWORD', ''),
        'database': os.environ.get('PGDATABASE', ''),
    }


def connect_db(db_params: dict):
    """Connect to database and return connection."""
    try:
        conn_kwargs = {
            'user': db_params['user'],
            'database': db_params['database'],
            'connect_timeout': 5
        }
        if db_params['host']:
            conn_kwargs['host'] = db_params['host']
        if db_params['port']:
            conn_kwargs['port'] = db_params['port']
        if db_params['password']:
            conn_kwargs['password'] = db_params['password']

        return psycopg2.connect(**conn_kwargs)
    except psycopg2.OperationalError as e:
        print(f"Feil: Kunne ikke koble til database: {e}", file=sys.stderr)
        sys.exit(1)


def find_schema(conn) -> Optional[str]:
    """Find turrutebasen schema dynamically."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT nspname
            FROM pg_namespace
            WHERE nspname LIKE 'turogfriluftsruter_%'
              AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
            ORDER BY nspname DESC
            LIMIT 1
        """)
        result = cur.fetchone()
        return result[0] if result else None


def get_ruteinfopunkt_columns(conn, schema: str) -> list:
    """Get all column names from ruteinfopunkt table."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = 'ruteinfopunkt'
            ORDER BY ordinal_position
        """, (schema,))
        return [row['column_name'] for row in cur.fetchall()]


def list_ruteinfopunkt_fields(conn, schema: str, sample_rows: int = 3):
    """List all fields in ruteinfopunkt table with their types and sample data."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get column information with data types
        cur.execute("""
            SELECT
                column_name,
                data_type,
                is_nullable,
                character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = 'ruteinfopunkt'
            ORDER BY ordinal_position
        """, (schema,))
        columns = cur.fetchall()

        if not columns:
            print("  Ingen kolonner funnet i ruteinfopunkt tabellen")
            return

        print(f"\n  Kolonner i ruteinfopunkt tabellen ({len(columns)} totalt):")
        print("  " + "=" * 80)

        # Get sample data
        cur.execute(f"""
            SELECT *
            FROM {schema}.ruteinfopunkt
            LIMIT %s
        """, (sample_rows,))
        sample_data = cur.fetchall()

        # Print column information
        for col in columns:
            col_name = col['column_name']
            data_type = col['data_type']
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            max_len = f"({col['character_maximum_length']})" if col['character_maximum_length'] else ""

            print(f"  {col_name:30} {data_type:20} {max_len:10} {nullable}")

        # Print sample data
        if sample_data:
            print(f"\n  Eksempeldata (første {len(sample_data)} rad(er)):")
            print("  " + "=" * 80)
            for i, row in enumerate(sample_data, 1):
                print(f"\n  Rad {i}:")
                for col in columns:
                    col_name = col['column_name']
                    value = row.get(col_name)
                    if value is None:
                        value_str = "NULL"
                    elif isinstance(value, (int, float)):
                        value_str = str(value)
                    elif isinstance(value, str):
                        # Truncate long strings
                        value_str = value[:60] + "..." if len(value) > 60 else value
                    else:
                        value_str = str(value)[:60]
                    print(f"    {col_name:30} = {value_str}")


def find_ruteinfopunkt(conn, schema: str, name: str, vedlikeholdsansvarlig: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Find ruteinfopunkt by name and optionally by vedlikeholdsansvarlig.

    Args:
        conn: Database connection
        schema: Schema name
        name: Name to search for (searches in opphav column)
        vedlikeholdsansvarlig: Optional filter for vedlikeholdsansvarlig field (e.g., 'DNT')
    """
    # First, get all columns to see what's available
    columns = get_ruteinfopunkt_columns(conn, schema)

    # Build SELECT clause - include all columns we might need
    select_cols = ['objid', 'opphav', 'informasjon', 'vedlikeholdsansvarlig', 'posisjon', 'ST_X(posisjon) as x', 'ST_Y(posisjon) as y']

    # Add type-related columns if they exist
    type_columns = [col for col in columns if any(keyword in col.lower() for keyword in ['type', 'kategori', 'klasse', 'betegnelse'])]
    for col in type_columns:
        if col not in select_cols:
            select_cols.append(col)

    # Build WHERE clause
    where_conditions = ['posisjon IS NOT NULL']
    params = []

    # Name search in opphav column
    name_pattern = f'%{name}%'
    name_starts_pattern = f'{name}%'
    where_conditions.append('(opphav ILIKE %s OR opphav ILIKE %s)')
    params.extend([name_pattern, name_starts_pattern])

    # Vedlikeholdsansvarlig filter if specified
    if vedlikeholdsansvarlig:
        # Search in vedlikeholdsansvarlig column
        vedlikeholdsansvarlig_pattern = f'%{vedlikeholdsansvarlig}%'
        where_conditions.append('vedlikeholdsansvarlig ILIKE %s')
        params.append(vedlikeholdsansvarlig_pattern)

    # Build ORDER BY clause for better prioritization
    order_by_parts = []

    # Prioritize exact name matches in opphav
    order_by_parts.append("""
        CASE
            WHEN opphav ILIKE %s THEN 1
            WHEN opphav ILIKE %s THEN 2
            ELSE 3
        END
    """)
    params.extend([name_starts_pattern, f'%{name}%'])

    # If vedlikeholdsansvarlig filter is specified, prioritize matches
    if vedlikeholdsansvarlig:
        order_by_parts.append(f"""
            CASE
                WHEN vedlikeholdsansvarlig ILIKE %s THEN 1
                ELSE 2
            END
        """)
        params.append(f'%{vedlikeholdsansvarlig}%')

    # Build query
    query = f"""
        SELECT {', '.join(select_cols)}
        FROM {schema}.ruteinfopunkt
        WHERE {' AND '.join(where_conditions)}
        ORDER BY {', '.join(order_by_parts)}, opphav
        LIMIT 20
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        results = cur.fetchall()

        if not results:
            return None

        # If we have multiple results and a vedlikeholdsansvarlig filter, try to find the best match
        if len(results) > 1 and vedlikeholdsansvarlig:
            vedlikeholdsansvarlig_lower = vedlikeholdsansvarlig.lower()
            # Prioritize results where vedlikeholdsansvarlig matches
            for result in results:
                vedlikeholdsansvarlig_val = (result.get('vedlikeholdsansvarlig') or '').lower()
                if vedlikeholdsansvarlig_lower in vedlikeholdsansvarlig_val:
                    return result

        # Return first result
        return results[0]


def find_nearest_link_endpoint(conn, schema: str, ruteinfopunkt_pos: Any) -> Optional[Dict[str, Any]]:
    """Find the nearest link endpoint (node) to a ruteinfopunkt position.

    Link endpoints are nodes that are either a_node or b_node in the links table.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find all unique nodes that are endpoints of links
        # Then find the nearest one to the ruteinfopunkt
        query = f"""
            WITH link_endpoints AS (
                -- Get all unique nodes that are endpoints of links
                SELECT DISTINCT node_id
                FROM (
                    SELECT a_node as node_id FROM {schema}.links
                    UNION
                    SELECT b_node as node_id FROM {schema}.links
                ) endpoints
            )
            SELECT
                n.id as node_id,
                n.geom,
                ST_X(n.geom) as x,
                ST_Y(n.geom) as y,
                ST_Distance(n.geom, %s) as distance_m
            FROM {schema}.nodes n
            INNER JOIN link_endpoints le ON n.id = le.node_id
            WHERE n.geom IS NOT NULL
            ORDER BY ST_Distance(n.geom, %s)
            LIMIT 1
        """
        cur.execute(query, (ruteinfopunkt_pos, ruteinfopunkt_pos))
        return cur.fetchone()


def get_link_endpoint_info(conn, schema: str, node_id: int) -> Dict[str, Any]:
    """Get information about links connected to a specific node endpoint."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = f"""
            SELECT
                COUNT(*) as link_count,
                ARRAY_AGG(DISTINCT link_id) as link_ids,
                ARRAY_AGG(DISTINCT
                    CASE
                        WHEN a_node = %s THEN 'a_node'
                        WHEN b_node = %s THEN 'b_node'
                    END
                ) FILTER (WHERE
                    CASE
                        WHEN a_node = %s THEN 'a_node'
                        WHEN b_node = %s THEN 'b_node'
                    END IS NOT NULL
                ) as endpoint_types
            FROM {schema}.links
            WHERE a_node = %s OR b_node = %s
        """
        cur.execute(query, (node_id, node_id, node_id, node_id, node_id, node_id))
        return cur.fetchone() or {}


def list_ruteinfopunkt_matches(conn, schema: str, name: str, limit: int = 20):
    """List all ruteinfopunkt matches for debugging - shows ALL fields."""
    columns = get_ruteinfopunkt_columns(conn, schema)

    # Select ALL columns from the table
    select_cols = []
    for col in columns:
        if col == 'posisjon':
            # Add position as both geometry and coordinates
            select_cols.append('posisjon')
            select_cols.append('ST_X(posisjon) as x')
            select_cols.append('ST_Y(posisjon) as y')
        else:
            select_cols.append(col)

    query = f"""
        SELECT {', '.join(select_cols)}
        FROM {schema}.ruteinfopunkt
        WHERE posisjon IS NOT NULL
          AND opphav ILIKE %s
        ORDER BY opphav
        LIMIT %s
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (f'%{name}%', limit))
        return cur.fetchall()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Find distance between ruteinfopunkt and nearest link endpoint'
    )
    parser.add_argument('database', nargs='?', default=None,
                       help='Database name (default: from PGDATABASE env or matrikkel)')
    parser.add_argument('--name', type=str, default='Fast',
                       help='Name of ruteinfopunkt to search for (default: Fast)')
    parser.add_argument('--vedlikeholdsansvarlig', type=str, default='DNT',
                       help='Filter for vedlikeholdsansvarlig field (default: DNT)')
    parser.add_argument('--list-all', action='store_true',
                       help='List all matches instead of finding distance')
    parser.add_argument('--list-fields', action='store_true',
                       help='List all fields/columns in ruteinfopunkt table with sample data')

    args = parser.parse_args()

    # Get database connection
    db_params = get_db_connection_params()
    db_params['database'] = args.database or db_params['database'] or 'matrikkel'

    print(f"==> Kobler til database: {db_params['database']}")
    conn = connect_db(db_params)
    print("✓ Tilkoblet")

    try:
        # Find schema
        schema = find_schema(conn)
        if not schema:
            print("\n✗ Fant ikke schema med prefix 'turogfriluftsruter_*'")
            print("\nMulige årsaker:")
            print("  1. Turrutebasen-datasettet er ikke lastet ennå")
            print("  2. Migrasjoner er ikke kjørt")
            print("\nFor å laste data:")
            print("  make update-datasets")
            print("  make run-migrations")
            sys.exit(1)

        print(f"✓ Fant schema: {schema}")

        # If --list-fields, show table structure
        if args.list_fields:
            print(f"\n==> Lister alle felter i ruteinfopunkt tabellen:")
            list_ruteinfopunkt_fields(conn, schema)
            return

        # If --list-all, show all matches
        if args.list_all:
            print(f"\n==> Lister alle ruteinfopunkt med navn som inneholder '{args.name}':")
            matches = list_ruteinfopunkt_matches(conn, schema, args.name)
            if not matches:
                print(f"  Ingen resultater funnet")
            else:
                print(f"  Fant {len(matches)} resultat(er):\n")
                for i, match in enumerate(matches, 1):
                    print(f"  {'='*80}")
                    print(f"  Resultat {i}:")
                    print(f"  {'='*80}")
                    # Show ALL fields
                    for key in sorted(match.keys()):
                        value = match[key]
                        if value is None:
                            value_str = "NULL"
                        elif isinstance(value, (int, float)):
                            value_str = str(value)
                        elif isinstance(value, str):
                            # Truncate very long strings
                            value_str = value[:100] + "..." if len(value) > 100 else value
                        elif isinstance(value, (list, tuple)):
                            value_str = str(value)[:100]
                        else:
                            value_str = str(value)[:100]
                        print(f"    {key:30} = {value_str}")
                    print()
            return

        # Find ruteinfopunkt
        print(f"\n==> Søker etter ruteinfopunkt:")
        print(f"    Navn: '{args.name}' (i opphav-feltet)")
        if args.vedlikeholdsansvarlig:
            print(f"    Vedlikeholdsansvarlig: '{args.vedlikeholdsansvarlig}'")
        ruteinfopunkt = find_ruteinfopunkt(conn, schema, args.name, args.vedlikeholdsansvarlig)

        if not ruteinfopunkt:
            print(f"\n✗ Fant ikke ruteinfopunkt med navn '{args.name}'")
            if args.vedlikeholdsansvarlig:
                print(f"   og vedlikeholdsansvarlig '{args.vedlikeholdsansvarlig}'")
            print("\nPrøv å:")
            print(f"  1. Liste alle treff: python3 scripts/test_ruteinfopunkt_distance.py {db_params['database']} --name {args.name} --list-all")
            print(f"  2. Søke med annet navn: python3 scripts/test_ruteinfopunkt_distance.py {db_params['database']} --name <navn>")
            print(f"  3. Fjerne vedlikeholdsansvarlig-filter: python3 scripts/test_ruteinfopunkt_distance.py {db_params['database']} --name {args.name} --vedlikeholdsansvarlig ''")
            sys.exit(1)

        print(f"✓ Fant ruteinfopunkt:")
        print(f"  ObjID: {ruteinfopunkt['objid']}")
        if 'opphav' in ruteinfopunkt:
            print(f"  Opphav: {ruteinfopunkt['opphav']}")
        if 'vedlikeholdsansvarlig' in ruteinfopunkt:
            print(f"  Vedlikeholdsansvarlig: {ruteinfopunkt['vedlikeholdsansvarlig']}")
        if 'informasjon' in ruteinfopunkt:
            print(f"  Informasjon: {ruteinfopunkt['informasjon']}")
        print(f"  Posisjon: ({ruteinfopunkt['x']:.2f}, {ruteinfopunkt['y']:.2f})")

        # Show additional columns if they exist
        for key in ruteinfopunkt.keys():
            if key not in ['objid', 'opphav', 'vedlikeholdsansvarlig', 'informasjon', 'posisjon', 'x', 'y'] and ruteinfopunkt[key]:
                print(f"  {key}: {ruteinfopunkt[key]}")

        # Find nearest link endpoint
        print(f"\n==> Søker etter nærmeste link endepunkt...")
        nearest_endpoint = find_nearest_link_endpoint(conn, schema, ruteinfopunkt['posisjon'])

        if not nearest_endpoint:
            print("\n✗ Fant ingen link endepunkter i databasen")
            print("\nMulige årsaker:")
            print("  1. Links er ikke bygget ennå")
            print("  2. Det finnes ingen links i databasen")
            print("\nFor å bygge links:")
            print("  make build-links")
            sys.exit(1)

        distance_m = nearest_endpoint['distance_m']
        print(f"✓ Fant nærmeste link endepunkt:")
        print(f"  Node ID: {nearest_endpoint['node_id']}")
        print(f"  Posisjon: ({nearest_endpoint['x']:.2f}, {nearest_endpoint['y']:.2f})")
        print(f"  Avstand: {distance_m:.2f} meter")

        # Get additional info about the endpoint
        endpoint_info = get_link_endpoint_info(conn, schema, nearest_endpoint['node_id'])
        if endpoint_info and endpoint_info.get('link_count'):
            print(f"\n  Link informasjon:")
            print(f"    Antall links: {endpoint_info['link_count']}")
            if endpoint_info.get('endpoint_types'):
                types_str = ', '.join(endpoint_info['endpoint_types'])
                print(f"    Endepunkt-typer: {types_str}")

        # Summary
        print(f"\n{'='*80}")
        print(f"RESULTAT:")
        ruteinfopunkt_name = ruteinfopunkt.get('opphav') or ruteinfopunkt.get('informasjon') or 'Ukjent'
        print(f"  Ruteinfopunkt: {ruteinfopunkt_name} (ObjID: {ruteinfopunkt['objid']})")
        print(f"  Nærmeste link endepunkt: Node {nearest_endpoint['node_id']}")
        print(f"  Avstand: {distance_m:.2f} meter ({distance_m/1000:.3f} km)")
        print(f"{'='*80}")

    finally:
        conn.close()
        print("\n✓ Tilkobling lukket")


if __name__ == "__main__":
    main()

