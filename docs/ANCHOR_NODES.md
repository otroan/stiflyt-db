# Anchor Nodes - Backend Guide

## Oversikt

`anchor_nodes` er en materialized view som inneholder alle endepunkter (anchor nodes) i rutenettverket, beriket med navn fra både `ruteinfopunkt` og `stedsnavn` databaser.

## Hvordan finne anchor_nodes

### 1. Finn riktig skjema

Anchor nodes ligger i et skjema med prefiks `turogfriluftsruter_` etterfulgt av en hash. For å finne det aktive skjemaet:

```sql
SELECT nspname
FROM pg_namespace
WHERE nspname LIKE 'turogfriluftsruter_%'
  AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
ORDER BY nspname DESC
LIMIT 1;
```

Eller i backend-kode (eksempel Python/psycopg2):

```python
def get_turrute_schema(conn):
    """Find the active turogfriluftsruter schema."""
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
```

### 2. Query anchor_nodes

Når du har skjema-navnet, kan du querye anchor_nodes direkte:

```sql
SELECT
    node_id,
    geom,
    degree,
    anchor_type,
    navn,
    navn_kilde,
    navn_distance_m
FROM <schema_name>.anchor_nodes
WHERE navn IS NOT NULL
ORDER BY navn_distance_m
LIMIT 10;
```

## Struktur

### Kolonner

| Kolonne | Type | Beskrivelse |
|---------|------|-------------|
| `node_id` | integer | Unik ID for noden (fra `nodes` tabellen) |
| `geom` | geometry(Point) | Punktgeometri i SRID 25833 (UTM zone 33N) |
| `degree` | bigint | Antall segmenter som kobler til denne noden |
| `anchor_type` | text | Type anchor: `'topology'` (degree != 2) eller `'ruteinfopunkt'` |
| `ruteinfopunkt_objid` | integer | ID fra ruteinfopunkt hvis matchet (NULL ellers) |
| `ruteinfopunkt_distance_m` | double precision | Avstand til ruteinfopunkt i meter |
| `navn` | text | Navn på stedet (fra ruteinfopunkt eller stedsnavn) |
| `navn_kilde` | text | Kilde for navnet: `'ruteinfopunkt'` eller `'stedsnavn'` |
| `navn_distance_m` | double precision | Avstand til navnekilden i meter |

### Indekser

- `idx_anchor_nodes_node_id` - BTREE på `node_id`
- `idx_anchor_nodes_geom_gist` - GIST på `geom` (for spatial queries)
- `idx_anchor_nodes_anchor_type` - BTREE på `anchor_type`
- `idx_anchor_nodes_navn` - BTREE på `navn` (partial, WHERE navn IS NOT NULL)

## Hvordan navnene er koblet

### Prioritering

1. **Ruteinfopunkt** (prioritet 1): Hvis en node er innenfor 100m av et `ruteinfopunkt`, brukes navnet derfra
2. **Stedsnavn** (prioritet 2): Hvis ingen ruteinfopunkt-match, søkes det i `stedsnavn` innenfor 200m

### Ruteinfopunkt-navn

Navn fra `ruteinfopunkt` bruker:
- `opphav` som primærkilde
- `informasjon` som fallback hvis `opphav` er tom

### Stedsnavn-navn

Navn fra `stedsnavn` bruker den normaliserte strukturen:
- `stedsnavn` tabell: metadata (objid, sted_fk, navnestatus, etc.)
- `skrivemate` tabell: faktisk navn (`komplettskrivemate`) koblet via `stedsnavn.objid = skrivemate.stedsnavn_fk`
- `sted_posisjon` tabell: geometri koblet via `stedsnavn.sted_fk = sted_posisjon.stedsnummer`

**Viktig:** `stedsnavn`, `skrivemate` og `sted_posisjon` ligger i `public` skjemaet, ikke i turogfriluftsruter-skjemaet.

## Eksempel-queries

### Finn alle anchor nodes med navn

```sql
SELECT
    node_id,
    ST_AsText(geom) as geom_wkt,
    navn,
    navn_kilde,
    navn_distance_m
FROM <schema_name>.anchor_nodes
WHERE navn IS NOT NULL
ORDER BY navn;
```

### Finn anchor nodes innenfor et område

```sql
SELECT
    node_id,
    navn,
    navn_kilde,
    ST_Distance(geom, ST_SetSRID(ST_MakePoint(?, ?), 25833)) as distance_m
FROM <schema_name>.anchor_nodes
WHERE ST_DWithin(
    geom,
    ST_SetSRID(ST_MakePoint(?, ?), 25833),
    1000  -- 1km radius
)
AND navn IS NOT NULL
ORDER BY distance_m
LIMIT 20;
```

### Finn anchor nodes av en spesifikk type

```sql
-- Topologiske endepunkter (degree != 2)
SELECT COUNT(*)
FROM <schema_name>.anchor_nodes
WHERE anchor_type = 'topology';

-- Ruteinfopunkt-matcher
SELECT COUNT(*)
FROM <schema_name>.anchor_nodes
WHERE anchor_type = 'ruteinfopunkt';
```

### Søk etter navn

```sql
SELECT
    node_id,
    navn,
    navn_kilde,
    navn_distance_m
FROM <schema_name>.anchor_nodes
WHERE navn ILIKE '%torget%'
ORDER BY navn_distance_m;
```

### Statistikk

```sql
SELECT
    anchor_type,
    COUNT(*) as total,
    COUNT(navn) as with_names,
    COUNT(CASE WHEN navn_kilde = 'ruteinfopunkt' THEN 1 END) as from_ruteinfopunkt,
    COUNT(CASE WHEN navn_kilde = 'stedsnavn' THEN 1 END) as from_stedsnavn
FROM <schema_name>.anchor_nodes
GROUP BY anchor_type;
```

## Oppdatering

`anchor_nodes` er en materialized view som må oppdateres manuelt eller via migrasjoner. For å oppdatere:

```sql
REFRESH MATERIALIZED VIEW <schema_name>.anchor_nodes;
```

**Merk:** Dette kan ta tid hvis det er mange noder. Vurder å kjøre `CONCURRENTLY` hvis det er mulig:

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY <schema_name>.anchor_nodes;
```

(Krever unik indeks på `node_id`, som allerede eksisterer)

## Relaterte tabeller

- `<schema_name>.nodes` - Alle noder i rutenettverket
- `<schema_name>.node_degree` - Degree (antall koblinger) per node
- `<schema_name>.ruteinfopunkt` - Ruteinformasjonspunkter med navn
- `public.stedsnavn` - Stedsnavn metadata
- `public.skrivemate` - Stedsnavn navn
- `public.sted_posisjon` - Stedsnavn geometri

## Eksempel: Full backend-funksjon

```python
import psycopg2
from typing import Optional, List, Dict

def get_anchor_nodes(
    conn,
    schema_name: Optional[str] = None,
    limit: int = 100,
    with_names_only: bool = False,
    bbox: Optional[tuple] = None  # (minx, miny, maxx, maxy)
) -> List[Dict]:
    """
    Get anchor nodes from the database.

    Args:
        conn: Database connection
        schema_name: Schema name (auto-detected if None)
        limit: Maximum number of results
        with_names_only: Only return nodes with names
        bbox: Optional bounding box filter (minx, miny, maxx, maxy)

    Returns:
        List of anchor node dictionaries
    """
    # Find schema if not provided
    if schema_name is None:
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
            if not result:
                return []
            schema_name = result[0]

    # Build query
    query = f"""
        SELECT
            node_id,
            ST_AsGeoJSON(geom)::json as geom,
            degree,
            anchor_type,
            navn,
            navn_kilde,
            navn_distance_m
        FROM {schema_name}.anchor_nodes
        WHERE 1=1
    """

    params = []

    if with_names_only:
        query += " AND navn IS NOT NULL"

    if bbox:
        minx, miny, maxx, maxy = bbox
        query += " AND ST_Intersects(geom, ST_MakeEnvelope(%s, %s, %s, %s, 25833))"
        params.extend([minx, miny, maxx, maxy])

    query += " ORDER BY navn_distance_m NULLS LAST LIMIT %s"
    params.append(limit)

    # Execute
    with conn.cursor() as cur:
        cur.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

# Usage example
conn = psycopg2.connect("dbname=matrikkel user=...")
nodes = get_anchor_nodes(conn, with_names_only=True, limit=50)
for node in nodes:
    print(f"Node {node['node_id']}: {node['navn']} ({node['navn_kilde']})")
```

## Notater

- Alle geometrier er i SRID 25833 (UTM zone 33N)
- `anchor_nodes` oppdateres automatisk når `make run-migrations` kjøres
- Navnene prioriterer ruteinfopunkt over stedsnavn
- Hvis en node har både ruteinfopunkt og stedsnavn-match, brukes ruteinfopunkt-navnet

