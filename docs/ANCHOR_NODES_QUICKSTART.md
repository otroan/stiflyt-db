# Anchor Nodes - Quick Start for Backend

## Finn anchor_nodes

```sql
-- 1. Finn skjema
SELECT nspname FROM pg_namespace
WHERE nspname LIKE 'turogfriluftsruter_%'
ORDER BY nspname DESC LIMIT 1;

-- 2. Query anchor_nodes
SELECT node_id, geom, navn, navn_kilde
FROM <schema>.anchor_nodes
WHERE navn IS NOT NULL;
```

## Struktur

- `node_id` - Unik node ID
- `geom` - Punktgeometri (SRID 25833)
- `navn` - Navn fra ruteinfopunkt eller stedsnavn
- `navn_kilde` - `'ruteinfopunkt'` eller `'stedsnavn'`
- `anchor_type` - `'topology'` eller `'ruteinfopunkt'`

## Navn-prioritering

1. **Ruteinfopunkt** (100m radius) - høyest prioritet
2. **Stedsnavn** (200m radius) - fallback

## Eksempel: Python

```python
# Finn skjema
cur.execute("""
    SELECT nspname FROM pg_namespace
    WHERE nspname LIKE 'turogfriluftsruter_%'
    ORDER BY nspname DESC LIMIT 1
""")
schema = cur.fetchone()[0]

# Query anchor_nodes
cur.execute(f"""
    SELECT node_id, ST_AsGeoJSON(geom)::json as geom, navn, navn_kilde
    FROM {schema}.anchor_nodes
    WHERE navn IS NOT NULL
    LIMIT 100
""")
nodes = cur.fetchall()
```

## Spatial query

```sql
-- Finn anchor nodes innenfor 1km av et punkt
SELECT node_id, navn, navn_kilde,
       ST_Distance(geom, ST_SetSRID(ST_MakePoint(?, ?), 25833)) as dist
FROM <schema>.anchor_nodes
WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(?, ?), 25833), 1000)
  AND navn IS NOT NULL
ORDER BY dist;
```

Se `ANCHOR_NODES.md` for full dokumentasjon.

