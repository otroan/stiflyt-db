#!/usr/bin/env python3
"""
Investigate suspected link-building / route aggregation bug for:

- link_id: 6279
- suspicious rutenummer: 20160407
- node_id (place): 91705 ("Slæom")

This script runs a focused set of SQL queries and produces a diagnosis:
- Does link 6279 contain route 20160407 in rutenummer_list?
- Which segment(s) in the link carry route 20160407 in fotruteinfo?
- If none: does 20160407 still appear in rutenummer_list (aggregation bug)?

Usage:
  python3 scripts/investigate_link_6279.py
  python3 scripts/investigate_link_6279.py --link-id 6279 --route 20160407 --node-id 91705

Connection:
  Uses standard env vars: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.
  If PGHOST is localhost/127.0.0.1, uses Unix socket (peer auth) by omitting host/port.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def _connect():
    """Connect using psycopg2 or psycopg3, whichever is installed."""
    host = os.getenv("PGHOST", "localhost")
    if host in ("localhost", "127.0.0.1"):
        host = None

    params: Dict[str, Any] = {
        "user": os.getenv("PGUSER", os.getenv("USER", "postgres")),
        "database": os.getenv("PGDATABASE", ""),
        "connect_timeout": 5,
    }
    if not params["database"]:
        raise RuntimeError("PGDATABASE is not set (and no default database configured).")

    if host:
        params["host"] = host
        port = os.getenv("PGPORT", "5432")
        if port:
            params["port"] = port
    password = os.getenv("PGPASSWORD", "")
    if password:
        params["password"] = password

    try:
        import psycopg2  # type: ignore
        from psycopg2.extras import RealDictCursor  # type: ignore

        conn = psycopg2.connect(**params)
        conn.autocommit = True
        return conn, RealDictCursor, 2
    except ImportError:
        try:
            import psycopg  # type: ignore

            conn = psycopg.connect(**params)
            conn.autocommit = True
            return conn, None, 3
        except ImportError as e:
            raise RuntimeError("Need psycopg2 or psycopg installed.") from e


def _q(cur, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
    # psycopg2 uses "pyformat" paramstyle and will treat '%' in the SQL string as
    # interpolation markers *if* a second argument is provided to execute().
    # Therefore, only pass params when we actually have parameters.
    if params is None:
        cur.execute(sql)
    else:
        cur.execute(sql, params)
    rows = cur.fetchall()
    # psycopg2 RealDictCursor returns dict rows already; psycopg3 default returns tuples
    if rows and isinstance(rows[0], dict):
        return rows  # type: ignore[return-value]
    # psycopg3: use cursor.description to map
    cols = [d.name for d in cur.description]  # type: ignore[union-attr]
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({cols[i]: r[i] for i in range(len(cols))})
    return out


def _print_kv(title: str, items: Iterable[Tuple[str, Any]]) -> None:
    print(f"\n==> {title}")
    for k, v in items:
        print(f"  - {k}: {v}")


def _print_rows(title: str, rows: List[Dict[str, Any]], limit: int = 50) -> None:
    print(f"\n==> {title}")
    if not rows:
        print("  (no rows)")
        return
    keys = list(rows[0].keys())
    for i, row in enumerate(rows[:limit], 1):
        parts = [f"{k}={row.get(k)!r}" for k in keys]
        print(f"  {i:>2}. " + ", ".join(parts))
    if len(rows) > limit:
        print(f"  ... ({len(rows) - limit} more rows)")


def _find_dynamic_schema(cur) -> Optional[str]:
    rows = _q(
        cur,
        """
        SELECT nspname
        FROM pg_namespace
        WHERE nspname LIKE 'turogfriluftsruter_%'
          AND nspname NOT IN ('pg_catalog','information_schema','pg_toast','pg_temp_1','pg_toast_temp_1')
        ORDER BY nspname DESC
        LIMIT 1
        """,
    )
    return rows[0]["nspname"] if rows else None


@dataclass(frozen=True)
class Diagnosis:
    has_route_in_link_list: bool
    segments_with_route: List[int]
    route_inferred_from_segments: List[str]
    mismatch: bool
    summary: str


def investigate(link_id: int, route: str, node_id: int) -> Diagnosis:
    conn, real_dict_cursor, psycopg_version = _connect()
    try:
        if psycopg_version == 2:
            cur = conn.cursor(cursor_factory=real_dict_cursor)
        else:
            cur = conn.cursor()

        dynamic_schema = _find_dynamic_schema(cur)
        _print_kv(
            "Connection",
            [
                ("PGDATABASE", os.getenv("PGDATABASE")),
                ("PGUSER", os.getenv("PGUSER", os.getenv("USER"))),
                ("PGHOST", os.getenv("PGHOST", "localhost")),
                ("dynamic_schema", dynamic_schema),
            ],
        )

        # 1) Link row (stable view)
        link_rows = _q(
            cur,
            """
            SELECT link_id, a_node, b_node, length_m, segment_objids, rutenummer_list, rutenavn_list
            FROM stiflyt.links_with_routes
            WHERE link_id = %s
            """,
            (link_id,),
        )
        if not link_rows:
            raise RuntimeError(f"link_id={link_id} not found in stiflyt.links_with_routes")
        link = link_rows[0]
        _print_rows(f"Link {link_id} (stiflyt.links_with_routes)", link_rows)

        rutenummer_list = link.get("rutenummer_list") or []
        has_route_in_link_list = route in rutenummer_list

        # 2) Segments in link
        seg_rows = _q(
            cur,
            """
            SELECT ls.seq, ls.segment_id, ls.from_node, f.source_node, f.target_node
            FROM stiflyt.link_segments ls
            JOIN stiflyt.fotrute f ON f.objid = ls.segment_id
            WHERE ls.link_id = %s
            ORDER BY ls.seq
            """,
            (link_id,),
        )
        _print_rows(f"Segments in link {link_id} (stiflyt.link_segments)", seg_rows)
        segment_ids = [int(r["segment_id"]) for r in seg_rows]

        if not segment_ids:
            raise RuntimeError(f"link_id={link_id} has no segments in stiflyt.link_segments")

        # 3) Routes for segments in link (source of truth)
        placeholders = ",".join(["%s"] * len(segment_ids))
        fi_rows = _q(
            cur,
            f"""
            SELECT fotrute_fk as segment_id, rutenummer, rutenavn, vedlikeholdsansvarlig, rutetype
            FROM stiflyt.fotruteinfo
            WHERE fotrute_fk IN ({placeholders})
              AND rutenummer IS NOT NULL
            ORDER BY fotrute_fk, rutenummer
            """,
            segment_ids,
        )
        _print_rows(f"Route rows for link {link_id} segments (stiflyt.fotruteinfo)", fi_rows, limit=200)

        # Compute distinct routes from segments
        routes_from_segments = sorted({str(r["rutenummer"]) for r in fi_rows if r.get("rutenummer") is not None})

        # Find which segments explicitly carry the suspicious route
        segs_with_route = sorted({int(r["segment_id"]) for r in fi_rows if str(r.get("rutenummer")) == route})

        # 4) Node info: prefer anchor_nodes; node_degree is not exposed in stiflyt
        node_rows = _q(
            cur,
            """
            SELECT
              n.node_id,
              an.degree,
              (an.node_id IS NOT NULL) AS is_anchor,
              an.anchor_type,
              an.ruteinfopunkt_objid,
              an.ruteinfopunkt_distance_m
            FROM stiflyt.nodes n
            LEFT JOIN stiflyt.anchor_nodes an ON an.node_id = n.node_id
            WHERE n.node_id = %s
            """,
            (node_id,),
        )
        _print_rows(f"Node {node_id} (stiflyt.nodes + stiflyt.anchor_nodes)", node_rows)

        # 5) Mismatch check: does link list contain a route not present on any segment?
        # Note: ordering differs, so compare as sets
        mismatch = set(map(str, rutenummer_list)) != set(routes_from_segments)

        # Focused error condition (the reported bug):
        # - link shows route in rutenummer_list
        # - BUT no segment has that route in fotruteinfo
        if has_route_in_link_list and not segs_with_route:
            summary = (
                f"ERROR: link {link_id} includes route {route} in rutenummer_list, "
                f"but NO segments in the link have rutenummer={route} in fotruteinfo. "
                f"This strongly suggests an aggregation/view/schema mismatch."
            )
        elif has_route_in_link_list and segs_with_route:
            summary = (
                f"OK (data explains it): link {link_id} includes route {route} because "
                f"segment(s) {segs_with_route} have rutenummer={route} in fotruteinfo."
            )
        elif (not has_route_in_link_list) and segs_with_route:
            summary = (
                f"ERROR: segment(s) {segs_with_route} have rutenummer={route} in fotruteinfo, "
                f"but link {link_id} rutenummer_list does NOT include {route}. "
                f"This suggests the view is stale or linking differs from link_segments."
            )
        else:
            summary = (
                f"No direct evidence for route {route} on link {link_id}: "
                f"route not in rutenummer_list and no segments carry it."
            )

        _print_kv(
            "Derived checks",
            [
                ("route_in_link_rutenummer_list", has_route_in_link_list),
                ("segments_with_route", segs_with_route),
                ("distinct_routes_from_segments", routes_from_segments),
                ("rutenummer_list_on_link", rutenummer_list),
                ("set_mismatch_link_vs_segments", mismatch),
            ],
        )

        print(f"\n==> Diagnosis\n{summary}\n")

        # Exit code conventions:
        # 0 = no error found for this claim
        # 2 = found mismatch / error condition
        return Diagnosis(
            has_route_in_link_list=has_route_in_link_list,
            segments_with_route=segs_with_route,
            route_inferred_from_segments=routes_from_segments,
            mismatch=mismatch,
            summary=summary,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Investigate link 6279 route aggregation bug")
    parser.add_argument("--link-id", type=int, default=6279)
    parser.add_argument("--route", type=str, default="20160407")
    parser.add_argument("--node-id", type=int, default=91705)
    args = parser.parse_args()

    diag = investigate(link_id=args.link_id, route=args.route, node_id=args.node_id)

    # error-ish if mismatch exists or summary starts with ERROR
    if diag.mismatch or diag.summary.startswith("ERROR:"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

