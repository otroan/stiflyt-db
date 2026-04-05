#!/usr/bin/env python3
"""
Pre/post snapshot and diff report for fotrute and fotruteinfo (Kartverket import).

Two modes:
  --after-load  (recommended) Run from update_datasets after load, before migrations.
                Snapshots raw turogfriluftsruter_* tables, diffs against previous
                run's raw snapshot, writes report, saves current raw for next time.
                Segment identity is by geometry hash (not objid), so re-imports with
                new objids are detected as unchanged when geometry is the same.
  --pre / --post (legacy) Snapshot stiflyt views before/after full refresh (identity by objid).

Report includes: added/removed segments, metadata changes (content only; objid and
fotrute_fk excluded from comparison and dumps). Text is written for weekly status
e-mail: short executive summary, Norwegian labels, grouping by route, no raw JSON.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import sql
except ImportError:
    print("Feil: psycopg2 ikke installert. Installer med: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

# Reuse db connection from scripts
scripts_dir = Path(__file__).resolve().parent
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))
from db_status import get_db_connection_params, connect_db

DEFAULT_LOG_DIR = Path("logs")
PRE_SNAPSHOT_FILENAME = "refresh_snapshot_pre.json"

# Fotruteinfo columns used for equality + report dumps (exclude objid, fotrute_fk: change each import)
FOTRUTEINFO_CONTENT_KEYS = frozenset(
    {"rutenavn", "rutenummer", "vedlikeholdsansvarlig", "ruteinformasjon",
     "spesialfotrutetype", "gradering", "rutetype", "rutebetydning", "tilpasning", "objtype"}
)

FOTRUTEINFO_DISPLAY_KEYS = FOTRUTEINFO_CONTENT_KEYS

# Norwegian labels for email-friendly output (weekly digest)
FOTRUTEINFO_FIELD_LABELS: Dict[str, str] = {
    "rutenummer": "Rutenummer",
    "rutenavn": "Rutenavn",
    "vedlikeholdsansvarlig": "Vedlikeholdsansvarlig",
    "ruteinformasjon": "Ruteinformasjon",
    "spesialfotrutetype": "Spesialfotrutetype",
    "gradering": "Gradering",
    "rutetype": "Rutetype",
    "rutebetydning": "Rutebetydning",
    "tilpasning": "Tilpasning",
    "objtype": "Objtype",
}

# Order fields in reports (unknown keys sort last)
_DISPLAY_KEY_ORDER = [
    "rutenummer",
    "rutenavn",
    "vedlikeholdsansvarlig",
    "rutetype",
    "gradering",
    "ruteinformasjon",
    "spesialfotrutetype",
    "rutebetydning",
    "tilpasning",
    "objtype",
]

# Cap per-section detail rows in text report (full data remains in logs/DB)
_MAX_DETAIL_SEGMENTS = 60
# Below this many segments, include a line-by-line technical appendix (email-friendly)
_MAX_FULL_DETAIL = 18


def _json_serial(obj: Any) -> Any:
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "hex"):
        return obj.hex()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _row_to_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in row.items():
        if v is None:
            out[k] = None
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif isinstance(v, (int, float, str, bool)):
            out[k] = v
        elif isinstance(v, (bytes, bytearray)):
            out[k] = "<binary>"
        else:
            out[k] = str(v)
    return out


def table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        return cur.fetchone() is not None


def find_turrutebasen_schema(conn) -> Optional[str]:
    """Return the current turogfriluftsruter_* schema name, or None."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT nspname
            FROM pg_namespace
            WHERE nspname LIKE 'turogfriluftsruter_%'
              AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
            ORDER BY nspname DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        return row[0] if row else None


def _column_exists(conn, schema: str, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
        """, (schema, table, column))
        return cur.fetchone() is not None


def collect_fotrute_snapshot(conn, schema: Optional[str] = None, raw: bool = False) -> List[Dict[str, Any]]:
    """Collect fotrute: objid and, when raw, a stable geom_key (MD5 of geometry) for identity across re-imports."""
    if schema:
        qual = sql.SQL(".").join([sql.Identifier(schema), sql.Identifier("fotrute")])
    else:
        qual = sql.SQL("stiflyt.fotrute")
    if raw and schema and _column_exists(conn, schema, "fotrute", "senterlinje"):
        # Stable identity: same geometry => same segment, regardless of objid change on re-import
        q = sql.SQL("""
            SELECT objid,
                   COALESCE(MD5(ST_AsBinary(senterlinje))::text, 'no-geom-' || objid::text) AS geom_key
            FROM {}
        """).format(qual)
    elif raw:
        q = sql.SQL("SELECT objid, objid::text AS geom_key FROM {}").format(qual)
    else:
        q = sql.SQL("SELECT objid, source_node, target_node FROM {}").format(qual)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q)
        rows = cur.fetchall()
    return [_row_to_dict(r) for r in rows]


def collect_fotruteinfo_snapshot(conn, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """Collect all fotruteinfo columns (metadata)."""
    if schema:
        qual = sql.SQL(".").join([sql.Identifier(schema), sql.Identifier("fotruteinfo")])
    else:
        qual = sql.SQL("stiflyt.fotruteinfo")
    q = sql.SQL("SELECT * FROM {}").format(qual)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q)
        rows = cur.fetchall()
    return [_row_to_dict(r) for r in rows]


def run_pre(database: str, log_dir: Path) -> int:
    """Save pre-refresh snapshot (stiflyt views). Returns 0 on success."""
    log_dir.mkdir(parents=True, exist_ok=True)
    out_path = log_dir / PRE_SNAPSHOT_FILENAME

    db_params = get_db_connection_params()
    db_params["database"] = database
    conn = connect_db(db_params)
    if not conn:
        print("✗ Kunne ikke koble til database for pre-snapshot", file=sys.stderr)
        return 1

    try:
        if not table_exists(conn, "stiflyt", "fotrute") or not table_exists(conn, "stiflyt", "fotruteinfo"):
            print("⊙ stiflyt.fotrute eller stiflyt.fotruteinfo finnes ikke; lagrer tom snapshot", file=sys.stderr)
            snapshot = {"ts": datetime.utcnow().isoformat() + "Z", "fotrute": [], "fotruteinfo": []}
        else:
            snapshot = {
                "ts": datetime.utcnow().isoformat() + "Z",
                "fotrute": collect_fotrute_snapshot(conn, schema=None, raw=False),
                "fotruteinfo": collect_fotruteinfo_snapshot(conn, schema=None),
            }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2, default=_json_serial, ensure_ascii=False)
        print(f"✓ Pre-snapshot lagret: {out_path} ({len(snapshot['fotrute'])} fotrute, {len(snapshot['fotruteinfo'])} fotruteinfo)")
        return 0
    except Exception as e:
        print(f"✗ Pre-snapshot feilet: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def _info_by_segment(fotruteinfo: List[Dict]) -> Dict[int, List[Dict]]:
    by_seg: Dict[int, List[Dict]] = {}
    for row in fotruteinfo:
        fk = row.get("fotrute_fk")
        if fk is not None:
            by_seg.setdefault(int(fk), []).append(row)
    return by_seg


def _fotrute_by_objid(fotrute: List[Dict]) -> Dict[int, Dict]:
    return {int(r["objid"]): r for r in fotrute}


def _fotruteinfo_content_fingerprint(row: Dict) -> Tuple[Any, ...]:
    """Fingerprint for one fotruteinfo row: content only (no objid / fotrute_fk in FOTRUTEINFO_CONTENT_KEYS)."""
    content = {k: row[k] for k in FOTRUTEINFO_CONTENT_KEYS if k in row}
    return (row.get("rutenummer"), json.dumps(content, sort_keys=True, default=str))


def _metadata_equal(a: List[Dict], b: List[Dict]) -> bool:
    """Compare two lists of fotruteinfo rows by content only (ignore objid and other surrogate keys)."""
    sa = sorted((_fotruteinfo_content_fingerprint(r) for r in a))
    sb = sorted((_fotruteinfo_content_fingerprint(r) for r in b))
    return sa == sb


def _fotruteinfo_display_dict(row: Dict) -> Dict[str, Any]:
    """Content-only dict for report display (no surrogate FKs / objid)."""
    return {k: row[k] for k in FOTRUTEINFO_DISPLAY_KEYS if k in row}


def _truncate_text(val: str, max_len: int = 140) -> str:
    s = val.replace("\n", " ").replace("\r", " ").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def _sorted_display_keys(keys: Set[str]) -> List[str]:
    order = {k: i for i, k in enumerate(_DISPLAY_KEY_ORDER)}
    return sorted(keys, key=lambda k: (order.get(k, 999), k))


def _fotruteinfo_human_lines(rows: List[Dict], indent: str = "    ") -> List[str]:
    """Readable key: value lines (Norwegian labels), no JSON."""
    if not rows:
        return [f"{indent}(ingen ruteinfo knyttet til segmentet)"]
    lines: List[str] = []
    for i, row in enumerate(rows, start=1):
        d = _fotruteinfo_display_dict(row)
        if len(rows) > 1:
            lines.append(f"{indent}Ruteinfo-rad {i}:")
            inner = indent + "  "
        else:
            inner = indent
        for k in _sorted_display_keys(set(d.keys())):
            v = d[k]
            if v is None or v == "":
                continue
            label = FOTRUTEINFO_FIELD_LABELS.get(k, k)
            lines.append(f"{inner}{label}: {_truncate_text(str(v))}")
    return lines


def _primary_route_label(info_rows: List[Dict]) -> str:
    """Short label for grouping (e.g. weekly mail)."""
    if not info_rows:
        return "Uten ruteinfo"
    for r in info_rows:
        num = r.get("rutenummer")
        navn = r.get("rutenavn")
        if num and navn:
            return f"{num} – {_truncate_text(str(navn), 60)}"
        if num:
            return str(num)
        if navn:
            return _truncate_text(str(navn), 80)
    return "Ukjent rute"


def _group_keys_by_route(
    keys: List[Any],
    info_for_key: Callable[[Any], List[Dict]],
) -> List[Tuple[str, List[Any]]]:
    buckets: Dict[str, List[Any]] = {}
    for k in keys:
        label = _primary_route_label(info_for_key(k))
        buckets.setdefault(label, []).append(k)
    return sorted(buckets.items(), key=lambda x: x[0].lower())


def _field_level_changes(old_d: Dict[str, Any], new_d: Dict[str, Any]) -> List[str]:
    """Human lines for fields that differ."""
    keys = _sorted_display_keys(set(old_d.keys()) | set(new_d.keys()))
    out: List[str] = []
    for k in keys:
        o, n = old_d.get(k), new_d.get(k)
        if o == n:
            continue
        label = FOTRUTEINFO_FIELD_LABELS.get(k, k)
        o_s = "(tom)" if o is None or o == "" else _truncate_text(str(o), 100)
        n_s = "(tom)" if n is None or n == "" else _truncate_text(str(n), 100)
        out.append(f"      • {label}: «{o_s}» → «{n_s}»")
    return out


def _metadata_change_human_lines(old_info: List[Dict], new_info: List[Dict]) -> List[str]:
    """Describe metadata change without raw JSON."""
    lines: List[str] = []
    old_sorted = sorted(old_info, key=lambda r: _fotruteinfo_content_fingerprint(r))
    new_sorted = sorted(new_info, key=lambda r: _fotruteinfo_content_fingerprint(r))

    if len(old_sorted) != len(new_sorted):
        lines.append(
            f"      Antall ruteinfo-rader knyttet til segmentet: {len(old_sorted)} → {len(new_sorted)}"
        )
        lines.append("      Før:")
        lines.extend(_fotruteinfo_human_lines(old_sorted, indent="        "))
        lines.append("      Etter:")
        lines.extend(_fotruteinfo_human_lines(new_sorted, indent="        "))
        return lines

    for o_row, n_row in zip(old_sorted, new_sorted):
        od = _fotruteinfo_display_dict(o_row)
        nd = _fotruteinfo_display_dict(n_row)
        if od == nd:
            continue
        rn = od.get("rutenummer") or nd.get("rutenummer") or "?"
        fld = _field_level_changes(od, nd)
        if fld:
            lines.append(f"      Endringer (rutenummer {rn}):")
            lines.extend(fld)
        else:
            lines.append(f"      Rutenummer {rn} – full sammenligning:")
            lines.append("        Før:")
            lines.extend(_fotruteinfo_human_lines([o_row], indent="          "))
            lines.append("        Etter:")
            lines.extend(_fotruteinfo_human_lines([n_row], indent="          "))
    return lines


def _segment_identity_key(row: Dict) -> Optional[str]:
    """Stable segment key: geom_key if present (raw mode), else None (use objid)."""
    return row.get("geom_key")


def _build_diff_report(
    pre_fotrute: List[Dict],
    pre_fotruteinfo: List[Dict],
    post_fotrute: List[Dict],
    post_fotruteinfo: List[Dict],
    pre_ts: str,
    report_note: str = "",
) -> List[str]:
    """Build report from pre/post snapshot. Uses geom_key as segment identity when present (raw mode), else objid.
    Excludes objid from all dumps so unchanged content does not appear changed."""
    pre_info = _info_by_segment(pre_fotruteinfo)
    post_info = _info_by_segment(post_fotruteinfo)
    # Use geom_key only when both snapshots have it (pre may be from before we added geom_key)
    use_geom_key = bool(
        pre_fotrute
        and post_fotrute
        and _segment_identity_key(pre_fotrute[0]) is not None
        and _segment_identity_key(post_fotrute[0]) is not None
    )

    if use_geom_key:
        pre_f_by_geom = {r["geom_key"]: r for r in pre_fotrute}
        post_f_by_geom = {r["geom_key"]: r for r in post_fotrute}
        pre_objid_by_geom = {r["geom_key"]: int(r["objid"]) for r in pre_fotrute}
        post_objid_by_geom = {r["geom_key"]: int(r["objid"]) for r in post_fotrute}
        pre_info_by_geom = {
            gk: pre_info.get(pre_objid_by_geom[gk], []) for gk in pre_objid_by_geom
        }
        post_info_by_geom = {
            gk: post_info.get(post_objid_by_geom[gk], []) for gk in post_objid_by_geom
        }
        pre_keys: Set[str] = set(pre_f_by_geom)
        post_keys: Set[str] = set(post_f_by_geom)
        added_keys = post_keys - pre_keys
        removed_keys = pre_keys - post_keys
        common_keys = pre_keys & post_keys
        metadata_changed = [
            (gk, pre_info_by_geom.get(gk, []), post_info_by_geom.get(gk, []))
            for gk in common_keys
            if not _metadata_equal(
                pre_info_by_geom.get(gk, []), post_info_by_geom.get(gk, [])
            )
        ]
        # Sort keys for stable output (geom_key is hex string)
        added_sorted = sorted(added_keys)
        removed_sorted = sorted(removed_keys)
    else:
        pre_f = _fotrute_by_objid(pre_fotrute)
        post_f = _fotrute_by_objid(post_fotrute)
        pre_objids = {int(r["objid"]) for r in pre_fotrute}
        post_objids = {int(r["objid"]) for r in post_fotrute}
        added_objids = post_objids - pre_objids
        removed_objids = pre_objids - post_objids
        common_objids = pre_objids & post_objids
        metadata_changed = [
            (oid, pre_info.get(oid, []), post_info.get(oid, []))
            for oid in common_objids
            if not _metadata_equal(pre_info.get(oid, []), post_info.get(oid, []))
        ]
        added_sorted = sorted(added_objids)
        removed_sorted = sorted(removed_objids)

    unchanged_segments = (
        len(common_keys) - len(metadata_changed)
        if use_geom_key
        else (len(pre_objids & post_objids) - len(metadata_changed))
    )
    has_changes = (
        len(added_sorted) > 0
        or len(removed_sorted) > 0
        or len(metadata_changed) > 0
    )

    n_pre_seg = len(pre_fotrute) if not use_geom_key else len(pre_keys)
    n_post_seg = len(post_fotrute) if not use_geom_key else len(post_keys)
    post_inf = (
        (lambda k: post_info_by_geom.get(k, []))
        if use_geom_key
        else (lambda k: post_info.get(k, []))
    )
    pre_inf = (
        (lambda k: pre_info_by_geom.get(k, []))
        if use_geom_key
        else (lambda k: pre_info.get(k, []))
    )

    report_lines: List[str] = []
    title = "Turrutebasen – endringsrapport"
    if report_note:
        title += f" ({report_note})"
    report_lines.append(title)
    report_lines.append("Sammenligning mot forrige vellykkede import (fotrute / fotruteinfo).")
    report_lines.append("")
    report_lines.append(f"Forrige snapshot:  {pre_ts}")
    report_lines.append(f"Denne kjøringen:   {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC")
    report_lines.append("")
    report_lines.append("-" * 52)

    report_lines.append("")
    report_lines.append("I KORTE TREKK")
    report_lines.append("")
    if not has_changes:
        report_lines.append("  • Ingen innholdsmessige endringer siden forrige gang.")
        report_lines.append(
            f"  • Alle {n_post_seg:,} segmenter matcher forrige import (samme geometri og samme ruteinfo)."
        )
    else:
        if len(added_sorted):
            report_lines.append(
                f"  • {len(added_sorted):,} nye segment(er): ny linjegeometri i Kartverkets leveranse."
            )
        if len(removed_sorted):
            report_lines.append(
                f"  • {len(removed_sorted):,} segment(er) borte: geometri finnes ikke lenger i leveransen."
            )
        if len(metadata_changed):
            report_lines.append(
                f"  • {len(metadata_changed):,} segment(er) har samme geometri, men oppdatert ruteinformasjon "
                f"(navn, beskrivelse, type, vedlikehold, osv.)."
            )
        report_lines.append(
            f"  • {unchanged_segments:,} segment(er) er uendret (geometri og metadata som før)."
        )

    report_lines.append("")
    report_lines.append("TALL")
    report_lines.append("")
    report_lines.append(f"  Segmenter (antall):     {n_pre_seg:,} → {n_post_seg:,}")
    report_lines.append(f"  Ruteinfo-rader (antall): {len(pre_fotruteinfo):,} → {len(post_fotruteinfo):,}")
    report_lines.append("")

    if not has_changes:
        report_lines.append("-" * 52)
        report_lines.append("")
        report_lines.append(
            "Vennlig hilsen / automatisk rapport (kan vedlegges ukentlig status-e-post)."
        )
        return report_lines

    report_lines.append("-" * 52)

    def _append_segment_details(
        keys: List[Any],
        info_fn: Callable[[Any], List[Dict]],
        heading: str,
        f_map: Optional[Dict[Any, Dict]] = None,
    ) -> None:
        if not keys:
            return
        report_lines.append("")
        report_lines.append(heading)
        report_lines.append("")
        report_lines.append(
            "  Fordeling på rute (der ruteinfo finnes på segmentet). "
            "Segmenter uten ruteinfo havner under «Uten ruteinfo»."
        )
        report_lines.append("")
        grouped = _group_keys_by_route(keys, info_fn)
        for route_label, gkeys in grouped:
            report_lines.append(f"  • {route_label}: {len(gkeys)} segment(er)")
        report_lines.append("")

        if grouped:
            first_label, first_keys = grouped[0]
            sample = info_fn(first_keys[0])
            if sample:
                report_lines.append(f"  Eksempel på ruteinfo ({first_label}):")
                report_lines.extend(_fotruteinfo_human_lines(sample, indent="    "))
                report_lines.append("")

        if len(keys) > _MAX_FULL_DETAIL:
            report_lines.append(
                f"  Full segment-for-segment-liste utelatt ({len(keys)} segmenter). "
                f"Bruk database eller loggfiler for detaljert sporbarhet."
            )
            report_lines.append("")
            return

        report_lines.append("  Segment-for-segment (kort teknisk oversikt):")
        report_lines.append("")
        n_list = min(len(keys), _MAX_DETAIL_SEGMENTS)
        for i, key in enumerate(keys[:n_list], start=1):
            if use_geom_key and f_map is not None:
                row = f_map.get(key, {})
                oid = row.get("objid", "?")
                report_lines.append(
                    f"  {i}. Intern sporings-ID {key[:24]}...  (objid i denne importen: {oid})"
                )
            else:
                report_lines.append(f"  {i}. Segment objid {key}")
            for line in _fotruteinfo_human_lines(info_fn(key), indent="     "):
                report_lines.append(line)
            report_lines.append("")
        if len(keys) > n_list:
            report_lines.append(f"  ... og {len(keys) - n_list} til (ikke vist; grense {_MAX_DETAIL_SEGMENTS}).")
            report_lines.append("")

    if added_sorted:
        _append_segment_details(
            added_sorted,
            post_inf,
            "NYE SEGMENTER",
            post_f_by_geom if use_geom_key else None,
        )

    if removed_sorted:
        _append_segment_details(
            removed_sorted,
            pre_inf,
            "FJERNEDE SEGMENTER",
            pre_f_by_geom if use_geom_key else None,
        )

    if metadata_changed:
        report_lines.append("")
        report_lines.append("ENDRET RUTEINFORMASJON (samme segment, ny tekst eller felter)")
        report_lines.append("")
        by_route: Dict[str, List[Tuple[Any, List[Dict], List[Dict]]]] = {}
        for key, old_info, new_info in metadata_changed:
            label = _primary_route_label(new_info or old_info)
            by_route.setdefault(label, []).append((key, old_info, new_info))
        for route_label in sorted(by_route.keys(), key=str.lower):
            items = by_route[route_label]
            report_lines.append(f"  {route_label} ({len(items)} segment(er))")
            report_lines.append("")
        n_meta = min(len(metadata_changed), _MAX_DETAIL_SEGMENTS)
        report_lines.append("  Detaljer per segment:")
        report_lines.append("")
        for idx, (key, old_info, new_info) in enumerate(metadata_changed[:n_meta], start=1):
            if use_geom_key:
                report_lines.append(
                    f"  {idx}. sporings-ID {str(key)[:20]}...  "
                    f"- {_primary_route_label(new_info or old_info)}"
                )
            else:
                report_lines.append(
                    f"  {idx}. segment objid {key}  - {_primary_route_label(new_info or old_info)}"
                )
            report_lines.extend(_metadata_change_human_lines(old_info, new_info))
            report_lines.append("")
        if len(metadata_changed) > n_meta:
            report_lines.append(
                f"  ... og {len(metadata_changed) - n_meta} segment(er) til med metadataendring (ikke vist)."
            )
            report_lines.append("")

    report_lines.append("-" * 52)
    report_lines.append("")
    report_lines.append(
        "Vennlig hilsen / automatisk rapport (kan vedlegges ukentlig status-e-post)."
    )
    return report_lines


def run_post(database: str, log_dir: Path) -> int:
    """Load pre snapshot, take post snapshot (stiflyt), diff, write report. Returns 0 on success."""
    log_dir.mkdir(parents=True, exist_ok=True)
    pre_path = log_dir / PRE_SNAPSHOT_FILENAME
    if not pre_path.exists():
        print("✗ Pre-snapshot ikke funnet:", pre_path, file=sys.stderr)
        return 1

    with open(pre_path, encoding="utf-8") as f:
        pre = json.load(f)

    db_params = get_db_connection_params()
    db_params["database"] = database
    conn = connect_db(db_params)
    if not conn:
        print("✗ Kunne ikke koble til database for post-snapshot", file=sys.stderr)
        return 1

    try:
        if not table_exists(conn, "stiflyt", "fotrute") or not table_exists(conn, "stiflyt", "fotruteinfo"):
            print("✗ stiflyt.fotrute eller stiflyt.fotruteinfo finnes ikke etter refresh", file=sys.stderr)
            return 1
        post_fotrute = collect_fotrute_snapshot(conn, schema=None, raw=False)
        post_fotruteinfo = collect_fotruteinfo_snapshot(conn, schema=None)
    finally:
        conn.close()

    pre_fotrute = pre.get("fotrute", [])
    pre_fotruteinfo = pre.get("fotruteinfo", [])
    pre_ts = pre.get("ts", "?")
    report_lines = _build_diff_report(
        pre_fotrute, pre_fotruteinfo, post_fotrute, post_fotruteinfo, pre_ts
    )
    report_path = log_dir / f"refresh_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"✓ Rapport skrevet: {report_path}")
    return 0


def run_after_load(
    database: str,
    log_dir: Path,
    log_fn: Optional[Callable[[str], None]] = None,
) -> int:
    """
    Run after load, before migrations: snapshot raw turogfriluftsruter_* tables,
    diff against previous run's raw snapshot, write report, save current raw for next time.
    """
    out = log_fn if log_fn else lambda msg: print(msg)
    log_dir = Path(log_dir).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    pre_path = log_dir / PRE_SNAPSHOT_FILENAME

    db_params = get_db_connection_params()
    db_params["database"] = database
    conn = connect_db(db_params)
    if not conn:
        out("  ✗ Kunne ikke koble til database for diff-snapshot")
        return 1

    try:
        schema = find_turrutebasen_schema(conn)
        if not schema:
            out("  ⊙ Ingen turogfriluftsruter_* schema funnet – hopper over diff")
            return 0
        if not table_exists(conn, schema, "fotrute") or not table_exists(conn, schema, "fotruteinfo"):
            out("  ⊙ fotrute/fotruteinfo finnes ikke i raw schema – hopper over diff")
            return 0

        post_fotrute = collect_fotrute_snapshot(conn, schema=schema, raw=True)
        post_fotruteinfo = collect_fotruteinfo_snapshot(conn, schema=schema)
    finally:
        conn.close()

    pre_fotrute = []
    pre_fotruteinfo = []
    pre_ts = "ingen"
    pre_snapshot_meta = None
    if pre_path.exists():
        try:
            with open(pre_path, encoding="utf-8") as f:
                pre_snapshot_meta = json.load(f)
            pre_fotrute = pre_snapshot_meta.get("fotrute", [])
            pre_fotruteinfo = pre_snapshot_meta.get("fotruteinfo", [])
            pre_ts = pre_snapshot_meta.get("ts", "?")
        except Exception as e:
            out(f"  ⚠ Kunne ikke lese forrige snapshot: {e}")
            pre_snapshot_meta = None

    if pre_fotrute or pre_fotruteinfo or post_fotrute or post_fotruteinfo:
        report_lines = _build_diff_report(
            pre_fotrute, pre_fotruteinfo, post_fotrute, post_fotruteinfo, pre_ts,
            report_note="rådata (før migrasjoner)",
        )
        report_path = log_dir / f"refresh_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
        report_path.write_text("\n".join(report_lines), encoding="utf-8")
        out(f"  ✓ Diff-rapport skrevet: {report_path}")

    snapshot = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "schema": schema,
        "fotrute": post_fotrute,
        "fotruteinfo": post_fotruteinfo,
    }
    with open(pre_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, default=_json_serial, ensure_ascii=False)
    out(f"  ✓ Raw snapshot lagret for neste kjøring ({len(post_fotrute)} fotrute, {len(post_fotruteinfo)} fotruteinfo)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Pre/post snapshot og diff-rapport for fotrute/fotruteinfo")
    parser.add_argument("--pre", action="store_true", help="Lagre pre-snapshot (stiflyt) før refresh")
    parser.add_argument("--post", action="store_true", help="Ta post-snapshot (stiflyt), diff, skriv rapport")
    parser.add_argument("--after-load", action="store_true", help="Rådata-snapshot etter load, før migrasjoner")
    parser.add_argument("database", nargs="?", default=None, help="Database (default: PGDATABASE eller matrikkel)")
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR, help="Mappe for snapshot og rapport")
    args = parser.parse_args()

    database = args.database or os.environ.get("PGDATABASE", "matrikkel")
    log_dir = args.log_dir.resolve()

    modes = sum([args.pre, args.post, args.after_load])
    if modes > 1:
        print("Bruk kun ett av --pre, --post, --after-load", file=sys.stderr)
        return 1
    if modes == 0:
        print("Angi --pre, --post eller --after-load", file=sys.stderr)
        return 1

    if args.after_load:
        return run_after_load(database, log_dir)
    if args.pre:
        return run_pre(database, log_dir)
    return run_post(database, log_dir)


if __name__ == "__main__":
    sys.exit(main())
