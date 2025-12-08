#!/usr/bin/env python3
"""
Download PostGIS and other format datasets from Kartverket/Geonorge API.

This script uses ATOM feeds from nedlasting.geonorge.no to download various
datasets. It can discover datasets automatically from the Tjenestefeed.xml catalog
or use predefined dataset names.

Usage:
    # List all available datasets
    python3 scripts/download_kartverket.py --list-datasets

    # View download options for a specific dataset (EPSG codes, areas, etc.)
    python3 scripts/download_kartverket.py --list-datasets --dataset "Matrikkelen - Eiendomskart Teig"

    # View only Norge (nationwide) options and generate config file output
    python3 scripts/download_kartverket.py --list-datasets --dataset "Matrikkelen - Eiendomskart Teig" --norge-only --config-output

    # Download a specific dataset
    python3 scripts/download_kartverket.py [dataset_name] [output_directory]

    # Download with explicit ATOM feed URL
    python3 scripts/download_kartverket.py --feed-url <URL> [output_directory]

    # Specify format preference (for datasets with multiple formats)
    python3 scripts/download_kartverket.py --format PostGIS,GML stedsnavn

    # Batch download from configuration file
    python3 scripts/download_kartverket.py --config datasets.yaml

Supported dataset names (examples):
    - teig (or matrikkel-teig): Matrikkelen Eiendomskart Teig (PostGIS)
    - turrutebasen: Turrutebasen (PostGIS)
    - stedsnavn: Stedsnavn (GML format, PostGIS not available)

The script automatically discovers datasets from the Geonorge catalog feed, so
you can use any dataset name that appears in --list-datasets output.

Examples:
    # List all available datasets
    python3 scripts/download_kartverket.py --list-datasets

    # View download options for a specific dataset
    python3 scripts/download_kartverket.py --list-datasets --dataset "Matrikkelen - Eiendomskart Teig"

    # Download teig dataset
    python3 scripts/download_kartverket.py teig

    # Download turrutebasen to custom directory
    python3 scripts/download_kartverket.py turrutebasen ./data/turrutebasen

    # Download stedsnavn (will use GML format automatically)
    python3 scripts/download_kartverket.py stedsnavn

    # Use custom feed URL
    python3 scripts/download_kartverket.py --feed-url https://.../Feed.xml ./data/custom

Environment variables:
    UTM_ZONE      - UTM projection zone (default: 25833)
                    Options: 25832, 25833, 25835
    AREA_FILTER   - Filter by area name (optional, e.g., "Norge", "Oslo")
                    Default: "Norge" (landsdekkende/nationwide)
    AREA_TYPE     - Filter by area type (optional, e.g., "Fylke", "Kommune")
                    Default: empty (matches any type)
"""

import os
import sys
import argparse
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any

# Try to import yaml for config file support
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


# Master catalog feed that lists all available datasets
TJENESTEFEED_URL = "https://nedlasting.geonorge.no/geonorge/Tjenestefeed.xml"

# Default dataset configurations (can be overridden by discovery)
DATASET_FEEDS: Dict[str, str] = {
    'teig': 'http://nedlasting.geonorge.no/fmedatastreaming/ATOM-feeds/MatrikkelenEiendomskartTeig_AtomFeedPostGIS.fmw?token=13f1a2e9c53d2ba77b527954b767e563213aaf3b',
    'matrikkel-teig': 'http://nedlasting.geonorge.no/fmedatastreaming/ATOM-feeds/MatrikkelenEiendomskartTeig_AtomFeedPostGIS.fmw?token=13f1a2e9c53d2ba77b527954b767e563213aaf3b',
    'turrutebasen': 'http://nedlasting.geonorge.no/fmedatastreaming/ATOM-feeds/TurOgFriluftsruter_AtomFeedPostGIS.fmw?token=13f1a2e9c53d2ba77b527954b767e563213aaf3b',
    'stedsnavn': 'http://nedlasting.geonorge.no/fmedatastreaming/ATOM-feeds/Stedsnavn_AtomFeedFGDB.fmw?token=13f1a2e9c53d2ba77b527954b767e563213aaf3b',  # FGDB format (no PostGIS available)
}

# Format preferences for datasets (PostGIS preferred, fallback to FGDB/GML)
DATASET_FORMAT_PREFERENCE: Dict[str, List[str]] = {
    'stedsnavn': ['PostGIS', 'FGDB', 'GML'],  # PostGIS not available, use FGDB
    'default': ['PostGIS', 'FGDB', 'GML', 'SOSI'],
}

# Default output directories for each dataset
DEFAULT_OUTPUT_DIRS: Dict[str, str] = {
    'teig': './data/matrikkel',
    'matrikkel-teig': './data/matrikkel',
    'turrutebasen': './data/turrutebasen',
    'stedsnavn': './data/stedsnavn',
}
NAMESPACES = {
    'atom': 'http://www.w3.org/2005/Atom',
    'georss': 'http://www.georss.org/georss',
    'gml': 'http://www.opengis.net/gml'
}


def discover_feeds_from_catalog(
    dataset_name: Optional[str] = None,
    format_preference: Optional[List[str]] = None
) -> Dict[str, Tuple[str, str]]:
    """Discover ATOM feed URLs from Tjenestefeed.xml catalog.

    Args:
        dataset_name: Optional dataset name to search for (case-insensitive partial match)
        format_preference: List of format preferences (e.g., ['PostGIS', 'FGDB'])

    Returns:
        Dictionary mapping dataset titles to (feed_url, format_type) tuples
    """
    if format_preference is None:
        if dataset_name:
            format_preference = DATASET_FORMAT_PREFERENCE.get(
                dataset_name.lower(),
                DATASET_FORMAT_PREFERENCE.get('default', ['PostGIS', 'FGDB', 'GML'])
            )
        else:
            format_preference = DATASET_FORMAT_PREFERENCE.get('default', ['PostGIS', 'FGDB', 'GML'])

    try:
        req = urllib.request.Request(TJENESTEFEED_URL)
        with urllib.request.urlopen(req, timeout=30) as response:
            xml_data = response.read()
        root = ET.fromstring(xml_data)
    except Exception as e:
        print(f"Advarsel: Kunne ikke hente katalogfeed: {e}", file=sys.stderr)
        return {}

    feeds = {}
    dataset_name_lower = dataset_name.lower() if dataset_name else None

    for entry in root.findall('.//atom:entry', NAMESPACES):
        title_elem = entry.find('.//atom:title', NAMESPACES)
        title = title_elem.text if title_elem is not None else ""

        # Filter by dataset name if provided
        if dataset_name_lower and dataset_name_lower not in title.lower():
            continue

        # Find alternate link (the actual feed URL)
        for link in entry.findall('.//atom:link[@rel="alternate"]', NAMESPACES):
            href = link.get('href', '')
            if not href:
                continue

            # Determine format from URL or title
            format_type = None
            for fmt in format_preference:
                if fmt.upper() in href.upper() or fmt.upper() in title.upper():
                    format_type = fmt
                    break

            if format_type:
                # Use first match for each title (preferred format)
                if title not in feeds:
                    feeds[title] = (href, format_type)
                else:
                    # Prefer formats earlier in preference list
                    current_format = feeds[title][1]
                    if format_preference.index(format_type) < format_preference.index(current_format):
                        feeds[title] = (href, format_type)
                break

    return feeds


def list_dataset_download_options(
    dataset_name: str,
    norge_only: bool = False,
    config_output: bool = False
) -> None:
    """List download options for a specific dataset."""
    print(f"==> Søker etter '{dataset_name}' i katalog...")

    # Find the dataset feed URL
    feeds = discover_feeds_from_catalog(dataset_name)

    if not feeds:
        print(f"Feil: Fant ikke dataset '{dataset_name}' i katalog.", file=sys.stderr)
        print("Kjør med --list-datasets for å se tilgjengelige datasett.", file=sys.stderr)
        sys.exit(1)

    # Get the feed URL (use first match)
    title, (feed_url, format_type) = next(iter(feeds.items()))

    print(f"  ✓ Fant: {title} ({format_type} format)")
    print(f"\n==> Henter nedlastingsalternativer fra feed...")

    # Fetch the dataset's ATOM feed
    try:
        root = fetch_atom_feed(feed_url)
    except Exception as e:
        print(f"Feil: Kunne ikke hente feed: {e}", file=sys.stderr)
        sys.exit(1)

    # Extract information from entries
    all_entries = root.findall('.//atom:entry', NAMESPACES)

    if not all_entries:
        print("  Ingen nedlastingsalternativer funnet i feeden.")
        return

    # Filter for Norge-only entries if requested
    entries = []
    norge_entries = []

    for entry in all_entries:
        title_elem = entry.find('.//atom:title', NAMESPACES)
        title_text = title_elem.text if title_elem is not None else ""

        # Check if this is a Norge (nationwide) entry
        is_norge = (
            "landsdekkende" in title_text.lower() or
            "norge" in title_text.lower() or
            any("/0000_Norge_" in link.get('href', '') or
                "Basisdata_0000_Norge" in link.get('href', '')
                for link in entry.findall('.//atom:link', NAMESPACES))
        )

        if is_norge:
            norge_entries.append(entry)

        if not norge_only or is_norge:
            entries.append(entry)

    if norge_only and not norge_entries:
        print("  Ingen landsdekkende (Norge) filer funnet i feeden.")
        return

    # Collect EPSG codes, area types, and sample URLs
    epsg_codes = set()
    area_types = set()
    area_names = set()
    sample_urls = []
    norge_epsg_codes = set()

    for entry in entries:
        # Extract EPSG codes
        categories = entry.findall('.//atom:category', NAMESPACES)
        for category in categories:
            term = category.get('term', '')
            if term.startswith('EPSG:'):
                epsg_codes.add(term)
                # Track EPSG codes for Norge entries
                title_elem = entry.find('.//atom:title', NAMESPACES)
                title_text = title_elem.text if title_elem is not None else ""
                if "landsdekkende" in title_text.lower() or "norge" in title_text.lower():
                    norge_epsg_codes.add(term)
            label = category.get('label', '')
            if label:
                # Try to identify area types (Fylke, Kommune, etc.)
                if any(t in label for t in ['Fylke', 'Kommune', 'Grunnkrets']):
                    area_types.add(label)
                # Collect area names
                if label and label not in ['EPSG/0/25832', 'EPSG/0/25833', 'EPSG/0/25835']:
                    area_names.add(label)

        # Extract download URL
        for link in entry.findall('.//atom:link', NAMESPACES):
            if link.get('rel') == 'alternate':
                url = link.get('href', '')
                if url and len(sample_urls) < 3:
                    sample_urls.append(url)
                break

        # Extract title for area names
        title_elem = entry.find('.//atom:title', NAMESPACES)
        if title_elem is not None:
            title_text = title_elem.text or ""
            # Look for area names in title
            if 'Norge' in title_text:
                area_names.add('Norge')

    # Display results
    filter_note = " (kun landsdekkende/Norge)" if norge_only else ""
    print(f"\n=== Nedlastingsalternativer for {title}{filter_note} ===\n")

    total_count = len(norge_entries) if norge_only else len(all_entries)
    filtered_count = len(entries)
    print(f"Totalt antall filer i feeden: {total_count}")
    if norge_only:
        print(f"Filtrert til landsdekkende filer: {filtered_count}")
    print()

    # Use Norge-specific EPSG codes if filtering
    display_epsg = sorted(norge_epsg_codes) if norge_only and norge_epsg_codes else sorted(epsg_codes)

    if display_epsg:
        epsg_label = "Tilgjengelige EPSG-koder for Norge" if norge_only else "Tilgjengelige EPSG-koder"
        print(f"{epsg_label} ({len(display_epsg)}):")
        for epsg in display_epsg:
            print(f"  • {epsg}")
        print()

    if area_types and not norge_only:
        print(f"Områdetyper ({len(area_types)}):")
        for area_type in sorted(area_types):
            print(f"  • {area_type}")
        print()

    if area_names and not norge_only:
        print(f"Eksempler på områdenavn ({min(len(area_names), 20)} av {len(area_names)}):")
        for area_name in sorted(list(area_names))[:20]:
            print(f"  • {area_name}")
        if len(area_names) > 20:
            print(f"  ... og {len(area_names) - 20} flere")
        print()

    if sample_urls:
        print("Eksempel på nedlastings-URLer:")
        for i, url in enumerate(sample_urls, 1):
            filename = os.path.basename(urllib.parse.urlparse(url).path)
            print(f"  {i}. {filename}")
            print(f"     {url[:80]}..." if len(url) > 80 else f"     {url}")
        print()

    # Generate config output if requested
    if config_output:
        print("=== Konfigurasjonsfil (YAML) ===")
        print("# Legg til i din konfigurasjonsfil:\n")

        # Create a safe dataset key name
        dataset_key = dataset_name.lower().replace(' ', '_').replace('-', '_')
        dataset_key = ''.join(c for c in dataset_key if c.isalnum() or c == '_')

        config_lines = [f"- name: {dataset_key}"]
        config_lines.append(f"  dataset: \"{title}\"")
        config_lines.append(f"  format: {format_type}")

        if display_epsg:
            # Default to most common EPSG (25833)
            default_epsg = "25833" if "25833" in [e.split(':')[1] for e in display_epsg] else display_epsg[0].split(':')[1]
            config_lines.append(f"  utm_zone: {default_epsg}  # Tilgjengelige: {', '.join([e.split(':')[1] for e in display_epsg])}")

        if norge_only:
            config_lines.append(f"  area_filter: Norge  # Kun landsdekkende data")
        else:
            config_lines.append(f"  area_filter: Norge  # Eller spesifiser område (f.eks. Oslo, Akershus)")

        config_lines.append(f"  # area_type:  # Valgfritt: Fylke, Kommune, etc.")
        config_lines.append(f"  output_dir: ./data/{dataset_key}")

        print("\n".join(config_lines))
        print()

    print("Bruk:")
    if norge_only:
        print(f"  UTM_ZONE=25833 AREA_FILTER=Norge python3 scripts/download_kartverket.py \"{title}\"")
    else:
        print(f"  python3 scripts/download_kartverket.py \"{title}\"")
        print("\nFor kun landsdekkende data:")
        print(f"  UTM_ZONE=25833 AREA_FILTER=Norge python3 scripts/download_kartverket.py \"{title}\"")


def list_available_datasets(
    format_filter: Optional[str] = None,
    dataset_name: Optional[str] = None,
    norge_only: bool = False,
    config_output: bool = False
) -> None:
    """List all available datasets from the catalog, or show options for a specific dataset."""
    # If dataset name is provided, show download options for that dataset
    if dataset_name:
        list_dataset_download_options(dataset_name, norge_only, config_output)
        return

    # Otherwise, list all datasets
    print("==> Henter liste over tilgjengelige datasett fra katalog...")

    feeds = discover_feeds_from_catalog()

    if not feeds:
        print("Feil: Kunne ikke hente datasettliste fra katalog.", file=sys.stderr)
        sys.exit(1)

    # Group by format
    by_format: Dict[str, List[Tuple[str, str]]] = {}
    for title, (url, fmt) in feeds.items():
        if format_filter and format_filter.upper() not in fmt.upper():
            continue
        if fmt not in by_format:
            by_format[fmt] = []
        by_format[fmt].append((title, url))

    print(f"\nFant {len(feeds)} datasett:\n")

    for fmt in sorted(by_format.keys()):
        print(f"=== {fmt} format ({len(by_format[fmt])} datasett) ===")
        for title, url in sorted(by_format[fmt]):
            print(f"  • {title}")
        print()


def fetch_atom_feed(url: str) -> ET.Element:
    """Fetch and parse the ATOM feed."""
    print("==> Henter ATOM feed fra Kartverket ...")
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as response:
            xml_data = response.read()
        root = ET.fromstring(xml_data)
        print("  ✓ ATOM feed hentet")
        return root
    except Exception as e:
        print(f"Feil: Kunne ikke hente ATOM feed fra {url}: {e}", file=sys.stderr)
        sys.exit(1)


def extract_download_urls(
    root: ET.Element,
    utm_zone: str,
    area_filter: Optional[str] = None,
    area_type: Optional[str] = None
) -> List[Tuple[str, Optional[str]]]:
    """Extract download URLs and their last updated timestamps from ATOM feed.

    Returns:
        List of (url, updated_url, updated_timestamp )
    """
    """Extract download URLs from ATOM feed entries matching criteria."""
    urls = []

    # Find all entries
    entries = root.findall('.//atom:entry', NAMESPACES)

    for entry in entries:
        # Check if entry has the EPSG code we want
        categories = entry.findall('.//atom:category', NAMESPACES)
        has_epsg = False

        for category in categories:
            term = category.get('term', '')
            if term == f'EPSG:{utm_zone}':
                has_epsg = True
                break

        if not has_epsg:
            continue

        # Get the link with rel="alternate"
        links = entry.findall('.//atom:link', NAMESPACES)
        download_url = None

        for link in links:
            if link.get('rel') == 'alternate':
                download_url = link.get('href')
                break

        if not download_url:
            continue

        # Apply area filter if specified
        if area_filter:
            # Get title and categories for matching
            title_elem = entry.find('.//atom:title', NAMESPACES)
            title = title_elem.text if title_elem is not None else ""

            category_labels = [cat.get('label', '') for cat in categories]
            all_text = f"{title} {' '.join(category_labels)}"

            # Special handling for "Norge" - only match the nationwide entry
            if area_filter == "Norge":
                # Check if this is the nationwide entry
                # Must have "0000_Norge" in URL (the nationwide code) OR title contains "Landsdekkende"
                is_nationwide = (
                    "landsdekkende" in title.lower() or
                    "/0000_Norge_" in download_url or
                    "Basisdata_0000_Norge" in download_url
                )
                if not is_nationwide:
                    continue
            else:
                # For other area filters, check if the filter text appears
                if area_filter.lower() not in all_text.lower():
                    # Also check URL as fallback
                    if area_filter.lower() not in download_url.lower():
                        continue

            # Check area type if specified
            if area_type:
                if not any(area_type.lower() in label.lower() for label in category_labels):
                    continue

        # Get updated timestamp from entry
        updated_elem = entry.find('.//atom:updated', NAMESPACES)
        updated_timestamp = updated_elem.text if updated_elem is not None else None

        urls.append((download_url, updated_timestamp))

    # Remove duplicates (keep first occurrence) and sort by URL
    seen = set()
    unique_urls = []
    for url, timestamp in urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append((url, timestamp))

    return sorted(unique_urls, key=lambda x: x[0])  # Sort by URL


def parse_iso_timestamp(timestamp_str: Optional[str]) -> Optional[float]:
    """Parse ISO 8601 timestamp to Unix timestamp."""
    if not timestamp_str:
        return None
    try:
        from datetime import datetime
        # Handle ISO 8601 format with timezone
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return dt.timestamp()
    except Exception:
        return None


def verify_existing_file(url: str, file_path: Path, feed_updated: Optional[str] = None) -> Tuple[bool, Optional[int], bool]:
    """Verify if an existing file is complete and matches server size.

    Returns:
        (is_valid, expected_size) - is_valid indicates if file is complete,
                                     expected_size is the server's file size or None
    """
    try:
        # Make a HEAD request to get file size without downloading
        req = urllib.request.Request(url, method='HEAD')
        req.add_header('User-Agent', 'Mozilla/5.0')

        with urllib.request.urlopen(req, timeout=30) as response:
            content_length = response.headers.get('Content-Length')
            expected_size = int(content_length) if content_length else None

            if expected_size:
                actual_size = file_path.stat().st_size
                if actual_size == expected_size:
                    # Size matches, verify it's a valid ZIP
                    if check_zip_integrity(file_path):
                        # Check if file is up to date with feed
                        is_up_to_date = True
                        if feed_updated:
                            feed_timestamp = parse_iso_timestamp(feed_updated)
                            file_timestamp = file_path.stat().st_mtime
                            if feed_timestamp and feed_timestamp > file_timestamp:
                                is_up_to_date = False
                        return (True, expected_size, is_up_to_date)
                    else:
                        return (False, expected_size, False)  # Size matches but ZIP is invalid
                else:
                    return (False, expected_size, False)  # Size mismatch
            else:
                # No Content-Length header, just check if ZIP is valid
                is_valid = check_zip_integrity(file_path)
                is_up_to_date = True
                if feed_updated and is_valid:
                    feed_timestamp = parse_iso_timestamp(feed_updated)
                    file_timestamp = file_path.stat().st_mtime
                    if feed_timestamp and feed_timestamp > file_timestamp:
                        is_up_to_date = False
                return (is_valid, None, is_up_to_date)

    except Exception:
        # If we can't verify, assume it might be invalid
        return (False, None, False)


def check_zip_integrity(zip_path: Path) -> bool:
    """Check if ZIP file appears to be complete."""
    if zip_path.stat().st_size == 0:
        return False

    # Try to read the end of the file for ZIP signature
    try:
        with open(zip_path, 'rb') as f:
            f.seek(-22, 2)  # ZIP end-of-central-directory is typically last 22 bytes
            end_bytes = f.read()
            # Check for ZIP end-of-central-directory signature (0x06054b50)
            if b'PK\x05\x06' in end_bytes:
                return True
    except Exception:
        pass

    return False


def download_file(url: str, output_path: Path, max_retries: int = 3) -> bool:
    """Download a file from URL to output path using streaming with progress and retries."""
    for attempt in range(max_retries):
        if attempt > 0:
            print(f"     Forsøk {attempt + 1}/{max_retries}...")

        try:
            req = urllib.request.Request(url)
            # Add headers to support range requests and large files
            req.add_header('User-Agent', 'Mozilla/5.0')
            req.add_header('Accept-Encoding', 'identity')  # Disable compression for large files

            # Use a longer timeout for large files (60 minutes)
            with urllib.request.urlopen(req, timeout=3600) as response:
                # Get file size if available for progress indication
                content_length = response.headers.get('Content-Length')
                total_size = int(content_length) if content_length else None

                downloaded = 0
                chunk_size = 1024 * 1024  # 1MB chunks for better performance

                with open(output_path, 'wb', buffering=8 * 1024 * 1024) as f:  # 8MB buffer
                    while True:
                        try:
                            chunk = response.read(chunk_size)
                            if not chunk:
                                break
                            f.write(chunk)
                            downloaded += len(chunk)

                            # Print progress every 50MB to minimize overhead
                            if downloaded % (50 * 1024 * 1024) == 0:
                                if total_size:
                                    percent = (downloaded / total_size) * 100
                                    print(f"     ... {percent:.1f}% ({format_size(downloaded)}/{format_size(total_size)})", end='\r', flush=True)
                                else:
                                    print(f"     ... {format_size(downloaded)} nedlastet", end='\r', flush=True)

                        except Exception as e:
                            print(f"\n     ✗ Feil under nedlasting: {e}", file=sys.stderr)
                            if attempt < max_retries - 1:
                                print(f"     Prøver på nytt (forsøk {attempt + 2}/{max_retries})...", file=sys.stderr)
                                if output_path.exists():
                                    output_path.unlink()  # Remove partial download
                                break  # Break inner loop to retry
                            return False

                # Verify download completed
                if total_size and downloaded != total_size:
                    print(f"\n     ✗ Nedlasting ufullstendig: {format_size(downloaded)} / {format_size(total_size)}", file=sys.stderr)
                    if attempt < max_retries - 1:
                        print(f"     Prøver på nytt (forsøk {attempt + 2}/{max_retries})...", file=sys.stderr)
                        if output_path.exists():
                            output_path.unlink()  # Remove incomplete file
                        continue  # Retry
                    output_path.unlink()  # Remove incomplete file
                    return False

                print()  # New line after progress
                return True

        except urllib.error.HTTPError as e:
            print(f"     ✗ HTTP feil ved nedlasting: {e.code} {e.reason}", file=sys.stderr)
            if attempt < max_retries - 1:
                print(f"     Prøver på nytt (forsøk {attempt + 2}/{max_retries})...", file=sys.stderr)
                if output_path.exists():
                    output_path.unlink()
                continue
            return False
        except urllib.error.URLError as e:
            print(f"     ✗ URL feil ved nedlasting: {e.reason}", file=sys.stderr)
            if attempt < max_retries - 1:
                print(f"     Prøver på nytt (forsøk {attempt + 2}/{max_retries})...", file=sys.stderr)
                if output_path.exists():
                    output_path.unlink()
                continue
            return False
        except Exception as e:
            print(f"     ✗ Feil ved nedlasting: {e}", file=sys.stderr)
            if attempt < max_retries - 1:
                print(f"     Prøver på nytt (forsøk {attempt + 2}/{max_retries})...", file=sys.stderr)
                if output_path.exists():
                    output_path.unlink()
                continue
            return False

    return False  # All retries exhausted


def format_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def process_download_urls(
    urls: List[Tuple[str, Optional[str]]],
    output_dir: Path,
    dataset_name: str,
    utm_zone: str,
    format_type: str = "PostGIS"
) -> int:
    """Process list of URLs, download if needed, return download count.

    Args:
        urls: List of (url, feed_updated_timestamp) tuples
        output_dir: Directory to save files
        dataset_name: Name of dataset (for filename generation)
        utm_zone: UTM zone (for filename generation)
        format_type: Format type (for filename generation)

    Returns:
        Number of files downloaded
    """
    download_count = 0

    for url, feed_updated in urls:
        # Generate filename from URL
        filename = os.path.basename(urllib.parse.urlparse(url).path)
        if not filename or filename == "/":
            # Fallback: generate generic filename
            dataset_suffix = dataset_name.replace('-', '_')
            filename = f"{dataset_suffix}_{format_type}_{utm_zone}.zip"

        output_path = output_dir / filename

        # Check if file already exists and verify it's complete and up to date
        if output_path.exists():
            print(f"  ⊙ {filename} (eksisterer, verifiserer ...)")
            is_valid, expected_size, is_up_to_date = verify_existing_file(url, output_path, feed_updated)

            if is_valid and is_up_to_date:
                file_size = format_size(output_path.stat().st_size)
                print(f"     ✓ Fil er komplett og oppdatert ({file_size})")
                download_count += 1
                continue
            elif is_valid and not is_up_to_date:
                file_size = format_size(output_path.stat().st_size)
                print(f"     ⊙ Fil er komplett men utdatert ({file_size})")
                if feed_updated:
                    print(f"     Feed oppdatert: {feed_updated}")
                print("     Sletter og laster ned ny versjon ...")
                output_path.unlink()
            else:
                if expected_size:
                    actual_size = output_path.stat().st_size
                    print(f"     ✗ Fil er ufullstendig ({format_size(actual_size)} / {format_size(expected_size)})")
                else:
                    print("     ✗ Fil ser ut til å være korrupt")
                print("     Sletter og laster ned på nytt ...")
                output_path.unlink()

        print(f"  -> {filename}")
        if download_file(url, output_path):
            download_count += 1
            file_size = format_size(output_path.stat().st_size)
            print(f"     ✓ Nedlastet ({file_size})")
        else:
            # Remove partial download on error
            if output_path.exists():
                output_path.unlink()

    return download_count


def get_atom_feed_url(
    dataset_name: Optional[str] = None,
    feed_url: Optional[str] = None,
    format_preference: Optional[List[str]] = None
) -> Tuple[str, Optional[str]]:
    """Get ATOM feed URL from dataset name or provided URL.

    Returns:
        Tuple of (feed_url, format_type)
    """
    if feed_url:
        # Try to determine format from URL
        format_type = None
        for fmt in ['PostGIS', 'FGDB', 'GML', 'SOSI']:
            if fmt.upper() in feed_url.upper():
                format_type = fmt
                break
        return feed_url, format_type

    if dataset_name:
        dataset_name_lower = dataset_name.lower()

        # Check hardcoded feeds first
        if dataset_name_lower in DATASET_FEEDS:
            url = DATASET_FEEDS[dataset_name_lower]
            # Determine format from URL
            format_type = None
            for fmt in ['PostGIS', 'FGDB', 'GML', 'SOSI']:
                if fmt.upper() in url.upper():
                    format_type = fmt
                    break
            return url, format_type

        # Try discovery from catalog
        if format_preference is None:
            format_preference = DATASET_FORMAT_PREFERENCE.get(
                dataset_name_lower,
                DATASET_FORMAT_PREFERENCE.get('default', ['PostGIS', 'FGDB', 'GML'])
            )

        print(f"==> Søker etter '{dataset_name}' i katalog...")
        discovered_feeds = discover_feeds_from_catalog(dataset_name, format_preference)

        if discovered_feeds:
            # Use first match (preferred format)
            title, (url, fmt) = next(iter(discovered_feeds.items()))
            print(f"  ✓ Fant: {title} ({fmt} format)")
            return url, fmt
        else:
            print(f"Feil: Fant ikke dataset '{dataset_name}' i katalog", file=sys.stderr)
            print(f"Kjør med --list-datasets for å se tilgjengelige datasett", file=sys.stderr)
            sys.exit(1)

    # Default to teig for backward compatibility
    url = DATASET_FEEDS['teig']
    format_type = 'PostGIS' if 'PostGIS' in url.upper() else None
    return url, format_type


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments using argparse."""
    parser = argparse.ArgumentParser(
        description='Download PostGIS and other format datasets from Kartverket/Geonorge API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all available datasets
  %(prog)s --list-datasets

  # View download options for a specific dataset
  %(prog)s --list-datasets --dataset "Matrikkelen - Eiendomskart Teig"

  # Download teig dataset
  %(prog)s teig

  # Download with custom output directory
  %(prog)s turrutebasen ./data/turrutebasen

  # Batch download from config file
  %(prog)s --config datasets.yaml
        """
    )

    # Options for listing datasets
    parser.add_argument('--list-datasets', action='store_true',
                       help='List all available datasets from catalog')
    parser.add_argument('--dataset', metavar='NAME',
                       help='Dataset name to show download options for (use with --list-datasets)')
    parser.add_argument('--norge-only', action='store_true',
                       help='Filter to nationwide (Norge) data only')
    parser.add_argument('--config-output', action='store_true',
                       help='Generate YAML configuration snippet')

    # Download options
    parser.add_argument('--feed-url', metavar='URL',
                       help='Explicit ATOM feed URL (overrides dataset name)')
    parser.add_argument('--format', metavar='FORMATS',
                       help='Format preference list (e.g., PostGIS,GML)')
    parser.add_argument('--config', metavar='FILE', type=Path,
                       help='YAML configuration file for batch download')

    # Positional arguments
    parser.add_argument('dataset_or_output', nargs='?',
                       help='Dataset name (e.g., teig, turrutebasen) or output directory')
    parser.add_argument('output_dir', nargs='?', type=Path,
                       help='Output directory (if dataset name provided)')

    args = parser.parse_args()

    # Determine dataset_name and output_dir from positional args
    dataset_name = None
    output_dir = None

    if args.dataset_or_output:
        if args.dataset_or_output.lower() in DATASET_FEEDS:
            dataset_name = args.dataset_or_output
            output_dir = args.output_dir
        elif args.output_dir is None:
            # First arg is output directory
            output_dir = Path(args.dataset_or_output)
        else:
            # Both provided, treat first as dataset name
            dataset_name = args.dataset_or_output
            output_dir = args.output_dir

    # Determine default output directory
    if output_dir is None:
        if dataset_name:
            dataset_name_lower = dataset_name.lower()
            output_dir = Path(DEFAULT_OUTPUT_DIRS.get(dataset_name_lower, './data'))
        else:
            # Default for backward compatibility
            output_dir = Path("./data/matrikkel")

    # Parse format preference
    format_preference = None
    if args.format:
        format_preference = [f.strip() for f in args.format.split(',')]

    # Store parsed values in args object
    args.dataset_name = dataset_name
    args.output_dir = output_dir
    args.format_preference = format_preference

    return args


def load_config_file(config_path: Path) -> List[Dict[str, Any]]:
    """Load and parse YAML configuration file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        List of dataset configurations
    """
    if not YAML_AVAILABLE:
        print("Feil: PyYAML er ikke installert. Installer med: pip install pyyaml", file=sys.stderr)
        sys.exit(1)

    if not config_path.exists():
        print(f"Feil: Konfigurasjonsfil ikke funnet: {config_path}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        if not isinstance(config, list):
            print("Feil: Konfigurasjonsfil må inneholde en liste av datasett", file=sys.stderr)
            sys.exit(1)

        return config
    except yaml.YAMLError as e:
        print(f"Feil: Kunne ikke parse YAML-fil: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Feil: Kunne ikke lese konfigurasjonsfil: {e}", file=sys.stderr)
        sys.exit(1)


def download_from_config(config_path: Path) -> None:
    """Download datasets from configuration file.

    Args:
        config_path: Path to YAML configuration file
    """
    configs = load_config_file(config_path)

    if not configs:
        print("Advarsel: Konfigurasjonsfil er tom.", file=sys.stderr)
        return

    print(f"==> Laster ned {len(configs)} datasett fra konfigurasjonsfil...\n")

    success_count = 0
    failed_count = 0

    for i, dataset_config in enumerate(configs, 1):
        name = dataset_config.get('name', f'dataset_{i}')
        dataset_name = dataset_config.get('dataset', '')
        format_pref = dataset_config.get('format', 'PostGIS')
        utm_zone = dataset_config.get('utm_zone', '25833')
        area_filter = dataset_config.get('area_filter', 'Norge')
        area_type = dataset_config.get('area_type', '')
        output_dir = dataset_config.get('output_dir', f'./data/{name}')

        print(f"[{i}/{len(configs)}] {name}")
        print(f"  Dataset: {dataset_name}")
        print(f"  Format: {format_pref}")
        print(f"  UTM Zone: {utm_zone}")
        print(f"  Area Filter: {area_filter}")
        if area_type:
            print(f"  Area Type: {area_type}")
        print(f"  Output: {output_dir}")
        print()

        try:
            # Determine format preference
            format_preference = [format_pref]
            if format_pref == 'PostGIS':
                format_preference.extend(['FGDB', 'GML'])
            elif format_pref == 'FGDB':
                format_preference.extend(['PostGIS', 'GML'])
            elif format_pref == 'GML':
                format_preference.extend(['PostGIS', 'FGDB'])

            # Get feed URL
            feed_url, _ = get_atom_feed_url(dataset_name, None, format_preference)

            # Create output directory
            outdir = Path(output_dir)
            outdir.mkdir(parents=True, exist_ok=True)

            # Fetch and parse ATOM feed
            root = fetch_atom_feed(feed_url)

            # Extract download URLs (pass parameters directly, no env vars needed)
            urls = extract_download_urls(root, str(utm_zone), area_filter, area_type if area_type else None)

            if not urls:
                print(f"  ⚠ Ingen filer funnet som matcher kriteriene\n")
                failed_count += 1
                continue

            print(f"  ✓ Fant {len(urls)} fil(er) som matcher kriteriene")

            # Download files
            download_count = process_download_urls(urls, outdir, name, str(utm_zone), format_pref)

            if download_count > 0:
                print(f"  ✓ Ferdig med {name} ({download_count} fil(er) lastet ned)\n")
                success_count += 1
            else:
                print(f"  ⚠ Ingen nye filer lastet ned for {name}\n")
                failed_count += 1

        except Exception as e:
            print(f"  ✗ Feil ved nedlasting av {name}: {e}\n", file=sys.stderr)
            failed_count += 1

    # Summary
    print("==> Sammendrag")
    print(f"  ✓ Vellykket: {success_count}")
    print(f"  ✗ Feilet: {failed_count}")
    print(f"  Total: {len(configs)}")


def main():
    """Main function."""
    # Parse arguments
    args = parse_arguments()

    # Handle --config option (batch download from config file)
    if args.config:
        download_from_config(args.config)
        return

    # Handle --list-datasets option
    if args.list_datasets:
        list_available_datasets(dataset_name=args.dataset, norge_only=args.norge_only, config_output=args.config_output)
        return

    # Get ATOM feed URL
    atom_feed_url, format_type = get_atom_feed_url(args.dataset_name, args.feed_url, args.format_preference)

    # Determine dataset display name
    if args.dataset_name:
        display_name = args.dataset_name
    elif args.feed_url:
        display_name = "custom"
    else:
        display_name = "teig"

    # Get environment variables
    utm_zone = os.environ.get("UTM_ZONE", "25833")
    area_filter = os.environ.get("AREA_FILTER", "Norge")
    area_type = os.environ.get("AREA_TYPE", "")

    # Create output directory
    outdir = args.output_dir
    outdir.mkdir(parents=True, exist_ok=True)

    format_label = format_type if format_type else "PostGIS"
    print(f"==> Parser feed for {format_label}-filer med EPSG:{utm_zone} ...")
    if dataset_name or feed_url:
        print(f"    Dataset: {display_name}")
        if format_type:
            print(f"    Format: {format_type}")

    # Fetch and parse ATOM feed
    root = fetch_atom_feed(atom_feed_url)

    # Extract download URLs
    urls = extract_download_urls(root, utm_zone, area_filter, area_type)

    if not urls:
        print(f"Feil: Fant ingen filer som matcher EPSG:{utm_zone}", file=sys.stderr)
        if area_filter:
            print(f"       med område-filter: {area_filter}", file=sys.stderr)
        print("", file=sys.stderr)
        print("Tilgjengelige EPSG-koder i feeden:", file=sys.stderr)
        # Extract available EPSG codes
        categories = root.findall('.//atom:category[@term]', NAMESPACES)
        epsg_codes = sorted(set(
            cat.get('term') for cat in categories
            if cat.get('term', '').startswith('EPSG:')
        ))
        for code in epsg_codes:
            print(f"  - {code}", file=sys.stderr)
        sys.exit(1)

    print(f"  ✓ Fant {len(urls)} fil(er) som matcher kriteriene")

    # Download files
    print(f"==> Laster ned filer til {outdir}")
    download_count = process_download_urls(urls, outdir, display_name, utm_zone, format_label)

    if download_count == 0 and len(urls) > 0:
        print("Advarsel: Ingen nye filer ble lastet ned (alle eksisterer allerede?).", file=sys.stderr)
    elif download_count == 0:
        print("Feil: Ingen filer ble lastet ned.", file=sys.stderr)
        sys.exit(1)

    print(f"==> Ferdig. {download_count} fil(er) lastet ned til {outdir}")


if __name__ == "__main__":
    main()

