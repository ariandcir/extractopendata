#!/usr/bin/env python3
"""Export ArcGIS Hub datasets to parquet with incremental watermarks."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

DEFAULT_WATERMARK_FIELDS = [
    "last_edited_date",
    "editdate",
    "lastupdate",
    "last_update",
    "created_date",
    "creationdate",
]

HASH_WATERMARK_FIELD = "__row_hashes_file__"
PORTAL_URL = "https://opendata.montgomeryal.gov"


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_") or "dataset"


def compile_optional_regex(pattern: str | None) -> re.Pattern[str] | None:
    if not pattern:
        return None
    return re.compile(pattern)


def build_session(max_retries: int, backoff_factor: float):
    try:
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
    except ModuleNotFoundError as exc:
        raise RuntimeError("Missing dependency: install requests (pip install requests)") from exc

    session = requests.Session()
    retry = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        status=max_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=backoff_factor,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def request_json(
    session,
    url: str,
    params: dict[str, Any] | None = None,
    timeout: int = 60,
) -> dict[str, Any]:
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and "error" in payload:
        raise RuntimeError(f"ArcGIS error for {url}: {payload['error']}")
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object from {url}, got {type(payload).__name__}")
    return payload


def list_datasets(session, search_base_url: str, page_size: int) -> list[dict[str, Any]]:
    datasets: list[dict[str, Any]] = []
    next_url = f"{search_base_url.rstrip('/')}/api/search/v1/collections/dataset/items?limit={page_size}"

    while next_url:
        payload = request_json(session, next_url)
        features = payload.get("features", [])
        if not features:
            break

        for feature in features:
            properties = feature.get("properties", {})
            service_url = properties.get("url")
            if not isinstance(service_url, str):
                continue
            if "/MapServer/" not in service_url and "/FeatureServer/" not in service_url:
                continue
            datasets.append(
                {
                    "id": properties.get("id") or feature.get("id"),
                    "title": properties.get("title") or "Untitled Dataset",
                    "service_url": service_url.rstrip("/"),
                }
            )

        next_url = None
        for link in payload.get("links", []):
            if isinstance(link, dict) and link.get("rel") == "next":
                next_url = link.get("href")
                break

    return datasets


def normalize_layer_url(service_url: str) -> str:
    normalized = service_url.rstrip("/")
    if normalized.lower().endswith(("/mapserver", "/featureserver")):
        normalized = f"{normalized}/0"
    return normalized


def build_single_dataset(session, service_url: str, dataset_id: str | None, title: str | None) -> dict[str, Any]:
    layer_url = normalize_layer_url(service_url)
    layer_meta = get_layer_metadata(session, layer_url)

    resolved_id = dataset_id
    if not resolved_id and isinstance(layer_meta, dict):
        resolved_id = str(layer_meta.get("id") or "")
    if not resolved_id:
        resolved_id = slugify(layer_url)

    resolved_title = title
    if not resolved_title and isinstance(layer_meta, dict):
        resolved_title = str(layer_meta.get("name") or "").strip()
    if not resolved_title:
        resolved_title = f"Dataset {resolved_id}"

    return {
        "id": resolved_id,
        "title": resolved_title,
        "service_url": layer_url,
    }


def get_layer_metadata(session, layer_url: str) -> dict[str, Any] | None:
    try:
        return request_json(session, layer_url, params={"f": "json"})
    except Exception:
        return None


def is_queryable_layer(layer_meta: dict[str, Any] | None) -> bool:
    if not layer_meta:
        return True

    capabilities = str(layer_meta.get("capabilities", "")).lower()
    type_name = str(layer_meta.get("type", "")).lower()

    if "query" not in capabilities and type_name not in {"feature layer", "table"}:
        return False

    return True


def detect_watermark_field(sample_row: dict[str, Any]) -> str | None:
    if not sample_row:
        return None

    keys = {k.lower(): k for k in sample_row.keys()}

    for candidate in DEFAULT_WATERMARK_FIELDS:
        if candidate in keys:
            return keys[candidate]

    for lower_name, original_name in keys.items():
        if lower_name.endswith("_date") or "edit" in lower_name:
            return original_name

    return None


def detect_object_id_field(layer_meta: dict[str, Any] | None, sample_row: dict[str, Any]) -> str | None:
    if not layer_meta or not sample_row:
        return None

    candidates: list[str] = []

    object_id_field = layer_meta.get("objectIdField")
    if isinstance(object_id_field, str):
        candidates.append(object_id_field)

    unique_id_info = layer_meta.get("uniqueIdField")
    if isinstance(unique_id_info, dict):
        unique_name = unique_id_info.get("name")
        if isinstance(unique_name, str):
            candidates.append(unique_name)

    fields = layer_meta.get("fields")
    if isinstance(fields, list):
        for field in fields:
            if not isinstance(field, dict):
                continue
            if str(field.get("type", "")).lower() == "esrifieldtypeoid":
                name = field.get("name")
                if isinstance(name, str):
                    candidates.append(name)

    row_keys = {k.lower(): k for k in sample_row.keys()}
    for candidate in candidates:
        lookup = row_keys.get(candidate.lower())
        if lookup:
            return lookup

    fallback = row_keys.get("objectid")
    if fallback:
        return fallback

    return None


def hash_row(row: dict[str, Any]) -> str:
    canonical = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()


def filter_changed_rows_with_hash_fallback(
    rows: list[dict[str, Any]], previous_hashes: set[str]
) -> tuple[list[dict[str, Any]], set[str]]:
    changed: list[dict[str, Any]] = []
    current_hashes: set[str] = set()

    for row in rows:
        row_hash = hash_row(row)
        current_hashes.add(row_hash)
        if row_hash not in previous_hashes:
            changed.append(row)

    return changed, current_hashes


def fetch_layer_rows(
    session,
    layer_url: str,
    page_size: int,
    where_clause: str,
    max_pages: int | None = None,
) -> list[dict[str, Any]]:
    offset = 0
    rows: list[dict[str, Any]] = []
    pages = 0

    while True:
        params = {
            "where": where_clause,
            "outFields": "*",
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": page_size,
        }
        payload = request_json(session, f"{layer_url}/query", params=params)
        features = payload.get("features", [])
        if not isinstance(features, list):
            raise RuntimeError(f"Unexpected features payload for {layer_url}")

        batch = [f.get("attributes", {}) for f in features if isinstance(f, dict)]
        rows.extend(batch)
        pages += 1

        if max_pages is not None and pages >= max_pages:
            break

        exceeded = bool(payload.get("exceededTransferLimit", False))
        if not batch:
            break

        if exceeded:
            offset += page_size
            continue

        if len(batch) < page_size:
            break

        offset += page_size

    return rows


def load_watermarks(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_watermarks(path: Path, watermarks: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(watermarks, indent=2, sort_keys=True), encoding="utf-8")


def max_numeric(values: list[Any]) -> int | None:
    numeric_values = [int(v) for v in values if isinstance(v, (int, float))]
    return max(numeric_values) if numeric_values else None


def load_hash_state(hash_state_dir: Path, dataset_id: str) -> set[str]:
    hash_file = hash_state_dir / f"{dataset_id}.json"
    if not hash_file.exists():
        return set()
    payload = json.loads(hash_file.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return set()
    return {str(v) for v in payload}


def save_hash_state(hash_state_dir: Path, dataset_id: str, hashes: set[str]) -> Path:
    hash_state_dir.mkdir(parents=True, exist_ok=True)
    hash_file = hash_state_dir / f"{dataset_id}.json"
    hash_file.write_text(json.dumps(sorted(hashes), indent=2), encoding="utf-8")
    return hash_file


def maybe_upsert_hash_rows(frame, parquet_path: Path, upsert_key: str | None):
    if not upsert_key or not parquet_path.exists() or upsert_key not in frame.columns:
        return frame

    try:
        import pandas as pd

        current = pd.read_parquet(parquet_path)
    except Exception:
        return frame

    if upsert_key not in current.columns:
        return frame

    merged = pd.concat([current, frame], ignore_index=True)
    merged = merged.drop_duplicates(subset=[upsert_key], keep="last")
    return merged


def filter_datasets(
    datasets: list[dict[str, Any]],
    include_id: re.Pattern[str] | None,
    include_title: re.Pattern[str] | None,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for dataset in datasets:
        dataset_id = str(dataset.get("id") or "")
        title = str(dataset.get("title") or "")

        if include_id and not include_id.search(dataset_id):
            continue
        if include_title and not include_title.search(title):
            continue
        filtered.append(dataset)

    return filtered


def export_datasets(
    output_dir: Path,
    watermark_path: Path | None,
    hash_state_dir: Path,
    page_size: int,
    pause_seconds: float,
    max_datasets: int | None,
    max_pages_per_dataset: int | None,
    include_id_regex: str | None,
    include_title_regex: str | None,
    upsert_key: str | None,
    service_url: str | None,
    dataset_id: str | None,
    dataset_title: str | None,
    session,
) -> dict[str, Any]:
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise RuntimeError("Missing dependency: install pandas and pyarrow (pip install pandas pyarrow)") from exc

    if service_url:
        discovered = []
        datasets = [build_single_dataset(session, service_url, dataset_id, dataset_title)]
        print("Running in single-dataset mode")
    else:
        discovered = list_datasets(session, PORTAL_URL, page_size)
        include_id = compile_optional_regex(include_id_regex)
        include_title = compile_optional_regex(include_title_regex)
        datasets = filter_datasets(discovered, include_id, include_title)
        print(f"Discovered {len(discovered)} dataset candidates; selected {len(datasets)}")

    state_enabled = watermark_path is not None
    watermarks = load_watermarks(watermark_path) if state_enabled and watermark_path else {}
    updated_watermarks = dict(watermarks)

    output_dir.mkdir(parents=True, exist_ok=True)

    if max_datasets is not None:
        datasets = datasets[:max_datasets]

    summary = {
        "discovered": len(discovered),
        "selected": len(datasets),
        "processed": 0,
        "skipped": 0,
        "failed": 0,
        "rows_written": 0,
        "fallback_counts": {"date_or_oid": 0, "row_hash": 0},
    }

    for index, dataset in enumerate(datasets, start=1):
        dataset_id = str(dataset.get("id") or "unknown")
        title = str(dataset.get("title") or "Untitled Dataset")
        layer_url = str(dataset["service_url"]).rstrip("/")
        summary["processed"] += 1

        print(f"[{index}/{len(datasets)}] {title} ({dataset_id})")

        layer_meta = get_layer_metadata(session, layer_url)
        if layer_meta is None:
            print("  - warning: could not verify layer metadata; attempting query anyway")
        elif not is_queryable_layer(layer_meta):
            print("  - skipped: layer does not appear queryable")
            summary["skipped"] += 1
            continue

        previous = watermarks.get(dataset_id)
        where = "1=1"

        if (
            state_enabled
            and isinstance(previous, dict)
            and previous.get("field")
            and previous.get("value") is not None
            and previous.get("field") != HASH_WATERMARK_FIELD
        ):
            where = f"{previous['field']} > {int(previous['value'])}"
            print(f"  - incremental where: {where}")

        try:
            rows = fetch_layer_rows(
                session,
                layer_url,
                page_size=page_size,
                where_clause=where,
                max_pages=max_pages_per_dataset,
            )
        except Exception as exc:
            print(f"  - skipped: query failed ({exc})")
            summary["failed"] += 1
            continue

        if not rows:
            print("  - no new rows")
            continue

        rows_to_write = rows
        if not state_enabled:
            frame = pd.DataFrame(rows_to_write)
            slug = slugify(title)
            parquet_path = output_dir / f"{slug}__{dataset_id}.parquet"
            frame.to_parquet(parquet_path, index=False)
            summary["rows_written"] += len(frame)
            print(f"  - wrote {len(frame)} rows to {parquet_path}")
            if pause_seconds > 0:
                time.sleep(pause_seconds)
            continue

        watermark_field = None
        if isinstance(previous, dict):
            watermark_field = previous.get("field")

        if watermark_field not in rows[0]:
            watermark_field = detect_watermark_field(rows[0])

        if watermark_field is None:
            watermark_field = detect_object_id_field(layer_meta, rows[0])
            if watermark_field:
                print(f"  - using object-id watermark fallback: {watermark_field}")

        if watermark_field and watermark_field in rows[0]:
            max_value = max_numeric([row.get(watermark_field) for row in rows])
            if max_value is not None:
                updated_watermarks[dataset_id] = {
                    "field": watermark_field,
                    "value": max_value,
                    "title": title,
                    "service_url": layer_url,
                }
                print(f"  - updated watermark: {watermark_field}={max_value}")
                summary["fallback_counts"]["date_or_oid"] += 1
            else:
                watermark_field = None

        slug = slugify(title)
        parquet_path = output_dir / f"{slug}__{dataset_id}.parquet"

        if watermark_field is None:
            previous_hashes = load_hash_state(hash_state_dir, dataset_id)
            changed_rows, current_hashes = filter_changed_rows_with_hash_fallback(rows, previous_hashes)
            if not changed_rows:
                print("  - no changed rows detected by hash fallback")
                hash_file = save_hash_state(hash_state_dir, dataset_id, current_hashes)
                updated_watermarks[dataset_id] = {
                    "field": HASH_WATERMARK_FIELD,
                    "value": {"file": str(hash_file), "count": len(current_hashes)},
                    "title": title,
                    "service_url": layer_url,
                }
                summary["fallback_counts"]["row_hash"] += 1
                continue

            rows_to_write = changed_rows
            hash_file = save_hash_state(hash_state_dir, dataset_id, current_hashes)
            updated_watermarks[dataset_id] = {
                "field": HASH_WATERMARK_FIELD,
                "value": {"file": str(hash_file), "count": len(current_hashes)},
                "title": title,
                "service_url": layer_url,
            }
            summary["fallback_counts"]["row_hash"] += 1
            print(
                "  - using row-hash fallback: "
                f"{len(changed_rows)} changed/new rows out of {len(rows)}"
            )

        frame = pd.DataFrame(rows_to_write)
        frame = maybe_upsert_hash_rows(frame, parquet_path, upsert_key)

        frame.to_parquet(parquet_path, index=False)
        summary["rows_written"] += len(frame)
        print(f"  - wrote {len(frame)} rows to {parquet_path}")

        if pause_seconds > 0:
            time.sleep(pause_seconds)

    if state_enabled and watermark_path:
        save_watermarks(watermark_path, updated_watermarks)
        print(f"Saved watermarks to {watermark_path}")
    return summary


def list_datasets_mode(
    page_size: int,
    max_datasets: int | None,
    include_id_regex: str | None,
    include_title_regex: str | None,
    list_format: str,
    session,
) -> dict[str, Any]:
    discovered = list_datasets(session, PORTAL_URL, page_size)
    include_id = compile_optional_regex(include_id_regex)
    include_title = compile_optional_regex(include_title_regex)
    datasets = filter_datasets(discovered, include_id, include_title)

    if max_datasets is not None:
        datasets = datasets[:max_datasets]

    if list_format == "json":
        print(json.dumps(datasets, indent=2))
    else:
        for index, dataset in enumerate(datasets, start=1):
            dataset_id = str(dataset.get("id") or "")
            title = str(dataset.get("title") or "")
            service_url = str(dataset.get("service_url") or "")
            print(f"{index}\t{dataset_id}\t{title}\t{service_url}")

    return {
        "mode": "list_datasets",
        "discovered": len(discovered),
        "selected": len(datasets),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=".interop/opendata-parquet", help="Directory to write parquet files")
    parser.add_argument("--watermark-file", default=None, help="Optional path to watermark json for incremental state")
    parser.add_argument(
        "--hash-state-dir",
        default=".interop/opendata-hash-state",
        help="Directory for row-hash fallback state files",
    )
    parser.add_argument("--page-size", default=1000, type=int, help="Rows per ArcGIS query request")
    parser.add_argument("--pause-seconds", default=0.0, type=float, help="Optional pause between datasets")
    parser.add_argument("--max-datasets", default=None, type=int, help="Optional cap for number of datasets to process")
    parser.add_argument("--max-pages-per-dataset", default=None, type=int, help="Optional cap for query pages per dataset")
    parser.add_argument("--include-id-regex", default=None, help="Only process datasets whose id matches this regex")
    parser.add_argument("--include-title-regex", default=None, help="Only process datasets whose title matches this regex")
    parser.add_argument("--upsert-key", default=None, help="Optional key column used to upsert during hash fallback")
    parser.add_argument(
        "--service-url",
        default=None,
        help="Optional ArcGIS layer URL to query directly (for example .../MapServer/0)",
    )
    parser.add_argument("--dataset-id", default=None, help="Optional dataset id override when --service-url is used")
    parser.add_argument("--dataset-title", default=None, help="Optional dataset title override when --service-url is used")
    parser.add_argument("--max-retries", default=4, type=int, help="HTTP retry attempts for transient errors")
    parser.add_argument("--backoff-factor", default=0.5, type=float, help="HTTP retry backoff factor")
    parser.add_argument("--summary-file", default=None, help="Optional JSON file path for structured run summary")
    parser.add_argument(
        "--list-datasets",
        action="store_true",
        help="List datasets after applying include filters, then exit without exporting parquet",
    )
    parser.add_argument(
        "--list-format",
        default="text",
        choices=["text", "json"],
        help="Output format for --list-datasets",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        session = build_session(args.max_retries, args.backoff_factor)
        if args.list_datasets:
            summary = list_datasets_mode(
                page_size=args.page_size,
                max_datasets=args.max_datasets,
                include_id_regex=args.include_id_regex,
                include_title_regex=args.include_title_regex,
                list_format=args.list_format,
                session=session,
            )
        else:
            summary = export_datasets(
                output_dir=Path(args.output_dir),
                watermark_path=Path(args.watermark_file) if args.watermark_file else None,
                hash_state_dir=Path(args.hash_state_dir),
                page_size=args.page_size,
                pause_seconds=args.pause_seconds,
                max_datasets=args.max_datasets,
                max_pages_per_dataset=args.max_pages_per_dataset,
                include_id_regex=args.include_id_regex,
                include_title_regex=args.include_title_regex,
                upsert_key=args.upsert_key,
                service_url=args.service_url,
                dataset_id=args.dataset_id,
                dataset_title=args.dataset_title,
                session=session,
            )
        if args.summary_file:
            summary_path = Path(args.summary_file)
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
            print(f"Saved summary to {summary_path}")
        return 0
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
