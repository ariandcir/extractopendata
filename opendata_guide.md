# ArcGIS Hub Data Extraction

Use `scripts/export_arcgis_hub_datasets.py` to export ArcGIS Hub datasets (for example, `https://opendata.montgomeryal.gov`) into parquet files.

## What it does

- Discovers datasets through `/api/search/v1/collections/dataset/items`
- Skips entries that do not look queryable (`MapServer` / `FeatureServer` layer URLs)
- Calls each layer's `/query` endpoint
- Uses `exceededTransferLimit` pagination when available
- Reuses a single HTTP session with retry/backoff for transient failures
- Writes one parquet file per dataset
- Stores per-dataset incremental watermarks in JSON
- Stores row-hash fallback state in separate per-dataset files
- Emits an optional structured summary JSON file

## Requirements

Install dependencies in your environment:

```bash
pip install requests pandas pyarrow
```

## CLI options

Show all options:

```bash
python3 scripts/export_arcgis_hub_datasets.py --help
```

Main options:

- Portal URL is fixed in the script as `https://opendata.montgomeryal.gov`.
- `--output-dir`: directory for parquet outputs.
- `--watermark-file`: optional JSON file for per-dataset incremental state. If omitted, the run is full-extract only and does not persist watermark/hash state.
- `--hash-state-dir`: directory for row-hash fallback state files.
- `--page-size`: rows requested per `/query` request.
- `--pause-seconds`: optional throttle between datasets.
- `--max-datasets`: limit number of datasets processed (useful for smoke tests).
- `--max-pages-per-dataset`: limit pages fetched per dataset (useful for fast validation).
- `--include-id-regex`: only process datasets whose ids match the regex.
- `--include-title-regex`: only process datasets whose titles match the regex.
- `--upsert-key`: when hash fallback is active, merge changed rows into existing parquet by this key.
- `--service-url`: query a specific ArcGIS layer URL directly (for example `.../FeatureServer/0`).
- `--dataset-id`: optional id override when using `--service-url`.
- `--dataset-title`: optional title override when using `--service-url`.
- `--max-retries`: retry count for transient HTTP failures (429/5xx).
- `--backoff-factor`: exponential backoff factor for retries.
- `--summary-file`: write structured run summary JSON.
- `--list-datasets`: discover + filter datasets and print them, then exit without exporting parquet.
- `--list-format`: output format for `--list-datasets` (`text` or `json`).

## Query examples (covering all script aspects)

### 1) Full export of all discovered datasets

```bash
python3 scripts/export_arcgis_hub_datasets.py \
  --output-dir .interop/opendata-parquet
```

### 2) Incremental refresh using previously saved watermarks

Run the same command again; the script automatically applies the best available mode for the Montgomery portal:

- `where=<date_field> > <saved_value>` (preferred)
- `where=<oid_field> > <saved_value>` (fallback)
- row-hash compare against `--hash-state-dir` files (final fallback)

```bash
python3 scripts/export_arcgis_hub_datasets.py \
  --output-dir .interop/opendata-parquet \
  --watermark-file .interop/opendata-watermarks.json \
  --hash-state-dir .interop/opendata-hash-state
```

### 3) Smoke test one dataset and one page

```bash
python3 scripts/export_arcgis_hub_datasets.py \
  --output-dir .interop/opendata-parquet \
  --watermark-file .interop/opendata-watermarks.json \
  --max-datasets 1 \
  --max-pages-per-dataset 1
```

### 4) Filter to selected datasets

```bash
python3 scripts/export_arcgis_hub_datasets.py \
  --include-title-regex 'permit|license' \
  --include-id-regex '^[a-f0-9]{32}$'
```

### 5) Hash fallback + upsert merge

```bash
python3 scripts/export_arcgis_hub_datasets.py \
  --watermark-file .interop/opendata-watermarks.json \
  --hash-state-dir .interop/opendata-hash-state \
  --upsert-key OBJECTID
```

### 6) Retry tuning + structured summary output

```bash
python3 scripts/export_arcgis_hub_datasets.py \
  --max-retries 6 \
  --backoff-factor 1.0 \
  --summary-file .interop/opendata-summary.json
```

### 7) List datasets directly from the script (no curl/jq)

Text output:

```bash
python3 scripts/export_arcgis_hub_datasets.py \
  --list-datasets \
  --include-title-regex 'permit|license'
```

JSON output:

```bash
python3 scripts/export_arcgis_hub_datasets.py \
  --list-datasets \
  --list-format json
```

### 8) Extract from a direct ArcGIS FeatureServer/MapServer layer URL

```bash
python3 export_arcgis_hub_datasets.py \
  --service-url 'https://services7.arcgis.com/xNUwUjOJqYE54USz/arcgis/rest/services/Most_Visited_Locations/FeatureServer/0' \
  --dataset-title 'Most Visited Locations' \
  --output-dir .interop/opendata-parquet \
  --summary-file .interop/most-visited-summary.json
```

This bypasses portal discovery and queries only that layer.

## ArcGIS layer `/query` examples used by the script

The script issues layer queries shaped like:

```text
<layer_url>/query?where=1=1&outFields=*&returnGeometry=false&f=json&resultOffset=0&resultRecordCount=1000
```

Incremental form:

```text
<layer_url>/query?where=last_edited_date>1743608590000&outFields=*&returnGeometry=false&f=json&resultOffset=0&resultRecordCount=1000
```

## Notes

- The default output path is under `.interop/`, which should remain untracked.
- Incremental fallback order is: date/edit watermark -> object-id watermark -> row-hash watermark.
- Row-hash watermark files are stored per dataset in `--hash-state-dir` to avoid large watermark JSON files.
- If `--watermark-file` is not provided, no watermark or hash state is persisted; each run performs a full extraction.
- `--upsert-key` is recommended when using hash fallback so changed rows merge cleanly into existing parquet.
- Dataset discovery and queryability checks still run even during smoke tests (`--max-datasets`).