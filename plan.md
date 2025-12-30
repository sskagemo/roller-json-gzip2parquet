# Implementation Plan: roller-json-gzip2parquet

## Overview
A Python script to download and convert the Brønnøysundregistrene roller (roles) dataset from JSON.gz to Parquet format, using memory-efficient streaming.

## Data Source
- **URL**: `https://data.brreg.no/enhetsregisteret/api/roller/totalbestand`
- **Format**: JSON array (gzip compressed)

## Target Schema (Flattened - One Row per Rolle)

| Column | Type | Description |
|--------|------|-------------|
| organisasjonsnummer | String | Organization ID |
| sist_endret | Date | Last modified (from rollegruppe) |
| rolle_kode | String | Role code (e.g., "DAGL", "LEDE", "MEDL") |
| rolle_beskrivelse | String | Role description |
| person_fodselsdato | Date | Birth date (null if enhet) |
| person_fornavn | String | First name |
| person_mellomnavn | String | Middle name (nullable) |
| person_etternavn | String | Last name |
| person_er_doed | Boolean | Is deceased |
| enhet_orgnr | String | Org number (if entity, not person) |
| enhet_navn | String | Entity name |
| enhet_organisasjonsform | String | Entity org form code |
| enhet_er_slettet | Boolean | Entity is deleted |
| valgt_av_kode | String | Elected by code (nullable) |
| valgt_av_beskrivelse | String | Elected by description (nullable) |
| fratraadt | Boolean | Resigned |
| rekkefolge | Int | Order/sequence |

## Technical Approach

### Libraries
- **ijson**: Streaming JSON parser (parse array items one at a time)
- **polars**: DataFrame operations and Parquet writing
- **httpx**: HTTP client with streaming support
- **argparse**: CLI argument parsing (stdlib)

### Architecture
1. Stream HTTP response → gzip decompression → ijson parser
2. ijson yields one organization object at a time
3. Flatten each organization's roles into rows
4. Accumulate rows in batches (default: 10,000)
5. Write batches to Parquet (append mode via pyarrow)

### CLI Interface
```bash
# Download from URL (default) and convert
python main.py -o output.parquet

# Custom URL
python main.py --url "https://..." -o output.parquet

# Download and save local JSON copy
python main.py -o output.parquet --save-json data.json.gz

# Read from local file
python main.py --local data.json.gz -o output.parquet

# Adjust batch size
python main.py -o output.parquet --batch-size 50000
```

## Implementation Steps

| Step | Description | Status |
|------|-------------|--------|
| 1 | Write plan.md | ✅ Done |
| 2 | Update `pyproject.toml` with dependencies | ✅ Done |
| 3 | Implement streaming download/decompression | ✅ Done |
| 4 | Implement ijson-based JSON parsing | ✅ Done |
| 5 | Implement flattening logic | ✅ Done |
| 6 | Implement batch writing to Parquet | ✅ Done |
| 7 | Add CLI with argparse | ✅ Done |
| 8 | Add progress reporting | ✅ Done |
| 9 | Install dependencies with uv | ✅ Done |
| 10 | Test with full download | ✅ Done |
| 11 | Update README.md | ⏳ Pending |

## Change Log

### 2025-12-30
- Created initial plan.md
- Updated pyproject.toml with dependencies (ijson, polars, httpx, pyarrow)
- Implemented main.py with full functionality:
  - Streaming JSON parsing with ijson
  - Flattening nested organization/rolle structure
  - Batch writing to Parquet via PyArrow
  - CLI with argparse (--url, --local, --save-json, --output, --batch-size)
  - Progress reporting during download and conversion

## Excluded Fields
- `_links` metadata (excluded to save space)
- `rollegruppe.type` (only `sist_endret` is kept and attached to each rolle)
