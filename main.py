#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "httpx>=0.27.0",
#     "ijson>=3.2.0",
#     "polars>=1.0.0",
#     "pyarrow>=15.0.0",
# ]
# ///
"""
Download and convert Brønnøysundregistrene roller data from JSON.gz to Parquet.

This script streams the JSON data to avoid loading the entire file into memory,
making it suitable for processing very large datasets.
"""

import argparse
import gzip
import sys
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from typing import IO, Any, cast

import httpx
import ijson
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq


# Default URL for the roller totalbestand
DEFAULT_URL = "https://data.brreg.no/enhetsregisteret/api/roller/totalbestand"

# Default batch size for writing to parquet
DEFAULT_BATCH_SIZE = 100_000

# Schema for the parquet file
PARQUET_SCHEMA = pa.schema([
    ("organisasjonsnummer", pa.string()),
    ("sist_endret", pa.date32()),
    ("rolle_kode", pa.string()),
    ("rolle_beskrivelse", pa.string()),
    ("person_fodselsdato", pa.date32()),
    ("person_fornavn", pa.string()),
    ("person_mellomnavn", pa.string()),
    ("person_etternavn", pa.string()),
    ("person_er_doed", pa.bool_()),
    ("enhet_orgnr", pa.string()),
    ("enhet_navn", pa.string()),
    ("enhet_organisasjonsform", pa.string()),
    ("enhet_er_slettet", pa.bool_()),
    ("valgt_av_kode", pa.string()),
    ("valgt_av_beskrivelse", pa.string()),
    ("fratraadt", pa.bool_()),
    ("rekkefolge", pa.int32()),
])


def flatten_organization(org: dict[str, Any]) -> Generator[dict[str, Any], None, None]:
    """
    Flatten an organization record into individual rolle rows.
    
    Each organization has multiple rollegrupper, and each rollegruppe has multiple roller.
    We flatten this to one row per rolle, attaching the sist_endret from the rollegruppe.
    """
    orgnr = org.get("organisasjonsnummer")
    
    for rollegruppe in org.get("rollegrupper", []):
        sist_endret = rollegruppe.get("sistEndret")
        
        for rolle in rollegruppe.get("roller", []):
            row: dict[str, Any] = {
                "organisasjonsnummer": orgnr,
                "sist_endret": sist_endret,
                "rolle_kode": rolle.get("type", {}).get("kode"),
                "rolle_beskrivelse": rolle.get("type", {}).get("beskrivelse"),
                "fratraadt": rolle.get("fratraadt"),
                "rekkefolge": rolle.get("rekkefolge"),
            }
            
            # Person fields
            person = rolle.get("person")
            if person:
                row["person_fodselsdato"] = person.get("fodselsdato")
                navn = person.get("navn", {})
                row["person_fornavn"] = navn.get("fornavn")
                row["person_mellomnavn"] = navn.get("mellomnavn")
                row["person_etternavn"] = navn.get("etternavn")
                row["person_er_doed"] = person.get("erDoed")
            else:
                row["person_fodselsdato"] = None
                row["person_fornavn"] = None
                row["person_mellomnavn"] = None
                row["person_etternavn"] = None
                row["person_er_doed"] = None
            
            # Enhet (entity) fields
            enhet = rolle.get("enhet")
            if enhet:
                row["enhet_orgnr"] = enhet.get("organisasjonsnummer")
                # navn is a list in enhet
                navn_list = enhet.get("navn", [])
                row["enhet_navn"] = navn_list[0] if navn_list else None
                row["enhet_organisasjonsform"] = enhet.get("organisasjonsform", {}).get("kode")
                row["enhet_er_slettet"] = enhet.get("erSlettet")
            else:
                row["enhet_orgnr"] = None
                row["enhet_navn"] = None
                row["enhet_organisasjonsform"] = None
                row["enhet_er_slettet"] = None
            
            # valgtAv fields
            valgt_av = rolle.get("valgtAv")
            if valgt_av:
                row["valgt_av_kode"] = valgt_av.get("kode")
                row["valgt_av_beskrivelse"] = valgt_av.get("beskrivelse")
            else:
                row["valgt_av_kode"] = None
                row["valgt_av_beskrivelse"] = None
            
            yield row


class StreamingGzipDecompressor:
    """
    A file-like wrapper that decompresses gzip data on-the-fly from an iterator of bytes.
    """
    
    def __init__(self, byte_iterator: Iterator[bytes]):
        self._iterator = byte_iterator
        self._decompressor = gzip.GzipFile(fileobj=self._create_readable())
        self._buffer = b""
    
    def _create_readable(self) -> IO[bytes]:
        """Create a file-like object from the byte iterator."""
        class IteratorReader:
            def __init__(inner_self, iterator: Iterator[bytes]):
                inner_self._iterator = iterator
                inner_self._buffer = b""
            
            def read(inner_self, size: int = -1) -> bytes:
                if size < 0:
                    # Read all remaining
                    result = inner_self._buffer
                    for chunk in inner_self._iterator:
                        result += chunk
                    inner_self._buffer = b""
                    return result
                
                while len(inner_self._buffer) < size:
                    try:
                        inner_self._buffer += next(inner_self._iterator)
                    except StopIteration:
                        break
                
                result = inner_self._buffer[:size]
                inner_self._buffer = inner_self._buffer[size:]
                return result
        
        return IteratorReader(self._iterator)  # type: ignore
    
    def read(self, size: int = -1) -> bytes:
        return self._decompressor.read(size)
    
    def close(self) -> None:
        self._decompressor.close()


@contextmanager
def open_json_stream(
    url: str | None = None,
    local_path: Path | None = None,
    save_json_path: Path | None = None,
) -> Generator[IO[bytes], None, None]:
    """
    Open a JSON stream from either a URL or a local file.
    
    Args:
        url: URL to download from (if not using local_path)
        local_path: Path to local .json.gz file (if not downloading)
        save_json_path: If provided, save the downloaded data to this path
    
    Yields:
        A file-like object for reading decompressed JSON data
    """
    if local_path:
        # Read from local gzip file
        print(f"Reading from local file: {local_path}")
        with gzip.open(local_path, "rb") as f:
            yield cast(IO[bytes], f)
    else:
        # Download from URL
        if not url:
            url = DEFAULT_URL
        
        print(f"Downloading from: {url}")
        
        with httpx.Client(timeout=None) as client:
            with client.stream("GET", url, headers={"Accept-Encoding": "gzip"}) as response:
                response.raise_for_status()
                
                content_length = response.headers.get("Content-Length")
                if content_length:
                    print(f"Content-Length: {int(content_length) / (1024*1024):.1f} MB")
                
                if save_json_path:
                    # Download to file first, then read from file
                    print(f"Saving to: {save_json_path}")
                    with open(save_json_path, "wb") as save_file:
                        total_bytes = 0
                        for chunk in response.iter_bytes():
                            save_file.write(chunk)
                            total_bytes += len(chunk)
                            # Progress every 10MB
                            if total_bytes % (10 * 1024 * 1024) < len(chunk):
                                print(f"  Downloaded: {total_bytes / (1024*1024):.1f} MB")
                    
                    print(f"Download complete: {total_bytes / (1024*1024):.1f} MB")
                    print(f"Reading from saved file: {save_json_path}")
                    
                    with gzip.open(save_json_path, "rb") as f:
                        yield cast(IO[bytes], f)
                else:
                    # Stream and decompress on-the-fly
                    # Collect all data since ijson needs seekable stream for some backends
                    print("Downloading and decompressing in memory...")
                    compressed_data = BytesIO()
                    total_bytes = 0
                    for chunk in response.iter_bytes():
                        compressed_data.write(chunk)
                        total_bytes += len(chunk)
                        # Progress every 10MB
                        if total_bytes % (10 * 1024 * 1024) < len(chunk):
                            print(f"  Downloaded: {total_bytes / (1024*1024):.1f} MB")
                    
                    print(f"Download complete: {total_bytes / (1024*1024):.1f} MB")
                    compressed_data.seek(0)
                    
                    with gzip.GzipFile(fileobj=compressed_data) as f:
                        yield cast(IO[bytes], f)


def parse_organizations(json_stream: IO[bytes]) -> Generator[dict[str, Any], None, None]:
    """
    Parse organizations from a JSON stream using ijson for memory efficiency.
    
    The JSON is an array of organization objects at the root level.
    """
    # ijson.items yields each item in the root array
    parser = ijson.items(json_stream, "item")
    
    for org in parser:
        yield org


# Polars schema for explicit type definition (avoids schema inference issues)
POLARS_SCHEMA = {
    "organisasjonsnummer": pl.String,
    "sist_endret": pl.String,  # Will be converted to Date
    "rolle_kode": pl.String,
    "rolle_beskrivelse": pl.String,
    "person_fodselsdato": pl.String,  # Will be converted to Date
    "person_fornavn": pl.String,
    "person_mellomnavn": pl.String,
    "person_etternavn": pl.String,
    "person_er_doed": pl.Boolean,
    "enhet_orgnr": pl.String,
    "enhet_navn": pl.String,
    "enhet_organisasjonsform": pl.String,
    "enhet_er_slettet": pl.Boolean,
    "valgt_av_kode": pl.String,
    "valgt_av_beskrivelse": pl.String,
    "fratraadt": pl.Boolean,
    "rekkefolge": pl.Int32,
}


def rows_to_polars_df(rows: list[dict[str, Any]]) -> pl.DataFrame:
    """Convert a list of row dictionaries to a Polars DataFrame."""
    if not rows:
        return pl.DataFrame()
    
    # Create DataFrame with explicit schema to avoid inference issues
    df = pl.DataFrame(rows, schema=POLARS_SCHEMA)
    
    # Convert date strings to actual Date type
    return df.with_columns([
        pl.col("sist_endret").str.to_date("%Y-%m-%d", strict=False),
        pl.col("person_fodselsdato").str.to_date("%Y-%m-%d", strict=False),
    ])


def process_json_to_parquet(
    url: str | None = None,
    local_path: Path | None = None,
    output_path: Path = Path("output.parquet"),
    save_json_path: Path | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    """
    Process JSON data to Parquet format.
    
    Args:
        url: URL to download from (if not using local_path)
        local_path: Path to local .json.gz file
        output_path: Path for output parquet file
        save_json_path: If provided, save downloaded JSON to this path
        batch_size: Number of rows to accumulate before writing
    """
    # Remove existing output file if it exists
    if output_path.exists():
        output_path.unlink()
        print(f"Removed existing file: {output_path}")
    
    writer: pq.ParquetWriter | None = None
    rows: list[dict[str, Any]] = []
    org_count = 0
    row_count = 0
    
    try:
        with open_json_stream(url=url, local_path=local_path, save_json_path=save_json_path) as json_stream:
            print("Parsing JSON and converting to Parquet...")
            
            for org in parse_organizations(json_stream):
                org_count += 1
                
                for row in flatten_organization(org):
                    rows.append(row)
                    row_count += 1
                    
                    # Write batch when we reach batch_size
                    if len(rows) >= batch_size:
                        df = rows_to_polars_df(rows)
                        table = df.to_arrow()
                        
                        if writer is None:
                            writer = pq.ParquetWriter(str(output_path), PARQUET_SCHEMA)
                        
                        # Cast to schema to ensure consistent types
                        table = table.cast(PARQUET_SCHEMA)
                        writer.write_table(table)
                        
                        print(f"  Processed {org_count:,} organizations, {row_count:,} rows")
                        rows = []
            
            # Write remaining rows
            if rows:
                df = rows_to_polars_df(rows)
                table = df.to_arrow()
                
                if writer is None:
                    writer = pq.ParquetWriter(str(output_path), PARQUET_SCHEMA)
                
                table = table.cast(PARQUET_SCHEMA)
                writer.write_table(table)
    
    finally:
        if writer:
            writer.close()
    
    print(f"\nConversion complete!")
    print(f"  Organizations processed: {org_count:,}")
    print(f"  Rows written: {row_count:,}")
    print(f"  Output file: {output_path}")
    
    # Show file size
    if output_path.exists():
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"  File size: {size_mb:.1f} MB")


def main() -> None:
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Download and convert Brønnøysundregistrene roller data from JSON.gz to Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download from default URL and convert
  %(prog)s -o roller.parquet

  # Download from custom URL
  %(prog)s --url "https://example.com/data.json.gz" -o output.parquet

  # Download and save local JSON copy
  %(prog)s -o roller.parquet --save-json roller.json.gz

  # Read from local file
  %(prog)s --local roller.json.gz -o roller.parquet

  # Adjust batch size for memory/performance tuning
  %(prog)s -o roller.parquet --batch-size 50000
        """,
    )
    
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=Path("roller.parquet"),
        help="Output parquet file path (default: roller.parquet)",
    )
    
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--url",
        type=str,
        default=DEFAULT_URL,
        help=f"URL to download from (default: {DEFAULT_URL})",
    )
    source_group.add_argument(
        "--local",
        type=Path,
        help="Path to local .json.gz file (instead of downloading)",
    )
    
    parser.add_argument(
        "--save-json",
        type=Path,
        help="Save downloaded JSON to this path (only when downloading)",
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Batch size for writing to parquet (default: {DEFAULT_BATCH_SIZE:,})",
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.local and args.save_json:
        parser.error("--save-json cannot be used with --local")
    
    try:
        process_json_to_parquet(
            url=args.url if not args.local else None,
            local_path=args.local,
            output_path=args.output,
            save_json_path=args.save_json,
            batch_size=args.batch_size,
        )
    except httpx.HTTPError as e:
        print(f"HTTP error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
