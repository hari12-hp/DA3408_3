#!/usr/bin/env python3
"""DA3408 Assignment 3 - Ray Data pipeline."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

os.environ.setdefault("RAY_DATA_ENABLE_DISK_SHUFFLE", "1")

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import ray


# -----------------------------------------------------------------------------
# Source schema / required columns
# -----------------------------------------------------------------------------

TRIP_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
]

REQUIRED_NON_NULL = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
]

# Keep the output focused on fields needed to demonstrate the assignment.
FINAL_TRIP_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "total_amount",
]

OUTPUT_COLUMNS = [
    *FINAL_TRIP_COLUMNS,
    "duration_minutes",
    "pickup_hour",
    "PU_Borough",
    "PU_Zone",
    "PU_ServiceZone",
    "avg_speed_mph",
]

DEFAULT_JOIN_PARTITIONS = 256
DEFAULT_DEDUP_BUCKETS = 32
DEFAULT_BATCH_SIZE = 100_000
DEFAULT_MAX_ROWS_PER_FILE = 2_000_000
DEFAULT_READ_BLOCKS = 8
DEFAULT_READ_CONCURRENCY = 4
DEFAULT_UDF_PROBE_ROWS = 1_000_000


# -----------------------------------------------------------------------------
# Validation / metrics helpers
# -----------------------------------------------------------------------------

def parquet_row_count(path: str) -> int:
    root = Path(path)
    files = root.rglob("*.parquet") if root.is_dir() else [root]
    return sum(pq.ParquetFile(str(f)).metadata.num_rows for f in files)


# -----------------------------------------------------------------------------
# Cleansing
# -----------------------------------------------------------------------------

def clean_batch(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()

    batch["tpep_pickup_datetime"] = pd.to_datetime(
        batch["tpep_pickup_datetime"], errors="coerce"
    )
    batch["tpep_dropoff_datetime"] = pd.to_datetime(
        batch["tpep_dropoff_datetime"], errors="coerce"
    )

    batch = batch.loc[
        batch[REQUIRED_NON_NULL].notna().all(axis=1)
    ].copy()

    batch["_duration_seconds"] = (
        batch["tpep_dropoff_datetime"]
        - batch["tpep_pickup_datetime"]
    ).dt.total_seconds()

    batch = batch.loc[
        (batch["trip_distance"] > 0)
        & (batch["_duration_seconds"] > 0)
    ].copy()

    batch["duration_minutes"] = batch["_duration_seconds"] / 60.0
    batch["pickup_hour"] = (
        batch["tpep_pickup_datetime"].dt.hour.astype("int8")
    )

    return batch


# -----------------------------------------------------------------------------
# Exact duplicate removal
# -----------------------------------------------------------------------------

def add_dedup_bucket(
    batch: pd.DataFrame,
    bucket_count: int,
) -> pd.DataFrame:
    batch = batch.copy()
    row_hash = pd.util.hash_pandas_object(
        batch[TRIP_COLUMNS], index=False
    )
    batch["_dedup_bucket"] = (
        row_hash.astype("uint64") % bucket_count
    ).astype("int32")
    return batch


def dedup_group(batch: pd.DataFrame) -> pd.DataFrame:
    return batch.drop_duplicates(
        subset=TRIP_COLUMNS,
        keep="first",
    )


def exact_deduplicate(
    ds: ray.data.Dataset,
    bucket_count: int,
    batch_size: int,
) -> ray.data.Dataset:
    ds = ds.map_batches(
        add_dedup_bucket,
        fn_kwargs={"bucket_count": bucket_count},
        batch_format="pandas",
        batch_size=batch_size,
        num_cpus=1,
    )

    return (
        ds.groupby(
            "_dedup_bucket",
            num_partitions=bucket_count,
        )
        .map_groups(
            dedup_group,
            batch_format="pandas",
            num_cpus=1,
        )
        .drop_columns(["_dedup_bucket"])
    )


# -----------------------------------------------------------------------------
# Taxi-zone lookup
# -----------------------------------------------------------------------------

def load_lookup(path: str) -> ray.data.Dataset:
    lookup = pd.read_csv(path)
    lookup.columns = [c.strip().replace(" ", "_") for c in lookup.columns]

    lookup = lookup[[
        "LocationID",
        "Borough",
        "Zone",
        "service_zone",
    ]].copy()

    lookup.columns = [
        "PULocationID",
        "PU_Borough",
        "PU_Zone",
        "PU_ServiceZone",
    ]

    lookup["PULocationID"] = pd.to_numeric(
        lookup["PULocationID"], errors="coerce"
    ).astype("Int32")

    lookup = (
        lookup.dropna(subset=["PULocationID"])
        .drop_duplicates(subset=["PULocationID"])
    )
    lookup["PULocationID"] = lookup["PULocationID"].astype("int32")

    return ray.data.from_pandas(lookup)


# -----------------------------------------------------------------------------
# Required custom Python UDF
# -----------------------------------------------------------------------------

def average_speed_mph(
    distance: float | None,
    duration_seconds: float | None,
) -> float | None:
    if distance is None or duration_seconds is None:
        return None
    if distance <= 0 or duration_seconds <= 0:
        return None
    return float(distance) / (float(duration_seconds) / 3600.0)


def speed_udf_batch(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()
    batch["avg_speed_mph"] = [
        average_speed_mph(distance, duration)
        for distance, duration in zip(
            batch["trip_distance"],
            batch["_duration_seconds"],
        )
    ]
    return batch


def speed_native_batch(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()
    duration = batch["_duration_seconds"].to_numpy(dtype="float64")
    distance = batch["trip_distance"].to_numpy(dtype="float64")
    batch["probe_speed"] = np.where(
        duration > 0,
        distance / (duration / 3600.0),
        np.nan,
    )
    return batch


# -----------------------------------------------------------------------------
# Ingestion / configuration
# -----------------------------------------------------------------------------

def ingest(input_path: str) -> ray.data.Dataset:
    files = sorted(
        str(p) for p in Path(input_path).rglob("*.parquet")
    )
    if not files:
        raise FileNotFoundError(
            f"No Parquet files found under {input_path}"
        )

    # Keep the complete source schema until exact deduplication is finished.
    # Projection happens immediately after deduplication.
    return ray.data.read_parquet(
        files,
        override_num_blocks=DEFAULT_READ_BLOCKS,
        concurrency=DEFAULT_READ_CONCURRENCY,
    )


def configure_ray() -> None:
    from ray.data.context import ShuffleStrategy

    ctx = ray.data.DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_disk_based_hash_shuffle = True

    if hasattr(ctx, "isolate_read_workers"):
        ctx.isolate_read_workers = True

    if hasattr(ctx, "shuffle_input_batch_bytes"):
        ctx.shuffle_input_batch_bytes = 256 * 1024 * 1024


# -----------------------------------------------------------------------------
# UDF overhead probe
# -----------------------------------------------------------------------------

def udf_probe(
    clean_ds: ray.data.Dataset,
    rows: int,
    batch_size: int,
) -> dict:
    probe = clean_ds.limit(max(1, int(rows))).materialize()

    start = time.perf_counter()
    native_sum = (
        probe.map_batches(
            speed_native_batch,
            batch_format="pandas",
            batch_size=batch_size,
            num_cpus=1,
        )
        .sum("probe_speed")
    )
    native_seconds = time.perf_counter() - start

    start = time.perf_counter()
    udf_sum = (
        probe.map_batches(
            speed_udf_batch,
            batch_format="pandas",
            batch_size=batch_size,
            num_cpus=1,
        )
        .sum("avg_speed_mph")
    )
    udf_seconds = time.perf_counter() - start

    return {
        "rows": int(rows),
        "native_seconds": native_seconds,
        "python_udf_seconds": udf_seconds,
        "python_udf_overhead_percent": (
            (udf_seconds - native_seconds) / native_seconds * 100.0
            if native_seconds > 0
            else None
        ),
        "native_speed_sum": native_sum,
        "python_udf_speed_sum": udf_sum,
    }


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="DA3408 Assignment 3 - Ray Data benchmark"
    )
    p.add_argument("--address", default="ray-head:6379")
    p.add_argument("--input", required=True)
    p.add_argument("--lookup", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--join-partitions", type=int, default=DEFAULT_JOIN_PARTITIONS)
    p.add_argument("--dedup-buckets", type=int, default=DEFAULT_DEDUP_BUCKETS)
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p.add_argument(
        "--max-rows-per-file",
        type=int,
        default=DEFAULT_MAX_ROWS_PER_FILE,
    )
    p.add_argument("--udf-probe", action="store_true")
    p.add_argument(
        "--udf-probe-rows",
        type=int,
        default=DEFAULT_UDF_PROBE_ROWS,
    )
    return p.parse_args()


# -----------------------------------------------------------------------------
# Main pipeline
# -----------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    if args.join_partitions <= 0:
        raise ValueError("--join-partitions must be positive")
    if args.dedup_buckets <= 0:
        raise ValueError("--dedup-buckets must be positive")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")

    ray.init(address=args.address, ignore_reinit_error=True)
    configure_ray()

    input_rows = parquet_row_count(args.input)
    input_files = len(list(Path(args.input).rglob("*.parquet")))

    print("=" * 72)
    print("DA3408 Ray Data benchmark")
    print(f"Ray version      : {ray.__version__}")
    print(f"Alive nodes      : {sum(n.get('Alive', False) for n in ray.nodes())}")
    print(f"Cluster resources: {ray.cluster_resources()}")
    print(f"Input files      : {input_files}")
    print(f"Input rows       : {input_rows:,}")
    print(f"Read blocks      : {DEFAULT_READ_BLOCKS}")
    print(f"Read concurrency : {DEFAULT_READ_CONCURRENCY}")
    print(f"Dedup partitions : {args.dedup_buckets}")
    print(f"Join partitions  : {args.join_partitions}")
    print("=" * 72)

    try:
        raw = ingest(args.input)

        clean_ds = raw.map_batches(
            clean_batch,
            batch_format="pandas",
            batch_size=args.batch_size,
            num_cpus=1,
        )

        start = time.perf_counter()

        # Exact duplicates are removed before dropping unused source columns.
        cleaned = exact_deduplicate(
            clean_ds,
            args.dedup_buckets,
            args.batch_size,
        )

        # Reduce the width of the large fact table before the expensive join.
        cleaned = cleaned.select_columns([
            *FINAL_TRIP_COLUMNS,
            "duration_minutes",
            "pickup_hour",
            "_duration_seconds",
        ])

        lookup = load_lookup(args.lookup)

        joined = cleaned.join(
            lookup,
            join_type="left_outer",
            num_partitions=args.join_partitions,
            on=("PULocationID",),
        )

        transformed = (
            joined.map_batches(
                speed_udf_batch,
                batch_format="pandas",
                batch_size=args.batch_size,
                num_cpus=1,
            )
            .select_columns(OUTPUT_COLUMNS)
        )

        transformed.write_parquet(
            args.output,
            mode="overwrite",
            compression="snappy",
            max_rows_per_file=args.max_rows_per_file,
        )

        elapsed = time.perf_counter() - start
        output_rows = parquet_row_count(args.output)
        output_files = len(list(Path(args.output).rglob("*.parquet")))

        metrics = {
            "framework": "ray",
            "ray_version": ray.__version__,
            "alive_nodes": sum(n.get("Alive", False) for n in ray.nodes()),
            "cluster_resources": ray.cluster_resources(),
            "input_files": input_files,
            "input_rows": input_rows,
            "output_files": output_files,
            "output_rows": output_rows,
            "ingestion_to_export_seconds": elapsed,
            "read_blocks": DEFAULT_READ_BLOCKS,
            "read_concurrency": DEFAULT_READ_CONCURRENCY,
            "dedup_buckets": args.dedup_buckets,
            "join_partitions": args.join_partitions,
            "batch_size": args.batch_size,
            "max_rows_per_file": args.max_rows_per_file,
        }

        if args.udf_probe:
            metrics["udf_probe"] = udf_probe(
                clean_ds,
                args.udf_probe_rows,
                args.batch_size,
            )

        metrics_path = Path(args.output).parent / "ray_metrics.json"
        metrics_path.write_text(json.dumps(metrics, indent=2, default=str))

        print("\nBENCHMARK COMPLETE")
        print(f"Wall clock ingestion -> export : {elapsed:.3f} s")
        print(f"Output rows                    : {output_rows:,}")
        print(f"Output Parquet files           : {output_files}")
        print(f"Metrics                        : {metrics_path}")

        if args.udf_probe:
            probe = metrics["udf_probe"]
            print(f"UDF probe rows                 : {probe['rows']:,}")
            print(f"Native calculation             : {probe['native_seconds']:.3f} s")
            print(f"Python UDF                     : {probe['python_udf_seconds']:.3f} s")
            print(f"Python UDF overhead            : {probe['python_udf_overhead_percent']:.2f}%")

    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
