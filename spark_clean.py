from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType




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

OUTPUT_COLUMNS = [
    *TRIP_COLUMNS,
    "duration_minutes",
    "pickup_hour",
    "PU_Borough",
    "PU_Zone",
    "PU_ServiceZone",
    "avg_speed_mph",
]

DEFAULT_SHUFFLE_PARTITIONS = 128
DEFAULT_MAX_RECORDS_PER_FILE = 2_000_000
DEFAULT_UDF_PROBE_ROWS = 1_000_000




def average_speed_mph(
    distance: float | None,
    duration_seconds: float | None,
) -> float | None:
    if distance is None or duration_seconds is None:
        return None

    if duration_seconds <= 0 or distance <= 0:
        return None

    return float(distance) / (float(duration_seconds) / 3600.0)


speed_udf = F.udf(average_speed_mph, DoubleType())


def build_spark(master: str, shuffle_partitions: int) -> SparkSession:
    return (
        SparkSession.builder
        .appName("DA3408-Spark-Clean")
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config(
            "spark.sql.files.maxPartitionBytes",
            str(128 * 1024 * 1024),
        )
        .config("spark.python.worker.reuse", "true")
        .config("spark.network.timeout", "120s")
        .config("spark.executor.heartbeatInterval", "20s")
        .getOrCreate()
    )


# ============================================================
# Ingestion
# ============================================================

def ingest(spark: SparkSession, input_path: str) -> DataFrame:
    df = spark.read.parquet(input_path)

    missing = [c for c in TRIP_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Required columns missing: {missing}")

    # Same column projection used for Ray.
    return df.select(*TRIP_COLUMNS)



def cleanse(df: DataFrame) -> DataFrame:
    out = (
        df
        # Timestamp normalization
        .withColumn(
            "tpep_pickup_datetime",
            F.to_timestamp("tpep_pickup_datetime"),
        )
        .withColumn(
            "tpep_dropoff_datetime",
            F.to_timestamp("tpep_dropoff_datetime"),
        )

        # Required non-null fields
        .dropna(subset=REQUIRED_NON_NULL)

        # Duration in seconds
        .withColumn(
            "_duration_seconds",
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime"),
        )

        # Same positivity requirements as Ray
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("_duration_seconds") > 0)

        # Derived features
        .withColumn(
            "duration_minutes",
            F.col("_duration_seconds") / F.lit(60.0),
        )
        .withColumn(
            "pickup_hour",
            F.hour("tpep_pickup_datetime"),
        )

        # Exact duplicate removal over the source columns
        .dropDuplicates(TRIP_COLUMNS)
    )

    return out


#taxi zone lookup

def normalize_lookup(lookup: DataFrame) -> DataFrame:
    lower = {
        c.lower().strip().replace(" ", "_"): c
        for c in lookup.columns
    }

    def get(name: str) -> str:
        if name not in lower:
            raise ValueError(
                f"Lookup CSV is missing '{name}'. "
                f"Found: {lookup.columns}"
            )
        return lower[name]

    location_id = get("locationid")
    borough = get("borough")
    zone = get("zone")
    service_zone = get("service_zone")

    return (
        lookup
        .select(
            F.col(location_id)
            .cast("int")
            .alias("PULocationID"),

            F.col(borough)
            .cast("string")
            .alias("PU_Borough"),

            F.col(zone)
            .cast("string")
            .alias("PU_Zone"),

            F.col(service_zone)
            .cast("string")
            .alias("PU_ServiceZone"),
        )
        .dropDuplicates(["PULocationID"])
    )

#transformation

def transform(
    df: DataFrame,
    lookup_path: str,
) -> DataFrame:

    spark = df.sparkSession

    lookup_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(lookup_path)
    )

    lookup = normalize_lookup(lookup_raw)

    # Small dimension table -> broadcast join.
    joined = df.join(
        F.broadcast(lookup),
        on="PULocationID",
        how="left",
    )

    # Custom scalar Python UDF.
    result = joined.withColumn(
        "avg_speed_mph",
        speed_udf(
            F.col("trip_distance"),
            F.col("_duration_seconds"),
        ),
    )

    return result.select(*OUTPUT_COLUMNS)



def udf_probe(
    base_clean: DataFrame,
    rows: int,
) -> dict:

    rows = max(1, int(rows))

    probe = base_clean.limit(rows).cache()

    # Materialize the same real subset once.
    probe.count()

    native_expr = F.when(
        F.col("_duration_seconds") > 0,
        F.col("trip_distance")
        / (
            F.col("_duration_seconds")
            / F.lit(3600.0)
        ),
    )

    # Native Spark arithmetic
    start = time.perf_counter()

    native_sum = (
        probe
        .select(native_expr.alias("speed"))
        .agg(F.sum("speed"))
        .first()[0]
    )

    native_seconds = time.perf_counter() - start

    # Python UDF
    start = time.perf_counter()

    udf_sum = (
        probe
        .select(
            speed_udf(
                F.col("trip_distance"),
                F.col("_duration_seconds"),
            ).alias("speed")
        )
        .agg(F.sum("speed"))
        .first()[0]
    )

    python_udf_seconds = time.perf_counter() - start

    probe.unpersist()

    overhead = (
        (python_udf_seconds - native_seconds)
        / native_seconds
        * 100.0
        if native_seconds > 0
        else None
    )

    return {
        "rows": rows,
        "native_seconds": native_seconds,
        "python_udf_seconds": python_udf_seconds,
        "python_udf_overhead_percent": overhead,
        "native_speed_sum": native_sum,
        "python_udf_speed_sum": udf_sum,
    }
#main 
def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--master",
        default=os.getenv(
            "SPARK_MASTER_URL",
            "spark://spark-master:7077",
        ),
    )

    parser.add_argument("--input", required=True)
    parser.add_argument("--lookup", required=True)
    parser.add_argument("--output", required=True)

    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=int(
            os.getenv(
                "SPARK_SHUFFLE_PARTITIONS",
                DEFAULT_SHUFFLE_PARTITIONS,
            )
        ),
    )

    parser.add_argument(
        "--max-records-per-file",
        type=int,
        default=int(
            os.getenv(
                "MAX_RECORDS_PER_FILE",
                DEFAULT_MAX_RECORDS_PER_FILE,
            )
        ),
    )

    parser.add_argument(
        "--udf-probe",
        action="store_true",
    )

    parser.add_argument(
        "--udf-probe-rows",
        type=int,
        default=int(
            os.getenv(
                "UDF_PROBE_ROWS",
                DEFAULT_UDF_PROBE_ROWS,
            )
        ),
    )

    return parser.parse_args()


def main() -> None:

    args = parse_args()

    spark = build_spark(
        args.master,
        args.shuffle_partitions,
    )

    print("=" * 72)
    print("DA3408 Spark benchmark")
    print(f"Spark version       : {spark.version}")
    print(f"Spark master        : {spark.sparkContext.master}")
    print(f"Input               : {args.input}")
    print(f"Lookup              : {args.lookup}")
    print(f"Output              : {args.output}")
    print(f"Shuffle partitions  : {args.shuffle_partitions}")
    print(f"Max records/file    : {args.max_records_per_file}")
    print("=" * 72)

    try:

        # ----------------------------------------------------
        # Benchmark starts immediately before ingestion
        # ----------------------------------------------------

        benchmark_start = time.perf_counter()

        raw = ingest(
            spark,
            args.input,
        )

        base_clean = cleanse(raw)

        transformed = transform(
            base_clean,
            args.lookup,
        )

        # ----------------------------------------------------
        # Parquet export
        # ----------------------------------------------------

        (
            transformed.write
            .mode("overwrite")
            .option("compression", "snappy")
            .option(
                "maxRecordsPerFile",
                str(args.max_records_per_file),
            )
            .parquet(args.output)
        )

        # Benchmark ends immediately after export.
        total_seconds = (
            time.perf_counter()
            - benchmark_start
        )

        # These checks happen OUTSIDE the benchmark timing.
        output_rows = (
            spark.read
            .parquet(args.output)
            .count()
        )

        output_files = len(
            list(
                Path(args.output).rglob("*.parquet")
            )
        )

        metrics = {
            "framework": "spark",
            "spark_version": spark.version,
            "master": spark.sparkContext.master,
            "input_path": args.input,
            "lookup_path": args.lookup,
            "output_path": args.output,
            "output_files": output_files,
            "output_rows": output_rows,
            "ingestion_to_export_seconds": total_seconds,
            "shuffle_partitions": args.shuffle_partitions,
            "max_records_per_file": args.max_records_per_file,
            "timestamp_utc": time.strftime(
                "%Y-%m-%dT%H:%M:%SZ",
                time.gmtime(),
            ),
        }

        # ----------------------------------------------------
        # Optional UDF probe
        # ----------------------------------------------------

        if args.udf_probe:

            print()
            print("Running 1M-row real-data UDF probe...")

            probe_metrics = udf_probe(
                base_clean,
                args.udf_probe_rows,
            )

            metrics["udf_probe"] = probe_metrics

        metrics_path = str(
            Path(args.output).parent
            / f"{Path(args.output).name}_metrics.json"
        )

        Path(metrics_path).write_text(
            json.dumps(
                metrics,
                indent=2,
                default=str,
            )
        )

        print()
        print("=" * 72)
        print("BENCHMARK COMPLETE")
        print(
            f"Wall clock ingestion -> export : "
            f"{total_seconds:.3f} s"
        )
        print(
            f"Output rows                   : "
            f"{output_rows:,}"
        )
        print(
            f"Output Parquet files          : "
            f"{output_files}"
        )
        print(
            f"Metrics JSON                  : "
            f"{metrics_path}"
        )

        if args.udf_probe:

            p = metrics["udf_probe"]

            print(
                f"UDF probe rows                : "
                f"{p['rows']:,}"
            )
            print(
                f"Native speed time             : "
                f"{p['native_seconds']:.3f} s"
            )
            print(
                f"Python UDF time               : "
                f"{p['python_udf_seconds']:.3f} s"
            )
            print(
                f"Python UDF overhead           : "
                f"{p['python_udf_overhead_percent']:.2f}%"
            )

        print("=" * 72)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
