# DA3408_3

# Spark vs. Ray — DA3408 Assignment 3

This repository contains the implementation and benchmark report for **DA3408 Assignment 3: Spark vs. Ray — The Data Engineering Duel**.

The project compares Apache Spark and Ray Data on a distributed preprocessing pipeline over NYC Yellow Taxi data.

## Project files

```text
.
├── spark_clean.py
├── ray_clean.py
└── DA3408_Spark_vs_Ray_Academic_Report_Final.pdf
```

### `spark_clean.py`
PySpark implementation of the preprocessing pipeline.

### `ray_clean.py`
Ray Data implementation of the same broad preprocessing pipeline.

### `DA3408_Spark_vs_Ray_Academic_Report_Final.pdf`
Final 5-page academic benchmark report containing the methodology, results, plots, tuning observations, UDF analysis, and cluster screenshots.

---

## Dataset

The benchmark uses **32 monthly NYC Yellow Taxi Parquet files** together with the taxi-zone lookup CSV.

The captured dataset inventory contains:

- **119,595,677 input rows**
- **1,940.21 MB** of Parquet data
- monthly files spanning **January 2024 to August 2026**
- schema evolution from 19 columns to 20/21 columns in later files

For benchmark consistency, the implementations operate on the **19 columns common to all monthly files**.

The taxi-zone lookup is joined using `PULocationID`.

---

## Pipeline

Both implementations follow the same main logical sequence:

```text
Parquet ingestion
        ↓
timestamp normalization
        ↓
required-null filtering
        ↓
positive distance / duration filtering
        ↓
exact duplicate removal
        ↓
taxi-zone lookup join
        ↓
average speed feature
        ↓
Snappy Parquet export
```

The derived speed feature is:

```text
avg_speed_mph = trip_distance / duration_in_hours
```

Invalid non-positive distance/duration values are not used for the speed calculation.

---

## Cluster setup

The benchmark was run with two workers for each framework.

### Spark

- Spark 3.5.6
- 2 workers
- 4 cores per worker
- 12 GiB memory per worker
- 8 worker cores total

Spark Master:

```text
spark://spark-master:7077
```

### Ray

- Ray 2.59.0
- 1 head + 2 workers
- 4 CPUs per worker
- 8 cluster CPUs
- approximately 32 GB cluster memory reported by Ray
- approximately 13.7 GB object-store memory

Ray address:

```text
ray-head:6379
```

The Docker cluster configuration and network setup were done manually.

---

## Running the Spark benchmark

The Spark containers must have the project and dataset mounted at the paths used below.

From the Spark master container:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /ray/assignment3/spark_clean.py \
  --input /ray/data/trips \
  --lookup /ray/data/taxi_zone_lookup.csv \
  --output /ray/data/output_spark
```

The script reports:

- input row count
- output row count
- output Parquet file count
- ingestion-to-export wall-clock time
- framework/configuration information

A successful run prints:

```text
BENCHMARK COMPLETE
```

---

## Running the Ray benchmark

Run the Ray implementation from the Ray head container:

```bash
docker exec -it docker-ray-head-1 \
python /ray/assignment3/ray_clean.py \
  --address ray-head:6379 \
  --input /ray/data/trips \
  --lookup /ray/data/taxi_zone_lookup.csv \
  --output /ray/data/ray_output \
  --join-partitions 256
```

The reported final Ray benchmark used **256 join partitions**.

The Ray configuration also uses:

- 8 read blocks
- 4 concurrent Parquet readers
- Shuffle V2
- disk-based hash shuffle
- isolated read workers

---

## UDF probe

Both implementations include a 1,000,000-row real-data probe for comparing native/vectorized computation with the custom Python speed transformation.

Spark example:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /ray/assignment3/spark_clean.py \
  --input /ray/data/trips \
  --lookup /ray/data/taxi_zone_lookup.csv \
  --output /ray/data/udf_probe_spark \
  --udf-probe
```

Ray example:

```bash
docker exec -it docker-ray-head-1 \
python /ray/assignment3/ray_clean.py \
  --address ray-head:6379 \
  --input /ray/data/trips \
  --lookup /ray/data/taxi_zone_lookup.csv \
  --output /ray/data/ray_udf_probe \
  --udf-probe \
  --udf-probe-rows 1000000
```

The 1M rows are taken from the real dataset; this is a controlled probe and does not replace the full benchmark.

---

## Reported benchmark results

### End-to-end benchmark

| Metric | Spark | Ray |
|---|---:|---:|
| Input rows | 119,595,677 | 119,595,677 |
| Output rows | 115,570,482 | 115,570,482 |
| Ingestion → export | **300.539 s** | **562.993 s** |
| Output Parquet files | 128 | 209 |

Spark was approximately **1.87× faster** in the selected end-to-end runs.

### Spark UDF probe

| Measurement | Time |
|---|---:|
| Native Spark arithmetic | 0.234 s |
| Python UDF | 1.184 s |
| Reported overhead | 406.90% |

The numeric Ray UDF timing result was not retained, so no Ray UDF percentage is reported in the benchmark.

---

## Ray tuning

The main tuning work focused on reducing memory pressure during distributed execution.

The Ray implementation was changed to use controlled read parallelism, disk-backed shuffle, Shuffle V2, isolated read workers, and different join partition counts.

Observed runs included:

| Join partitions | Wall clock |
|---:|---:|
| 32 | 536.110 s |
| 256 | 547.221 s |
| Later 256-partition run | 562.993 s |

Increasing join partitions from 32 to 256 did not remove the observed memory-pressure behaviour.

During Ray execution, nodes reached approximately 13.8–13.9 GB of 14.43 GB memory usage, which caused worker killing and task retries around the join shuffle/reduce stage.

---

## Resource monitoring

Peak CPU and memory observations were collected from the running Docker containers with:

```bash
docker stats
```

Spark traces showed worker CPU usage reaching roughly:

- Spark worker 1: 965.86%
- Spark worker 2: 1040.49%

Memory in the corresponding monitoring trace reached about:

- Spark worker 1: 1.899 GiB
- Spark worker 2: 1.925 GiB

Ray showed stronger memory pressure during shuffle/reduce, including the approximately 13.8–13.9 GB node-level usage events described above.

CPU percentages above 100% are aggregate multi-core container usage.

---

## Cluster and application evidence

The report contains the required cluster screenshots:

- Spark Master UI showing **2 active workers**
- Ray Dashboard showing the **head and 2 workers**

Additional screenshots in the report show:

- Spark 4040 executor/application information
- Ray full-dataset runtime
- Ray UDF-probe runtime
- Ray output verification

---

## AI usage

AI assistance was used as part of the development process, but the cluster and network configuration were performed manually.

The main implementation was developed from the Spark version first and then translated into Ray Data. AI assistance was used selectively for:

- translating PySpark operations into Ray Data equivalents
- debugging Python/API errors
- reasoning about memory pressure and execution plans
- suggesting targeted tuning changes such as limiting read parallelism, using disk-backed shuffle, and changing join partition counts

The reported benchmark numbers were obtained from actual executions. No synthetic performance values were generated.

---

## Notes on parity

Both selected runs produced the same output row count:

```text
115,570,482
```

The preprocessing logic is intended to be equivalent, but the current Ray implementation narrows the retained trip columns before the join, so the final output schema is not identical to Spark's.

For this reason, the benchmark should be described as matching in **logical processing and output row count**, rather than claiming full record-level/schema equality.

---

## References

- DA3408 Assignment 3 handout
- Apache Spark documentation
- Ray Data documentation
