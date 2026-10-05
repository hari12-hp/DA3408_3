# DA3408_3

# Spark vs. Ray — DA3408 Assignment 3

This repository contains the implementation and benchmark report for **DA3408 Assignment 3: Spark vs. Ray — The Data Engineering Duel**.

The project compares Apache Spark and Ray Data on a distributed preprocessing pipeline over NYC Yellow Taxi data.

---

## Project Files

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

### Dataset summary

| Property | Value |
|---|---|
| Input files | 32 Parquet files |
| Input rows | 119,595,677 |
| Parquet size | 1,940.21 MB |
| File range | January 2024 – August 2026 |
| Benchmark schema | 19 columns common to all files |

The monthly files exhibit schema evolution from 19 columns to 20/21 columns in later files. For benchmark consistency, the implementations operate on the **19 columns common to all monthly files**.

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

### Derived feature

The average-speed feature is calculated as:

```text
avg_speed_mph = trip_distance / duration_in_hours
```

Invalid non-positive distance or duration values are not used for the speed calculation.

The taxi-zone lookup enriches the trip records with:

- `PU_Borough`
- `PU_Zone`
- `PU_ServiceZone`

These columns are appended to the trip records; `PULocationID` is retained.

---

# Cluster Setup

The benchmark was run using two workers for each framework.

## Spark

| Configuration | Value |
|---|---|
| Version | Spark 3.5.6 |
| Workers | 2 |
| Cores per worker | 4 |
| Memory per worker | 12 GiB |
| Total worker cores | 8 |

Spark Master address:

```text
spark://spark-master:7077
```

Spark Master UI:

```text
http://localhost:8080
```

Spark Application UI:

```text
http://localhost:4040
```

The Spark Application UI is available while a Spark application is running.

## Ray

| Configuration | Value |
|---|---|
| Version | Ray 2.59.0 |
| Head nodes | 1 |
| Workers | 2 |
| CPUs per worker | 4 |
| Total cluster CPUs | 8 |
| Reported cluster memory | ~32 GB |
| Reported object-store memory | ~13.7 GB |

Ray address:

```text
ray-head:6379
```

Ray Dashboard:

```text
http://localhost:8265
```

The Docker cluster configuration and network setup were performed manually.

---

# Reproducing the Reported Results

The reported results were obtained from the two-worker Docker clusters using the dataset and lookup paths shown above.

There are two **full end-to-end benchmarks**:

1. Spark on the complete 119M-row dataset.
2. Ray on the complete 119M-row dataset.

There are also two separate **1M-row UDF probes** used to investigate the cost of the custom Python speed transformation.

> **Important:** The UDF probe is a separate controlled experiment. It does not replace the 119M-row end-to-end benchmark.

---

## 1. Spark — Full 119M-Row Benchmark

Make sure the Spark Master and both Spark workers are running.

Run from the Spark master container:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /ray/assignment3/spark_clean.py \
  --input /ray/data/trips \
  --lookup /ray/data/taxi_zone_lookup.csv \
  --output /ray/data/output_spark \
  --shuffle-partitions 128
```

The reported Spark benchmark used **128 shuffle partitions**.

### Reported result

| Metric | Result |
|---|---:|
| Input rows | 119,595,677 |
| Output rows | 115,570,482 |
| Ingestion → export | **300.539 s** |
| Output Parquet files | 128 |

A successful run prints:

```text
BENCHMARK COMPLETE
```

---

## 2. Ray — Full 119M-Row Benchmark

Make sure the Ray head and both Ray workers are running.

Run from the Ray head container:

```bash
docker exec -it docker-ray-head-1 \
python /ray/assignment3/ray_clean.py \
  --address ray-head:6379 \
  --input /ray/data/trips \
  --lookup /ray/data/taxi_zone_lookup.csv \
  --output /ray/data/ray_output \
  --join-partitions 256
```

The reported final Ray benchmark specifically used **256 join partitions**.

The Ray configuration also uses:

```text
Read blocks             : 8
Read concurrency        : 4
Shuffle strategy        : ShuffleStrategy.SHUFFLE_V2
Disk-based hash shuffle : enabled
Isolated read workers   : enabled
```

### Reported result

| Metric | Result |
|---|---:|
| Input rows | 119,595,677 |
| Output rows | 115,570,482 |
| Ingestion → export | **562.993 s** |
| Output Parquet files | 209 |

---

## 3. Spark — 1M-Row UDF Probe

The Spark UDF probe uses **1,000,000 rows from the real dataset**.

It compares the same `avg_speed_mph` calculation using:

```text
Native Spark arithmetic
        vs.
Custom Python UDF
```

Run:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /ray/assignment3/spark_clean.py \
  --input /ray/data/trips \
  --lookup /ray/data/taxi_zone_lookup.csv \
  --output /ray/data/udf_probe_spark \
  --udf-probe \
  --udf-probe-rows 1000000
```

### Reported result

| Measurement | Time |
|---|---:|
| Rows | 1,000,000 |
| Native Spark arithmetic | 0.234 s |
| Python UDF | 1.184 s |
| Reported UDF overhead | **406.90%** |

The native implementation and Python UDF compute the same mathematical feature. The probe measures the additional execution cost of using the Python UDF.

The 1M-row probe uses real data and is not a synthetic benchmark.

---

## 4. Ray — 1M-Row UDF Probe

The Ray implementation also contains a 1,000,000-row UDF probe.

Run from the Ray head container:

```bash
docker exec -it docker-ray-head-1 \
python /ray/assignment3/ray_clean.py \
  --address ray-head:6379 \
  --input /ray/data/trips \
  --lookup /ray/data/taxi_zone_lookup.csv \
  --output /ray/data/ray_udf_probe \
  --udf-probe \
  --udf-probe-rows 1000000 \
  --join-partitions 256
```

The probe compares:

```text
Native/vectorized Ray calculation
        vs.
Custom Python speed transformation
```

The Ray UDF-probe job completed successfully, but the final numeric timing output was not retained. Therefore, no Ray UDF timing or overhead percentage is reported.

---

# UDF Probe

The UDF probe is a focused experiment rather than another full ETL benchmark.

The same speed calculation is evaluated in two ways:

```text
1,000,000 real rows
        │
        ├── Native implementation
        │
        └── Python UDF
```

The purpose is to answer:

> **How much additional execution time is introduced by the custom Python UDF?**

For Spark, the retained measurement was:

| Measurement | Time |
|---|---:|
| Native Spark arithmetic | 0.234 s |
| Python UDF | 1.184 s |
| Reported overhead | 406.90% |

The main 119M-row benchmark uses the **custom Python speed UDF** as part of the preprocessing pipeline.

---

# End-to-End Benchmark Results

| Metric | Spark | Ray |
|---|---:|---:|
| Input rows | 119,595,677 | 119,595,677 |
| Output rows | 115,570,482 | 115,570,482 |
| Ingestion → export | **300.539 s** | **562.993 s** |
| Output Parquet files | 128 | 209 |

For the selected completed runs, Spark was approximately **1.87× faster** than Ray.

The primary comparison metric is the wall-clock time measured from ingestion through Parquet export.

---

# Ray Partition Tuning

The Ray join is a distributed key-based join on `PULocationID`.

The number of join partitions was exposed as a parameter because the distributed shuffle was the main memory-intensive part of the Ray execution.

### Observed completed runs

| Join partitions | Wall clock |
|---:|---:|
| 32 | 536.110 s |
| 256 | 547.221 s |
| Later 256-partition run | 562.993 s |

Increasing the join partition count from 32 to 256 did not remove the observed memory-pressure behaviour.

The later 256-partition run at **562.993 s** was used as the selected final Ray benchmark.

Additional tuning included:

- controlled read parallelism
- 8 read blocks
- 4 concurrent Parquet readers
- Shuffle V2
- disk-based hash shuffle
- isolated read workers
- early column projection before the Ray join

---

# Spark vs. Ray Join Behaviour

The taxi-zone lookup is a small table keyed by `LocationID`.

## Spark

Spark broadcasts the small lookup table:

```text
Small lookup table
        ↓
Copied to workers
        ↓
Large trip data remains distributed
```

This avoids a large fact-table shuffle for the lookup join.

## Ray

Ray performs a distributed key-based join:

```text
Large trip data
        ↓
Partition by PULocationID
        ↓
Shuffle across workers
        ↓
Join matching keys
```

This makes the join shuffle a more significant part of the Ray execution.

The Ray execution plan contains `JoinShuffleMap` and `JoinShuffleReduce` stages.

---

# Why Ray Projects to 11 Columns Before the Join

The benchmark begins with the 19 columns common to all input files.

After exact deduplication, the Ray implementation narrows the large trip table to 11 trip fields before the distributed join.

The purpose is to reduce the amount of data carried through the expensive join shuffle.

Conceptually:

```text
19 trip columns
        ↓
Exact deduplication
        ↓
11 trip columns
        ↓
Distributed join
```

Spark instead keeps the 19 retained source fields through the join and broadcasts the small taxi-zone lookup.

This is an implementation difference rather than a change to the logical purpose of the pipeline.

Because the Ray implementation narrows the source projection before the join, its final output schema is narrower than Spark's.

---

# Resource Monitoring

Peak CPU and memory observations were collected from the running Docker containers with:

```bash
docker stats
```

### Spark

Observed worker CPU peaks:

```text
Spark worker 1 : 965.86%
Spark worker 2 : 1040.49%
```

Observed memory in the corresponding monitoring trace:

```text
Spark worker 1 : 1.899 GiB
Spark worker 2 : 1.925 GiB
```

### Ray

Ray showed stronger memory pressure during distributed shuffle/reduce, including node-level usage around:

```text
13.8–13.9 GB used / 14.43 GB available
```

This memory pressure caused Ray worker killing and task retries around the shuffle/reduce stage.

> CPU percentages above 100% represent aggregate usage across multiple cores.

The Docker `stats` observations are workload traces and were not perfectly time-synchronized with every selected benchmark run.

---

# Cluster and Application Evidence

The report contains the required cluster screenshots:

- Spark Master UI showing **2 active Spark workers**
- Ray Dashboard showing the **Ray head and 2 workers**

Additional evidence includes:

- Spark Application UI at port `4040`
- Spark executor/application information
- Ray Dashboard during the full 119M-row dataset run
- Ray Dashboard during the 1M-row UDF probe
- Ray output verification

## Spark Master UI

```text
http://localhost:8080
```

Use this page to verify the two-worker Spark cluster.

## Spark Application UI

```text
http://localhost:4040
```

This page shows application-level information such as jobs, stages, executors, SQL, and task statistics.

The Application UI may disappear after the Spark application terminates.

## Ray Dashboard

```text
http://localhost:8265
```

Use this page to verify the Ray cluster and inspect distributed execution.

---

# Docker Execution

The distributed benchmark was executed using Docker containers.

### Ray cluster

```text
Ray head
├── Ray worker 1
└── Ray worker 2
```

### Spark cluster

```text
Spark Master
├── Spark worker 1
└── Spark worker 2
```

The Docker network and cluster setup were configured manually.

The dataset was mounted into the containers under:

```text
/ray/data/trips
```

The taxi-zone lookup was mounted as:

```text
/ray/data/taxi_zone_lookup.csv
```

---

# AI Usage

AI assistance was used as part of the development process, but the cluster and network configuration were performed manually.

The main implementation was developed from the Spark version first and then translated into Ray Data.

AI assistance was used selectively for:

- translating PySpark operations into Ray Data equivalents
- debugging Python and framework/API errors
- reasoning about execution plans and memory pressure
- suggesting targeted performance-tuning changes

The reported benchmark numbers were obtained from actual executions.

No synthetic benchmark values were generated.

---

# Notes on Parity

Both selected full-data runs produced the same output row count:

```text
115,570,482
```

The preprocessing stages are intended to implement the same logical workflow.

However, the current Ray implementation narrows the retained trip columns before the distributed join, while Spark keeps all 19 retained source fields.

Therefore:

| Property | Status |
|---|---|
| Output row count parity | **Yes** |
| Final output schema parity | **No** |

The results should therefore be described as matching in **logical processing and output row count**, rather than as full record-level or schema-level equality.

---

# Successful and Failed Runs

Some earlier executions encountered failures during development and tuning.

### Spark

One earlier Spark run reached the Parquet export stage but failed while creating a temporary output directory.

### Ray

Ray encountered memory-pressure failures during distributed shuffle/reduce, including worker killing and task retries.

These failures were separate from the completed runs used for the reported benchmark results.

The benchmark figures in this README correspond to the completed runs described above.

---

# Quick Reproduction Summary

## Spark — Full Benchmark

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /ray/assignment3/spark_clean.py \
  --input /ray/data/trips \
  --lookup /ray/data/taxi_zone_lookup.csv \
  --output /ray/data/output_spark \
  --shuffle-partitions 128
```

## Ray — Full Benchmark

```bash
docker exec -it docker-ray-head-1 \
python /ray/assignment3/ray_clean.py \
  --address ray-head:6379 \
  --input /ray/data/trips \
  --lookup /ray/data/taxi_zone_lookup.csv \
  --output /ray/data/ray_output \
  --join-partitions 256
```

## Spark — UDF Probe

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /ray/assignment3/spark_clean.py \
  --input /ray/data/trips \
  --lookup /ray/data/taxi_zone_lookup.csv \
  --output /ray/data/udf_probe_spark \
  --udf-probe \
  --udf-probe-rows 1000000
```

## Ray — UDF Probe

```bash
docker exec -it docker-ray-head-1 \
python /ray/assignment3/ray_clean.py \
  --address ray-head:6379 \
  --input /ray/data/trips \
  --lookup /ray/data/taxi_zone_lookup.csv \
  --output /ray/data/ray_udf_probe \
  --udf-probe \
  --udf-probe-rows 1000000 \
  --join-partitions 256
```

---

# References

- DA3408 Assignment 3 handout
- Apache Spark documentation
- Ray Data documentation
