# Lesson 2-2. Setting Up Apache Spark

> View [Ukrainian version](README_ua.md)

**Discipline:** BIG DATA (Processing of Very Large Data Sets)

**Content Module 2:** Apache Spark and Machine Learning on Big Data

**Type:** Practical

**Duration:** 4 hours (2 sessions × 2 hours)

**Maximum score:** 6 points

---

## Learning Objectives

After completing both sessions, students should be able to:

- install Apache Spark / PySpark on MacOS, Linux, or Windows;
- launch a SparkSession and inspect the Spark Web UI;
- distinguish SparkContext and SparkSession in working code;
- apply RDD transformations and actions to a real-world dataset;
- use the DataFrame API and Spark SQL to query structured data;
- explain why Spark outperforms MapReduce on the same workload.

---

## Prerequisites

- Completed Lesson 2-1 (theory: architecture, RDD, transformations vs actions)
- Python 3.10+ installed
- Java 11 or 17 installed (`java -version` should print a version)
- ~2 GB of free disk space (Spark + dataset)
- Optional: Docker installed (for Session 2 preview of `spark-tutorial`)

---

## Dataset

We will use the **NASA-HTTP access log** from the [Internet Traffic Archive](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html) — two months (July and August 1995) of all HTTP requests to the NASA Kennedy Space Center web server. This is a classic Spark/Hadoop teaching dataset:

- ~3.4 million requests total (~205 MB uncompressed)
- Standard NCSA Common Log Format
- Real-world messy data: missing fields, malformed lines, varied URLs

Each line looks like:

```
piweba3y.prodigy.com - - [01/Aug/1995:00:00:09 -0400] "GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0" 200 4085
```

---

## Grading

| Session | Focus | Points |
|---------|-------|--------|
| Session 1 | Install Spark, first SparkSession, SparkContext vs SparkSession | 3 |
| Session 2 | RDD + DataFrame + Spark SQL on NASA logs, spark-tutorial preview | 3 |
| **Total** | | **6** |

Each session is graded on:
- Working code submitted (`.py` files or notebook)
- Screenshots: PySpark shell output, Spark Web UI, query results
- Short written answers to control questions at the end of each session

---

# Session 1: Installing Spark and First Steps (2 hours)

**Points: 3**

---

## 1.1. Installing Apache Spark (30 min)

You may pick **one** of three installation paths. Pick the one that matches your machine — all three give you a working `pyspark` for the rest of the lesson.

### Path A — `pip install pyspark` (recommended for MacOS / Linux / Windows)

The simplest option. PySpark ships with a bundled Spark distribution, so you do not need to download Spark separately.

```bash
# Verify Python and Java first
python3 --version    # 3.10 or newer
java -version        # 11 or 17

# Install PySpark in a virtual environment
python3 -m venv ~/spark-env
source ~/spark-env/bin/activate         # Windows: ~/spark-env/Scripts/activate
pip install --upgrade pip
pip install pyspark==3.5.1 jupyter
```

Verify:

```bash
pyspark --version
# Welcome to
#       ____              __
#      / __/__  ___ _____/ /__
#     _\ \/ _ \/ _ `/ __/  '_/
#    /___/ .__/\_,_/_/ /_/\_\   version 3.5.1
```

### Path B — Download the full Spark distribution (MacOS / Linux)

Use this if you want the full `spark-shell` (Scala) and `spark-submit` toolchain.

```bash
# 1. Download
cd ~
curl -O https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
mv spark-3.5.1-bin-hadoop3 spark

# 2. Add to PATH (append to ~/.zshrc or ~/.bashrc)
echo 'export SPARK_HOME=~/spark' >> ~/.zshrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.zshrc
echo 'export PYSPARK_PYTHON=python3' >> ~/.zshrc
source ~/.zshrc

# 3. Verify
spark-shell --version
pyspark --version
```

### Path C — Standalone Jupyter+Spark container (Windows users with Docker, or anyone who hates installs)

Use the official **`quay.io/jupyter/pyspark-notebook`** image from the Jupyter Project — a single container with Python, Java, Spark, and Jupyter Lab pre-installed (~1 GB). No Kafka, Postgres, or other extras you don't need yet.

```bash
mkdir -p ~/lesson2-2 && cd ~/lesson2-2

docker run -d --name pyspark-lab \
  -p 8888:8888 -p 4040:4040 \
  -v "$PWD":/home/jovyan/work \
  quay.io/jupyter/pyspark-notebook:latest

# Get the Jupyter URL with the access token
docker logs pyspark-lab 2>&1 | grep -E "http://127.0.0.1:8888"
```

Open the printed URL in your browser. Your local `~/lesson2-2` is mounted at `/home/jovyan/work` inside the container, so any notebooks/scripts you save there live on your host machine.

> The full `spark-tutorial` stack (Kafka + PostgreSQL + log-generator) is overkill for Sessions 1 and most of Session 2 — we only start it at the very end in Section 2.5, as a preview for Lesson 2-3.

---

### ✅ Checkpoint 1.1

Submit a screenshot of `pyspark --version` (or, for Path C, the running Jupyter container) showing **Spark 3.5.x**.

---

## 1.2. First SparkSession (20 min)

Create a working directory for this lesson:

```bash
mkdir -p ~/lesson2-2 && cd ~/lesson2-2
```

Save the following as `hello_spark.py`:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HelloSpark")
    .master("local[*]")              # Use all local CPU cores
    .config("spark.ui.port", "4040") # Web UI port
    .getOrCreate()
)

# A SparkSession transparently wraps a SparkContext
sc = spark.sparkContext

print(f"Spark version: {spark.version}")
print(f"Application ID: {sc.applicationId}")
print(f"Default parallelism: {sc.defaultParallelism}")
print(f"Master: {sc.master}")

# Quick smoke test
nums = sc.parallelize(range(1, 1_000_001))
print(f"Sum of 1..1_000_000 = {nums.sum()}")

# Keep alive so we can inspect the Spark UI
input("Press Enter to exit (Spark UI: http://localhost:4040)...")
spark.stop()
```

Run it:

```bash
python hello_spark.py
```

Expected output (numbers may differ):

```
Spark version: 3.5.1
Application ID: local-1718000000000
Default parallelism: 8
Master: local[*]
Sum of 1..1_000_000 = 500000500000
Press Enter to exit (Spark UI: http://localhost:4040)...
```

While the script is paused, open **http://localhost:4040** in your browser. You should see the Spark Web UI with the `HelloSpark` application, one completed job, and the executor tab showing your local executor.

---

### ✅ Checkpoint 1.2

Submit:
- Console output of `hello_spark.py`
- Screenshot of the Spark Web UI **Jobs** tab showing the completed job

---

## 1.3. SparkContext vs SparkSession in Practice (30 min)

In Lesson 2-1 you learned the conceptual difference. Now let's see it in code.

Save as `context_vs_session.py`:

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext

# --- Modern way: create a SparkSession (recommended) ---
spark = SparkSession.builder.appName("ContextDemo").master("local[*]").getOrCreate()

# Get the SparkContext from the session — same JVM-wide singleton
sc = spark.sparkContext
print("Same SparkContext instance:", sc is SparkContext.getOrCreate())

# RDD API — accessed through SparkContext
rdd = sc.parallelize([("apple", 3), ("banana", 5), ("apple", 2)])
print("RDD reduceByKey:", rdd.reduceByKey(lambda a, b: a + b).collect())

# DataFrame API — accessed through SparkSession
df = spark.createDataFrame(
    [("apple", 3), ("banana", 5), ("apple", 2)],
    ["fruit", "qty"],
)
df.groupBy("fruit").sum("qty").show()

# SQL API — also through SparkSession
df.createOrReplaceTempView("fruits")
spark.sql("SELECT fruit, SUM(qty) AS total FROM fruits GROUP BY fruit").show()

spark.stop()
```

Run it. The key takeaways:

| What you want to do | Use |
|--|--|
| Low-level RDD operations | `spark.sparkContext` (or `sc`) |
| DataFrames, SQL, Hive, structured streaming | `spark` (SparkSession) directly |
| Read/write files in any format | `spark.read.*` / `df.write.*` |
| Manage lifecycle | `spark.stop()` (stops the wrapped SparkContext too) |

**Question to think about:** Why did Spark 2.0 unify these into one entry point? Hint — what would happen if you had a separate `SparkContext`, `SQLContext`, and `HiveContext` and they each cached their own metadata?

---

### ✅ Checkpoint 1.3

Submit `context_vs_session.py` and its output. Briefly answer (2–3 sentences) the question above.

---

## 1.4. Control Questions — Session 1 (10 min)

Answer in your report:

1. What does `local[*]` mean as a Spark master URL? When would you use `local[4]` vs `local[*]`?
2. What does `getOrCreate()` do, and why is it preferred over `SparkSession()`?
3. After you call `spark.stop()`, what happens to the Spark UI on port 4040?
4. Name two operations you can only do via `SparkContext` (not `SparkSession`).

---

# Session 2: RDDs and DataFrames on Real Data (2 hours)

**Points: 3**

---

## 2.1. Download the NASA HTTP Logs (10 min)

```bash
cd ~/lesson2-2
mkdir -p data && cd data

# July 1995 (~21 MB compressed, ~167 MB uncompressed)
curl -O https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
gunzip NASA_access_log_Jul95.gz

# August 1995 (smaller — server crashed mid-month)
curl -O https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
gunzip NASA_access_log_Aug95.gz

# Verify
wc -l NASA_access_log_*95
# Should print ~1.9M lines for Jul, ~1.5M for Aug
head -3 NASA_access_log_Jul95
```

> **Tip:** if `ita.ee.lbl.gov` is blocked, the same files are mirrored on Kaggle as the [NASA HTTP Access Log dataset](https://www.kaggle.com/datasets/souhagaa/nasa-access-log-dataset-1995). Download the two `.gz` files, place them in `~/lesson2-2/data/`, and gunzip.

---

## 2.2. RDD Word Count (compare with Hadoop MapReduce) (25 min)

Recall the Word Count we ran on Hadoop in Lesson 1-5. Same logic in Spark — count how many requests each client made.

Save as `rdd_top_clients.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NASA-RDD").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# 1. Load both months as one RDD of raw lines
lines = sc.textFile("data/NASA_access_log_Jul95,data/NASA_access_log_Aug95")
print(f"Total lines: {lines.count():,}")

# 2. Extract the host (the first whitespace-separated field)
hosts = lines.map(lambda line: line.split(" ", 1)[0])

# 3. (host, 1) pairs → reduce by key → top 10
top_clients = (
    hosts
    .map(lambda h: (h, 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda kv: -kv[1])
    .take(10)
)

print("\nTop 10 client hosts:")
for host, count in top_clients:
    print(f"  {count:>7,}  {host}")

spark.stop()
```

Run it:

```bash
python rdd_top_clients.py
```

You should see something like:

```
Total lines: 3,461,613

Top 10 client hosts:
   17,572  piweba3y.prodigy.com
   11,591  piweba4y.prodigy.com
    9,868  piweba1y.prodigy.com
    ...
```

**Discussion:** Compare with the Hadoop MapReduce job from Lesson 1-5:
- How many lines of code did the Mapper + Reducer require?
- How long did the Hadoop job take vs this Spark job?
- Where does Spark "win" — code volume, runtime, both?

---

### ✅ Checkpoint 2.2

Submit `rdd_top_clients.py` plus the output (top 10 hosts). Briefly note the runtime (use `time python rdd_top_clients.py`).

---

## 2.3. DataFrame API — Parsing Logs into Columns (30 min)

The RDD code is nice, but the data is still just strings. The DataFrame API lets us parse the log into typed columns and run SQL on it.

Save as `df_log_analysis.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, to_timestamp, count, avg, sum as _sum, desc
)

spark = SparkSession.builder.appName("NASA-DF").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load both files at once
raw = spark.read.text("data/NASA_access_log_Jul95,data/NASA_access_log_Aug95")
print(f"Raw lines: {raw.count():,}")

# NCSA Common Log regex:
#   host - - [timestamp] "method path proto" status bytes
LOG_PATTERN = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)$'

logs = (
    raw
    .select(
        regexp_extract("value", LOG_PATTERN, 1).alias("host"),
        regexp_extract("value", LOG_PATTERN, 2).alias("ts_raw"),
        regexp_extract("value", LOG_PATTERN, 3).alias("method"),
        regexp_extract("value", LOG_PATTERN, 4).alias("path"),
        regexp_extract("value", LOG_PATTERN, 6).cast("int").alias("status"),
        regexp_extract("value", LOG_PATTERN, 7).alias("bytes_raw"),
    )
    # Drop malformed lines (regex returned empty host)
    .filter(col("host") != "")
    # Convert types
    .withColumn("ts", to_timestamp("ts_raw", "dd/MMM/yyyy:HH:mm:ss Z"))
    .withColumn("bytes", col("bytes_raw").cast("long"))
    .drop("ts_raw", "bytes_raw")
    .cache()  # we'll query this DataFrame multiple times
)

print(f"Parsed records: {logs.count():,}")
logs.printSchema()
logs.show(5, truncate=False)

# --- Q1: HTTP status code distribution ---
print("\n--- Q1: Status code distribution ---")
logs.groupBy("status").count().orderBy(desc("count")).show()

# --- Q2: Top 10 most-requested paths ---
print("\n--- Q2: Top 10 paths ---")
(logs.groupBy("path")
     .agg(count("*").alias("hits"), _sum("bytes").alias("total_bytes"))
     .orderBy(desc("hits"))
     .show(10, truncate=False))

# --- Q3: 404 errors — which paths cause the most? ---
print("\n--- Q3: Top 10 paths returning 404 ---")
(logs.filter(col("status") == 404)
     .groupBy("path").count()
     .orderBy(desc("count"))
     .show(10, truncate=False))

# --- Q4: Traffic per hour of day ---
print("\n--- Q4: Hits per hour of day ---")
from pyspark.sql.functions import hour
(logs.groupBy(hour("ts").alias("hour"))
     .count()
     .orderBy("hour")
     .show(24))

spark.stop()
```

Run it. Notice:
- We `.cache()` the parsed DataFrame so the four queries reuse the same in-memory result instead of re-parsing 3.4M lines four times.
- The regex drops malformed lines (NASA logs have a few thousand of them — non-ASCII paths, truncated lines, etc.).

---

### ✅ Checkpoint 2.3

Submit `df_log_analysis.py` and a screenshot of the **Storage** tab in the Spark Web UI showing the cached DataFrame.

---

## 2.4. Spark SQL on the Same Data (15 min)

Same data, SQL syntax. Add to the previous script (or save as `sql_log_analysis.py`):

```python
logs.createOrReplaceTempView("logs")

# How many bytes did NASA serve in total?
spark.sql("""
    SELECT
        ROUND(SUM(bytes) / 1024.0 / 1024.0 / 1024.0, 2) AS total_gb,
        COUNT(*)                                        AS total_requests,
        ROUND(AVG(bytes), 0)                            AS avg_bytes_per_request
    FROM logs
    WHERE status = 200
""").show()

# Daily request volume
spark.sql("""
    SELECT
        DATE(ts)        AS day,
        COUNT(*)        AS hits,
        COUNT(DISTINCT host) AS unique_hosts
    FROM logs
    GROUP BY DATE(ts)
    ORDER BY day
""").show(31)

# Show the query plan — Spark SQL is optimized by Catalyst
spark.sql("SELECT host, COUNT(*) FROM logs GROUP BY host ORDER BY 2 DESC LIMIT 10") \
     .explain(mode="formatted")
```

The `explain()` output shows the **physical plan**: notice `HashAggregate` instead of a shuffle-then-group, and the projection is pushed down so only the `host` column is scanned.

---

## 2.5. Preview: spark-tutorial (Streaming + Kafka) (10 min)

The next lesson (2-3) will build on a containerized environment with Spark Structured Streaming, Kafka, and PostgreSQL. Get it running now so you are ready:

```bash
cd ~
git clone https://github.com/dmytro-ustynov/spark-tutorial.git
cd spark-tutorial

# Start Kafka, PostgreSQL, log generator, Jupyter+Spark
docker compose up -d

# Verify all services are healthy
./lab-control.sh status
```

Open these in your browser to confirm the lab is alive:

- **Kafka UI** — http://localhost:8080 — should show a `security-events` topic with messages flowing in
- **Log generator status** — http://localhost:3000/status
- **Jupyter Lab** — http://localhost:8888 (token shown in `docker compose logs jupyter`)

That's all for Session 2. We will write our first streaming query against this Kafka topic in Lesson 2-3.

---

### ✅ Checkpoint 2.5

Submit a screenshot of the Kafka UI showing messages in the `security-events` topic.

---

## 2.6. Control Questions — Session 2 (10 min)

Answer in your report:

1. What is the difference between `cache()` and `persist()`? Which storage level does `cache()` default to?
2. We used `regexp_extract` instead of writing a Python parser inside `.map()`. Why is that better for performance? (Hint: Catalyst, JVM, serialization.)
3. In the DataFrame queries, where does Spark perform a **shuffle**? How could you tell from the Web UI?
4. The NASA logs are ~205 MB total. What changes if the dataset is 205 GB instead — do you change the code, the cluster, both?

---

## Independent Work

**Topic:** Apache Spark setup, RDD vs DataFrame, comparison with MapReduce.

### Tasks

1. **Practical extension** (mandatory):
   - Find the **client host with the most distinct paths visited** in the NASA dataset.
   - Find the **5-minute window with the highest request rate** (hint: `window()` function).
   - Submit code, output, and a 1-paragraph explanation of which API (RDD / DataFrame / SQL) you used and why.

2. **Written report** (mandatory, ~2 pages):
   - Compare the line count and runtime of your RDD top-clients job (Section 2.2) against the Hadoop MapReduce equivalent from Lesson 1-5.
   - Explain `local[*]`, `local[N]`, `yarn`, and `k8s://...` master URLs and when each is appropriate.
   - Describe one situation where dropping to the RDD API is still the right choice in 2026.

### Grading Criteria

| Component | Points |
|-----------|--------|
| Sessions 1 + 2 checkpoints (5 checkpoints × 1 pt) | 5 |
| Control questions (Sessions 1 + 2) | 1 |
| Independent practical extension | +1 (bonus) |
| **Total** | **6 (+1 bonus)** |

---

## Recommended Resources

- [Lesson 2-1 — Apache Spark (theory)](../lesson2-1/README.md)
- [Lesson 1-5 — Hadoop MapReduce practical](../lesson1-5/README.md)
- Apache Spark Documentation — https://spark.apache.org/docs/latest/
- PySpark API Reference — https://spark.apache.org/docs/latest/api/python/
- Spark RDD Programming Guide — https://spark.apache.org/docs/latest/rdd-programming-guide.html
- Spark SQL Guide — https://spark.apache.org/docs/latest/sql-programming-guide.html
- NASA-HTTP dataset description — https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
- spark-tutorial repository — https://github.com/dmytro-ustynov/spark-tutorial
- Damji J.S. et al. — *Learning Spark*, 2nd edition (O'Reilly, 2020), chapters 1–4
