# Control Work — Question Pool

> View [Ukrainian version](README_ua.md)

**Discipline:** BIG DATA (Processing of Very Large Data Sets)

**Covers:** Content Module 1 (Big Data Engineering — Lessons 1-1…1-5) and
Content Module 2 (Apache Spark & ML on Big Data — Lessons 2-1…2-3 + the
[spark-tutorial](https://github.com/dmytro-ustynov/spark-tutorial) lab).

---

## How the exam works

- This document is the **full pool of 30 questions**, shared in advance for preparation.
- Questions are split into three tiers: **Easy (1–10)**, **Medium (11–20)**, **Hard (21–30)**.
- On exam day each student receives a **sheet of 3 questions** — one drawn randomly
  from each tier (one Easy + one Medium + one Hard). Ten such sheets are prepared.
- Suggested grading per sheet: **Easy — 3 pts, Medium — 4 pts, Hard — 5 pts** (12 pts total),
  or any scale the instructor prefers.

> **Note for the instructor:** each question below has a collapsible **answer outline**
> — a grading skeleton listing the key points a full answer should contain, not verbatim
> text the student must reproduce. To share a *questions-only* version with students,
> delete the `<details>…</details>` blocks (or export the headings only).

---

# TIER 1 — EASY (recall & definitions)

### Q1. The 6V of Big Data
Name and briefly explain the six characteristics (6V) of Big Data. Give one way each
is visible in the COVID-19 dataset used in the practical.

<details><summary>Answer outline</summary>

**Volume** (amount of data — hundreds of thousands of rows, 60+ columns, 200+ countries),
**Velocity** (speed — data updated daily worldwide),
**Variety** (numeric, text, date fields; structured/semi/unstructured),
**Veracity** (reliability — missing values, uneven reporting between countries),
**Value** (usefulness — supports quarantine/vaccination decisions),
**Variability** (fluctuation — pandemic waves, seasonal peaks).
</details>

### Q2. Data by structure
Define **structured**, **semi-structured**, and **unstructured** data and give one
concrete example of each.

<details><summary>Answer outline</summary>

- **Structured** — fixed schema (tables/rows/columns); e.g. CSV, relational DB, Excel.
- **Semi-structured** — some organization, no rigid schema; e.g. JSON, XML, server logs, API responses.
- **Unstructured** — no predefined model; e.g. text, images, video, audio, social-media posts (est. 70–90 % of all data).
</details>

### Q3. SQL vs NoSQL
State the main differences between SQL (relational) and NoSQL databases, and name the
four NoSQL types with an example of each.

<details><summary>Answer outline</summary>

- **SQL:** fixed schema, tables, ACID guarantees, good for structured data with clear
  relationships; scales vertically. Examples: PostgreSQL, MySQL, Oracle.
- **NoSQL:** flexible/no schema, horizontal scaling across clusters, for large heterogeneous data.
- Types: **document** (MongoDB), **columnar/column-family** (Cassandra, HBase),
  **graph** (Neo4j), **key-value** (Redis).
</details>

### Q4. Batch vs stream processing
Explain the difference between batch and stream processing. Give one tool and one
example use case for each.

<details><summary>Answer outline</summary>

- **Batch:** processes large accumulated volumes periodically; high throughput but latency.
  Tools: Hadoop MapReduce, Spark. Example: daily COVID-19 report for all countries.
- **Stream:** processes data in real time as it arrives; minimal latency, small portions.
  Tools: Kafka, Flink, Storm. Example: real-time patient-temperature monitoring via IoT sensors.
</details>

### Q5. HDFS basics
What are the default HDFS block size and replication factor? What is the role of the
**NameNode** versus the **DataNode**?

<details><summary>Answer outline</summary>

Default block size **128 MB**, default replication factor **3**.
**NameNode (master)** stores metadata only — the namespace (directory tree),
file→block mapping, and block locations; it holds no actual data.
**DataNode (slave)** stores the actual data blocks on local disk and sends heartbeats
and block reports to the NameNode.
</details>

### Q6. MapReduce pipeline
List the stages of the MapReduce pipeline in order and say in one line what each does.

<details><summary>Answer outline</summary>

**Input → Split → Map → Shuffle & Sort → Reduce → Output.**
Input = source data; Split = fixed-size chunks per mapper; Map = emit intermediate
(key, value) pairs; Shuffle & Sort = group all values by key and sort; Reduce = aggregate
values per key; Output = write final results to the distributed file system.
</details>

### Q7. Hadoop core components
Name the three core components of Apache Hadoop and state the responsibility of each.

<details><summary>Answer outline</summary>

- **HDFS** — distributed storage of data across the cluster (blocks + replication).
- **YARN** — cluster resource management and job scheduling.
- **MapReduce** — the batch parallel-computation model (Map → Reduce).
(Spark is a faster in-memory processing engine that can replace MapReduce.)
</details>

### Q8. What is an RDD?
What does **RDD** stand for, and what are its three key properties?

<details><summary>Answer outline</summary>

**Resilient Distributed Dataset** — an immutable, distributed collection processed in parallel.
- **Resilient** — recomputable from its lineage if a partition is lost.
- **Distributed** — split into partitions across cluster nodes.
- **Dataset** — a collection of records (any object type).
</details>

### Q9. Transformations vs actions
In Spark, what is the difference between a **transformation** and an **action**?
Give two examples of each and explain "lazy evaluation."

<details><summary>Answer outline</summary>

- **Transformations** define a new RDD from an existing one and are **lazy** (not executed
  until an action). Examples: `map`, `filter`, `flatMap`, `reduceByKey`.
- **Actions** trigger execution and return a result to the driver or write to storage.
  Examples: `collect`, `count`, `take`, `reduce`, `saveAsTextFile`.
- **Lazy evaluation:** Spark only records transformations as a DAG (lineage) and computes
  the whole chain when an action is called.
</details>

### Q10. SparkContext vs SparkSession
What is `SparkContext` and what is `SparkSession`? Which is recommended for new
applications and why?

<details><summary>Answer outline</summary>

`SparkContext` (Spark 1.x) — the original low-level entry point: connects to the cluster
manager, creates RDDs, broadcast variables, accumulators (one per JVM).
`SparkSession` (Spark 2.0+) — the **unified** entry point that wraps SparkContext, SQLContext,
and HiveContext and gives access to DataFrames, Datasets, and SQL.
**SparkSession is recommended** for all new code; you still reach the RDD API via
`spark.sparkContext`.
</details>

---

# TIER 2 — MEDIUM (explain, compare, trace)

### Q11. Trace a MapReduce Word Count
For the input below (2 splits), work through Map, Shuffle & Sort, and Reduce and give the
final counts.
`Split 1: "data is big data"`  `Split 2: "big data fast"`

<details><summary>Answer outline</summary>

- **Map:** S1 → (data,1)(is,1)(big,1)(data,1); S2 → (big,1)(data,1)(fast,1).
- **Shuffle & Sort:** big→[1,1], data→[1,1,1], fast→[1], is→[1].
- **Reduce:** **big:2, data:3, fast:1, is:1**.
</details>

### Q12. HDFS blocks & replicas
A 512 MB file is stored in HDFS (block size 128 MB, replication factor 3). How many
blocks and how many total block replicas exist? Explain what **rack awareness** adds.

<details><summary>Answer outline</summary>

512 ÷ 128 = **4 blocks**; 4 × 3 = **12 total replicas**.
**Rack awareness:** HDFS places replicas on different racks so the file survives a whole
rack failing. Typical RF=3 policy: 1st replica on the local-rack node, 2nd on a node in a
different rack, 3rd on another node in that second rack — balancing fault tolerance and
write bandwidth.
</details>

### Q13. Why Spark beats MapReduce
Explain why Spark is faster than MapReduce, especially for **iterative** algorithms
(e.g. machine learning).

<details><summary>Answer outline</summary>

MapReduce writes intermediate results to disk after every stage, so each iteration re-reads
and re-writes HDFS. Spark keeps data **in memory** between operations and can **cache** an
RDD/DataFrame once and reuse it across iterations, avoiding repeated disk I/O — up to ~100×
faster. It also uses an arbitrary **DAG** of stages instead of a fixed 2-phase map→reduce,
and lineage-based fault tolerance instead of replication.
</details>

### Q14. YARN architecture
Describe the roles of the **ResourceManager**, **NodeManager**, **ApplicationMaster**, and
**Container** in YARN.

<details><summary>Answer outline</summary>

- **ResourceManager (master):** global resource allocation via its **Scheduler**, plus an
  **ApplicationsManager** that accepts jobs and restarts failed AMs. Does not monitor tasks.
- **NodeManager (per worker node):** manages containers, monitors their CPU/memory, sends
  heartbeats, kills containers exceeding limits.
- **ApplicationMaster (one per app):** negotiates containers from the RM, launches and
  monitors tasks, re-runs failed ones.
- **Container:** an isolated CPU+memory allocation on a node in which a task/executor runs.
</details>

### Q15. Data locality
What does "move the computation to the data" mean, and why is data locality important in
distributed processing?

<details><summary>Answer outline</summary>

Instead of shipping large data across the network to the program, the framework runs the
program (map task) on the node that already holds that data block. This minimizes network
traffic — the most expensive part of distributed processing — and improves throughput.
Both MapReduce and HDFS reads exploit locality (client reads from the nearest DataNode).
</details>

### Q16. Lazy evaluation, DAG, and the job hierarchy
Why does Spark use lazy evaluation, and what is a DAG? Explain the
Application → Job → Stage → Task hierarchy and what marks a stage boundary.

<details><summary>Answer outline</summary>

Lazy evaluation lets Spark see the **whole** computation before running it, so it can
optimize (pipeline transformations, skip unnecessary work). The recorded plan is a
**DAG** of transformations. A **Job** is triggered by an action; it is divided into
**Stages** bounded by **shuffles**; each Stage has one **Task per partition** running on an
executor. Within a stage, tasks run in parallel with no data exchange.
</details>

### Q17. Fault tolerance: lineage vs replication
Compare how HDFS and Spark achieve fault tolerance. Why is lineage efficient for
compute-heavy workloads?

<details><summary>Answer outline</summary>

**HDFS** replicates each block (default ×3); if a node dies, another replica serves the data
and the NameNode re-replicates lost blocks. **Spark** uses **lineage**: each RDD remembers
the transformations that built it, so a lost partition is **recomputed** from its parent —
no extra storage/network for replication. Lineage is efficient for compute-heavy in-memory
workloads because only the lost partition is recomputed rather than all data being duplicated.
</details>

### Q18. Hadoop ecosystem tools
Describe the purpose of at least four of these tools and their category: Hive, Pig, HBase,
Sqoop, Flume, Kafka, ZooKeeper, Oozie.

<details><summary>Answer outline</summary>

- **Hive** — SQL-like queries on HDFS data (data access).
- **Pig** — high-level scripting language for transformations (data access).
- **HBase** — column-family NoSQL database on HDFS (storage).
- **Sqoop** — transfers data between RDBMS and HDFS (ingestion).
- **Flume** — collects/aggregates log data into HDFS (ingestion).
- **Kafka** — distributed streaming platform for real-time ingestion.
- **ZooKeeper** — coordination/configuration/synchronization service.
- **Oozie** — workflow scheduler that chains Hadoop jobs.
</details>

### Q19. DataFrames vs RDDs
Why prefer the DataFrame API / Spark SQL over raw RDDs for structured data? Name the
optimizer involved and one optimization it performs.

<details><summary>Answer outline</summary>

DataFrames carry schema and are optimized by the **Catalyst** optimizer (RDDs are not — the
user must optimize manually). Catalyst produces logical → optimized → physical plans and
applies optimizations such as **predicate pushdown** (apply filters before groupBy/scan) and
column pruning, so only needed columns/rows are processed. This gives better performance and
more concise code than hand-written RDD pipelines.
</details>

### Q20. Lambda architecture
Draw/describe the Lambda architecture. What does each of its three layers do?

<details><summary>Answer outline</summary>

- **Batch layer** — complete, accurate processing of all historical data.
- **Speed layer** — real-time (stream) processing of new data with low latency.
- **Serving layer** — merges batch and speed results to answer user queries.
It combines batch and stream processing so users get both accurate historical views and
up-to-date real-time results.
</details>

---

# TIER 3 — HARD (design, calculate, multi-concept)

### Q21. The computational function problem
Explain the computational function problem. Why is `sum()` decomposable across partitions
but `median()` is not? List the mechanisms Spark uses to reduce shuffle cost.

<details><summary>Answer outline</summary>

Computing `f(D)` over data distributed across nodes is hard because moving data is expensive
and not every function combines from partial results.
- **`sum`** is **decomposable**: partial sums per partition, then sum the partials.
  **`mean`** is partially decomposable (needs sum + count). **`median`** is **not**
  decomposable — it needs the full sorted dataset, forcing a shuffle of all data.
- Spark reduces shuffle via: **lazy evaluation + DAG optimization**; distinguishing
  **narrow** (map, filter — no shuffle) vs **wide** (groupByKey, join — shuffle) dependencies
  and pipelining narrow ones; **partitioning** to co-locate related keys; **broadcast
  variables** for small lookup data; and **accumulators** for distributed counters.
</details>

### Q22. Design a brute-force detector (Structured Streaming)
Using Spark Structured Streaming on the `security-events` Kafka topic, design a brute-force
login detector. Specify the window, threshold, watermark, why you cannot use an exact
distinct count, and how you write alerts to PostgreSQL.

<details><summary>Answer outline</summary>

Filter `event_type = "auth_attempt"` and `result = "failed"`, then group by a **sliding
window** (e.g. `window(timestamp, "2 minutes", "30 seconds")`), `source_ip`, and
`destination_ip`; `count()` and keep groups with `count >= 5`. Add
`.withWatermark("timestamp", "…")` so late events are handled and old window state is dropped.
Exact `distinct` is unsupported on streaming DataFrames, so use **`approx_count_distinct`**
for unique users/sources. JDBC can't be a streaming sink directly, so write with
**`foreachBatch`** (append output mode) plus a **checkpointLocation** into
`brute_force_alerts`. Expected: alerts within 2–3 min of attack start.
</details>

### Q23. Watermarking and late data
What is a watermark in Structured Streaming and what problem does it solve? How do the
output modes (append / update / complete) interact with it, and what is the trade-off in
choosing the watermark duration?

<details><summary>Answer outline</summary>

A **watermark** = event-time threshold `max(seen event time) − delay`; Spark assumes no data
older than the watermark will arrive, so it can **finalize windowed aggregations** and drop
their state (bounding memory). Events older than the watermark are dropped as "too late."
**Append** mode emits a window's result only once it is finalized (after the watermark passes)
— common for windowed aggregations; **update** emits changed rows incrementally; **complete**
re-emits the whole result table (no state dropped). **Trade-off:** a longer watermark catches
more late data but delays finalization and keeps more state in memory; a shorter watermark is
faster/leaner but drops more late events.
</details>

### Q24. Full HDFS write path and failure recovery
Walk through what happens when a client writes a file to HDFS (block pipeline, NameNode role,
replica placement). Then explain what happens if a whole rack loses power.

<details><summary>Answer outline</summary>

1. Client asks the **NameNode** to create the file; NameNode returns a list of DataNodes for
   the first block. 2. Client streams the block to DataNode 1, which **pipelines** it to
   DataNode 2, then to DataNode 3; ACKs flow back through the pipeline. 3. Actual data never
   passes through the NameNode (metadata only). 4. Replicas are placed with **rack awareness**
   (local rack + a second rack).
If a **rack** loses power, blocks remain available from replicas on other racks (that is why
the 2nd/3rd replicas sit on a different rack). The NameNode detects missing **heartbeats** and
instructs surviving DataNodes to create new replicas to restore replication factor 3. No data
is lost as long as ≥1 replica of each block survives.
</details>

### Q25. YARN application lifecycle & schedulers
Describe the YARN application lifecycle from submission to completion. Then compare the FIFO,
Capacity, and Fair schedulers and when each is appropriate.

<details><summary>Answer outline</summary>

**Lifecycle:** client submits app to RM → RM asks a NodeManager to allocate a container and
launches the **ApplicationMaster** → AM registers with RM and requests resources → RM
allocates containers per scheduling policy → AM tells NodeManagers to launch task containers →
tasks report progress to AM → on completion AM notifies RM and releases resources.
**Schedulers:** **FIFO** — single queue, first-come-first-served (a big job blocks everything;
testing only). **Capacity** — multiple queues with guaranteed minimum capacity, can borrow idle
resources (multi-tenant production; Apache default). **Fair** — shares resources equally among
running apps (interactive workloads where each job should get a fair share).
</details>

### Q26. Spark on a cluster: stages and dependencies
Explain how a Spark application runs on a cluster (driver, executors, cluster manager) and how
a job is broken into stages and tasks. What creates a stage boundary? Give a narrow- and a
wide-dependency example.

<details><summary>Answer outline</summary>

The **driver** runs `main()`, builds the DAG, negotiates resources with the **cluster manager**
(YARN/K8s/Standalone), and schedules **tasks** on **executors** (JVMs on worker nodes that run
tasks and cache data). An **action** triggers a **Job**; the DAG scheduler splits it into
**Stages** at **shuffle boundaries**; each stage runs one **Task per partition**.
A **shuffle** (wide dependency) marks the boundary. **Narrow** dependencies — `map`, `filter`
(each parent partition feeds one child partition, pipelined in one stage). **Wide** dependencies
— `groupByKey`, `reduceByKey`, `join` (data must be exchanged across partitions → new stage).
</details>

### Q27. Scaling from 205 MB to 205 GB & master URLs
Your NASA-log Spark job runs on ~205 MB locally. What changes if the dataset becomes 205 GB —
the code, the cluster, or both? Explain the master URLs `local[*]`, `local[N]`, `yarn`,
`k8s://…` and when each is appropriate.

<details><summary>Answer outline</summary>

The **application code stays essentially the same** (Spark's abstractions are scale-independent);
what changes is the **cluster/deployment**: you move from a single machine to a multi-node
cluster, run on YARN/Kubernetes, add executors/memory, ensure enough partitions and shuffle
capacity, and read from HDFS/S3 rather than a local file. Cache selectively (205 GB won't fit
in RAM). **Master URLs:** `local[*]` = run locally using all cores (dev/testing);
`local[N]` = local with exactly N worker threads; `yarn` = run on a Hadoop YARN cluster
(most common production); `k8s://…` = run on a Kubernetes cluster (containerized deployment).
</details>

### Q28. Distributed classification with MLlib
How does Spark MLlib train a classifier in a distributed, data-parallel way? Why does Spark's
in-memory caching matter here? Compare Logistic Regression, Random Forest, and Naive Bayes and
say when each is the best choice.

<details><summary>Answer outline</summary>

**Data-parallel training:** data is partitioned across nodes; each executor computes **local
gradients** on its partition; gradients are **aggregated (reduced)** across nodes; model
parameters are **updated**; steps repeat until convergence. Because training is **iterative**,
caching the training data in memory across iterations avoids re-reading from disk each pass —
exactly where Spark beats MapReduce.
- **Logistic Regression** — linear, interpretable; good baseline for binary/multiclass, scales
  via distributed gradient descent.
- **Random Forest** — ensemble of trees trained in parallel on samples; high accuracy, reduced
  overfitting, non-linear relationships + feature importance.
- **Naive Bayes** — probabilistic, single-pass count aggregation; very fast; great for text
  classification / high-dimensional sparse data.
</details>

### Q29. Hadoop Streaming — write and explain
Explain how Hadoop Streaming works (I/O contract, key format, who sorts). Then write a Python
mapper and reducer that compute **total population per country** from CSV lines
`city,country,continent,population,lat,lng`. Why test locally first?

<details><summary>Answer outline</summary>

**Streaming** lets you write MapReduce in any language: mapper and reducer are programs reading
`stdin` and writing `stdout`; the mapper emits `key\tvalue` (tab-separated); the **framework
sorts** all mapper output by key before the reducer, which receives keys grouped together.
```python
# mapper.py
import sys
for line in sys.stdin:
    line = line.strip()
    if line.startswith("city,"):      # skip header
        continue
    p = line.split(",")
    print(f"{p[1]}\t{p[3]}")           # country, population
```
```python
# reducer.py
import sys
cur, total = None, 0
for line in sys.stdin:
    key, val = line.strip().split("\t", 1)
    val = int(val)
    if key == cur:
        total += val
    else:
        if cur is not None: print(f"{cur}\t{total}")
        cur, total = key, val
if cur is not None: print(f"{cur}\t{total}")
```
Test locally (`cat cities.csv | python mapper.py | sort | python reducer.py`) to catch logic/
parsing bugs cheaply before submitting a slow cluster job. **Pro:** any language / fast to
write; **con:** slower and less type-safe than native Java MapReduce.
</details>

### Q30. DDoS vs brute-force detection & the end-to-end pipeline
Contrast the streaming detection logic for a **DDoS** attack versus a **brute-force** attack
(event type, window, threshold, metrics). Why does streaming forbid an exact distinct count,
and what is used instead? Sketch the full data pipeline used in the lab.

<details><summary>Answer outline</summary>

- **Brute force:** `event_type=auth_attempt`, `result=failed`, grouped by `source_ip` +
  `destination_ip` over a ~2-min sliding window; alert when failed count ≥ threshold (e.g. 5).
- **DDoS:** `event_type=network_connection`, grouped by **target** `destination_ip` over a
  ~1-min window; alert when `request_count ≥ ~100`, and track `approx_count_distinct(source_ip)`
  as unique sources — DDoS is high **volume from many sources to one target**, brute force is
  many **failed auths from one source**.
- Streaming does not support exact `count(distinct …)` (would require unbounded state), so
  **`approx_count_distinct`** (HyperLogLog) is used.
- **Pipeline:** Log Generator (Node.js) → **Kafka** (`security-events`) → **Spark Structured
  Streaming** (parse `from_json`, window + watermark aggregations) → **PostgreSQL** via
  `foreachBatch` (`brute_force_alerts`, `ddos_alerts`, …), with the Spark UI / Kafka UI / pgAdmin
  for monitoring.
</details>

---

## Coverage map (for the instructor)

| Tier | Questions | Main sources |
|------|-----------|--------------|
| Easy | 1–4 | Lesson 1-2 (6V, data types, SQL/NoSQL, batch/stream) |
| Easy | 5–7 | Lessons 1-3, 1-4 (HDFS, MapReduce, Hadoop core) |
| Easy | 8–10 | Lesson 2-1 (RDD, transformations/actions, SparkContext/Session) |
| Medium | 11–12 | Lessons 1-3, 1-4 (MapReduce trace, HDFS blocks) |
| Medium | 13, 16–17, 19 | Lesson 2-1 (Spark vs MR, DAG, lineage, DataFrames) |
| Medium | 14–15 | Lessons 1-4, 1-3 (YARN, data locality) |
| Medium | 18, 20 | Lessons 1-4, 1-2 (ecosystem, Lambda) |
| Hard | 21, 26, 28 | Lesson 2-1 (function problem, stages, MLlib) |
| Hard | 22–23, 30 | Lesson 2-3 + spark-tutorial (streaming, watermark, DDoS/BF) |
| Hard | 24–25 | Lessons 1-3, 1-4 (HDFS write path, YARN lifecycle) |
| Hard | 27, 29 | Lessons 2-2, 1-5 (scaling & master URLs, Hadoop Streaming) |
