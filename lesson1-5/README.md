# Lesson 1.5. Setting Up and Working with Apache Hadoop

> View [Ukrainian version](README_ua.md)

**Discipline:** BIG DATA (Processing of Very Large Data Sets)

**Content Module 1:** Big Data Engineering

**Type:** Practical

**Duration:** 6 hours (3 sessions × 2 hours)

**Maximum score:** 10 points

---

## Learning Objectives

After completing all three sessions, students should be able to:

- deploy and configure a Hadoop cluster using Docker;
- perform HDFS operations: create directories, upload/download files, manage permissions and replication;
- write and run MapReduce programs using Hadoop Streaming (Python);
- analyze MapReduce job execution through YARN logs and counters;
- monitor cluster health using Hadoop Web UIs;
- process a real-world dataset using the full Hadoop pipeline.

---

## Prerequisites

- Completed Lessons 1.1–1.4 (theory on MapReduce, HDFS, YARN, Hadoop ecosystem)
- Docker installed and running (at least 4 GB of free RAM)
- Python 3.x installed on your local machine
- Basic command-line skills

---

## Grading

| Session | Focus | Points |
|---------|-------|--------|
| Session 1 | Hadoop setup + HDFS operations | 3 |
| Session 2 | Writing and running MapReduce jobs | 4 |
| Session 3 | Monitoring, real-world dataset processing | 3 |
| **Total** | | **10** |

Each session is graded based on:
- Completing all required tasks (screenshots + command output)
- Submitting working code (Sessions 2–3)
- Answering control questions at the end of each session

---

# Session 1: Hadoop Environment and HDFS Deep Dive (2 hours)

**Points: 3**

---

## 1.1. Setting Up the Hadoop Environment (30 min)

In Lesson 1.4 we briefly used a Docker container. Now we will set up a more robust environment and understand the configuration.

### Step 1. Start the Hadoop Container

```bash
# Pull the image (skip if already done in Lesson 1.4)
docker pull sequenceiq/hadoop-docker:2.7.1

# Remove old container if it exists
docker rm -f hadoop-sandbox 2>/dev/null

# Start with all necessary ports exposed
docker run -it \
  --name hadoop-sandbox \
  -p 9870:50070 \
  -p 8088:8088 \
  -p 19888:19888 \
  -p 8042:8042 \
  sequenceiq/hadoop-docker:2.7.1 \
  /etc/bootstrap.sh -bash
```

### Step 2. Explore Hadoop Configuration Files

Inside the container, examine the key configuration files:

```bash
# Core Hadoop settings
cat $HADOOP_PREFIX/etc/hadoop/core-site.xml
```

**Key properties in `core-site.xml`:**

| Property | Value | Meaning |
|----------|-------|---------|
| `fs.defaultFS` | `hdfs://localhost:9000` | Address of the NameNode |
| `hadoop.tmp.dir` | `/tmp/hadoop-*` | Temporary data directory |

```bash
# HDFS settings
cat $HADOOP_PREFIX/etc/hadoop/hdfs-site.xml
```

**Key properties in `hdfs-site.xml`:**

| Property | Value | Meaning |
|----------|-------|---------|
| `dfs.replication` | `1` | Replication factor (1 for single-node) |
| `dfs.blocksize` | `134217728` | Block size in bytes (128 MB) |
| `dfs.namenode.name.dir` | `file:///...` | Where NameNode stores metadata |
| `dfs.datanode.data.dir` | `file:///...` | Where DataNode stores blocks |

```bash
# YARN settings
cat $HADOOP_PREFIX/etc/hadoop/yarn-site.xml
```

**Key properties in `yarn-site.xml`:**

| Property | Value | Meaning |
|----------|-------|---------|
| `yarn.resourcemanager.hostname` | `localhost` | ResourceManager address |
| `yarn.nodemanager.resource.memory-mb` | varies | Memory available for containers |
| `yarn.nodemanager.aux-services` | `mapreduce_shuffle` | Required for MapReduce |

> **Task 1.1:** Take a screenshot of the `hdfs-site.xml` content showing replication factor and block size.

### Step 3. Verify Cluster Health

```bash
# Detailed HDFS report
hdfs dfsadmin -report

# Safe mode status
hdfs dfsadmin -safemode get

# YARN nodes
yarn node -list

# YARN queue info
yarn queue -status default
```

> **Task 1.2:** Record the output of `hdfs dfsadmin -report`. Note: total capacity, used space, number of DataNodes.

---

## 1.2. HDFS Operations in Depth (50 min)

### Directory and File Management

```bash
# Create a directory structure
hdfs dfs -mkdir -p /user/student/data/raw
hdfs dfs -mkdir -p /user/student/data/processed
hdfs dfs -mkdir -p /user/student/scripts

# List with details (permissions, owner, size)
hdfs dfs -ls -R /user/student/
```

### Creating and Uploading Files

```bash
# Create multiple test files locally
cat > file1.txt << 'EOF'
Apache Hadoop is an open source framework
for distributed storage and processing
of large datasets across clusters
EOF

cat > file2.txt << 'EOF'
HDFS provides high throughput access
to application data and is suitable
for applications with large data sets
EOF

cat > file3.txt << 'EOF'
YARN is the resource management layer
of Hadoop that manages cluster resources
and schedules user applications
EOF

# Upload individual files
hdfs dfs -put file1.txt /user/student/data/raw/
hdfs dfs -put file2.txt /user/student/data/raw/
hdfs dfs -put file3.txt /user/student/data/raw/

# Upload multiple files at once
# hdfs dfs -put file1.txt file2.txt file3.txt /user/student/data/raw/

# Verify uploads
hdfs dfs -ls /user/student/data/raw/
```

### Reading and Downloading Files

```bash
# Display file content
hdfs dfs -cat /user/student/data/raw/file1.txt

# Display first/last lines (useful for large files)
hdfs dfs -head /user/student/data/raw/file1.txt
hdfs dfs -tail /user/student/data/raw/file1.txt

# Download a file from HDFS to local filesystem
hdfs dfs -get /user/student/data/raw/file1.txt ./downloaded_file1.txt
cat ./downloaded_file1.txt

# Merge multiple HDFS files into one local file
hdfs dfs -getmerge /user/student/data/raw/ ./merged_all.txt
cat ./merged_all.txt
```

### File Operations

```bash
# Copy a file within HDFS
hdfs dfs -cp /user/student/data/raw/file1.txt /user/student/data/processed/file1_copy.txt

# Move (rename) a file within HDFS
hdfs dfs -mv /user/student/data/processed/file1_copy.txt /user/student/data/processed/renamed.txt

# Check file sizes
hdfs dfs -du -h /user/student/data/

# Count files and directories
hdfs dfs -count /user/student/data/

# Delete a file
hdfs dfs -rm /user/student/data/processed/renamed.txt

# Delete a directory recursively
# hdfs dfs -rm -r /user/student/data/processed/
```

### Permissions and Ownership

```bash
# Change permissions (similar to Unix chmod)
hdfs dfs -chmod 755 /user/student/data/raw/file1.txt

# Change owner
hdfs dfs -chown student:student /user/student/data/raw/file1.txt

# View permissions
hdfs dfs -ls /user/student/data/raw/
```

### Block Information and File System Check

```bash
# Detailed block information for a file
hdfs fsck /user/student/data/raw/file1.txt -files -blocks -locations

# Full filesystem check
hdfs fsck /

# View file status (block size, replication)
hdfs dfs -stat "%b %r %n" /user/student/data/raw/file1.txt
# %b = file size in bytes, %r = replication factor, %n = filename
```

> **Task 1.3:** Perform the following operations and capture screenshots:
> 1. Create the directory structure `/user/student/project/input` and `/user/student/project/output`
> 2. Create a text file with at least 10 lines of text on any topic
> 3. Upload it to `/user/student/project/input/`
> 4. Run `hdfs fsck` on the uploaded file — note the block count and replication factor
> 5. Use `hdfs dfs -du -h` to show disk usage of `/user/student/`

---

## 1.3. Working with Larger Files (30 min)

Let's generate a larger file to see how HDFS handles data at scale.

```bash
# Generate a ~50 MB text file with random words
python3 -c "
import random
words = ['hadoop', 'spark', 'hdfs', 'yarn', 'mapreduce', 'cluster',
         'data', 'node', 'block', 'replica', 'distributed', 'processing',
         'storage', 'framework', 'batch', 'stream', 'analytics', 'big',
         'fault', 'tolerance', 'scalability', 'throughput', 'latency']
with open('large_file.txt', 'w') as f:
    for i in range(500000):
        line = ' '.join(random.choices(words, k=random.randint(5, 15)))
        f.write(line + '\n')
print('File generated')
" 2>/dev/null || python -c "
import random
words = ['hadoop', 'spark', 'hdfs', 'yarn', 'mapreduce', 'cluster',
         'data', 'node', 'block', 'replica', 'distributed', 'processing',
         'storage', 'framework', 'batch', 'stream', 'analytics', 'big',
         'fault', 'tolerance', 'scalability', 'throughput', 'latency']
with open('large_file.txt', 'w') as f:
    for i in range(500000):
        line = ' '.join(random.choices(words, k=random.randint(5, 15)))
        f.write(line + '\n')
print('File generated')
"

# Check local file size
ls -lh large_file.txt

# Upload to HDFS
hdfs dfs -put large_file.txt /user/student/data/raw/

# Check how HDFS split it
hdfs fsck /user/student/data/raw/large_file.txt -files -blocks -locations

# Disk usage comparison
hdfs dfs -du -h /user/student/data/raw/
```

> **Task 1.4:** Answer the following questions in your report:
> 1. How large is the generated file in bytes?
> 2. How many HDFS blocks does it occupy? Why?
> 3. If the replication factor were 3, how much total storage would this file consume?

---

## Session 1 — Control Questions

Before leaving, answer these questions (written or orally to the instructor):

1. What is the purpose of each Hadoop configuration file (`core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`)?
2. What is the difference between `hdfs dfs -put` and `hdfs dfs -copyFromLocal`?
3. How does HDFS decide how many blocks to split a file into?
4. What does `hdfs fsck` show and when is it useful?

---

## Session 1 — Deliverables

| # | Deliverable | Points |
|---|-------------|--------|
| 1 | Screenshot of `hdfs-site.xml` configuration | 0.5 |
| 2 | Output of `hdfs dfsadmin -report` with annotations | 0.5 |
| 3 | Screenshots of HDFS operations (Task 1.3) | 1.0 |
| 4 | Answers to large file questions (Task 1.4) | 0.5 |
| 5 | Control questions answers | 0.5 |
| | **Total** | **3** |

---

# Session 2: MapReduce Programming with Hadoop Streaming (2 hours)

**Points: 4**

---

## 2.1. Understanding Hadoop Streaming (15 min)

Hadoop Streaming allows you to write MapReduce jobs in **any language** (Python, Ruby, Bash, etc.) instead of Java. The mapper and reducer are separate programs that read from `stdin` and write to `stdout`.

```
Input (HDFS) → stdin → MAPPER → stdout → [Shuffle & Sort] → stdin → REDUCER → stdout → Output (HDFS)
```

Rules:
- **Mapper** reads lines from `stdin`, outputs `key\tvalue` pairs to `stdout`
- The framework **sorts** all mapper output by key
- **Reducer** reads `key\tvalue` pairs from `stdin` (grouped by key), outputs results to `stdout`
- Key and value are separated by a **tab character** (`\t`)

---

## 2.2. Word Count with Hadoop Streaming (30 min)

### Step 1. Write the Mapper

```bash
cat > mapper.py << 'PYEOF'
#!/usr/bin/env python3
"""Mapper: reads lines from stdin, emits (word, 1) for each word."""
import sys

for line in sys.stdin:
    line = line.strip().lower()
    words = line.split()
    for word in words:
        # Remove punctuation
        word = ''.join(c for c in word if c.isalnum())
        if word:
            print(f"{word}\t1")
PYEOF

chmod +x mapper.py
```

### Step 2. Write the Reducer

```bash
cat > reducer.py << 'PYEOF'
#!/usr/bin/env python3
"""Reducer: reads (word, count) pairs from stdin, sums counts per word."""
import sys

current_word = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    try:
        word, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        continue

    if word == current_word:
        current_count += count
    else:
        if current_word is not None:
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

# Output the last word
if current_word is not None:
    print(f"{current_word}\t{current_count}")
PYEOF

chmod +x reducer.py
```

### Step 3. Test Locally Before Submitting to Hadoop

Always test your mapper and reducer locally first:

```bash
# Test mapper
echo "Hello World Hello Hadoop World" | python3 mapper.py

# Expected output:
# hello   1
# world   1
# hello   1
# hadoop  1
# world   1

# Test full pipeline locally (simulating shuffle & sort with 'sort')
echo "Hello World Hello Hadoop World" | python3 mapper.py | sort | python3 reducer.py

# Expected output:
# hadoop  1
# hello   2
# world   2
```

### Step 4. Run on Hadoop

```bash
# Make sure input data exists in HDFS
hdfs dfs -ls /user/student/data/raw/

# Find the Hadoop streaming jar
STREAMING_JAR=$(find $HADOOP_PREFIX/share/hadoop/tools/lib/ \
  -name "hadoop-streaming-*.jar" | head -1)
echo "Streaming JAR: $STREAMING_JAR"

# Run the MapReduce job
hadoop jar $STREAMING_JAR \
  -input /user/student/data/raw/large_file.txt \
  -output /user/student/data/processed/wordcount-output \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -file mapper.py \
  -file reducer.py

# View results
hdfs dfs -ls /user/student/data/processed/wordcount-output/
hdfs dfs -cat /user/student/data/processed/wordcount-output/part-00000
```

> **Task 2.1:** Run the Word Count job on the large file from Session 1. Take a screenshot of:
> 1. The terminal output showing job progress (map/reduce percentages)
> 2. The final word count results
> 3. The ResourceManager Web UI (http://localhost:8088) showing the completed job

---

## 2.3. Custom MapReduce: Log Analyzer (45 min)

Now write your own MapReduce job from scratch. We will analyze server access logs.

### Step 1. Generate Sample Log Data

```bash
cat > generate_logs.py << 'PYEOF'
#!/usr/bin/env python3
"""Generate realistic server access logs."""
import random
import datetime

ips = [f"192.168.1.{i}" for i in range(1, 51)]
endpoints = [
    "/api/login", "/api/data", "/api/users", "/api/reports",
    "/index.html", "/dashboard", "/api/upload", "/api/search",
    "/api/health", "/static/css/main.css", "/static/js/app.js"
]
methods = ["GET", "GET", "GET", "POST", "POST", "PUT", "DELETE"]
statuses = [200, 200, 200, 200, 200, 301, 404, 404, 500, 403]

start = datetime.datetime(2025, 1, 1, 0, 0, 0)

with open("access_logs.txt", "w") as f:
    for i in range(100000):
        ts = start + datetime.timedelta(seconds=random.randint(0, 86400 * 30))
        ip = random.choice(ips)
        method = random.choice(methods)
        endpoint = random.choice(endpoints)
        status = random.choice(statuses)
        size = random.randint(200, 50000)
        f.write(f'{ip} - - [{ts.strftime("%d/%b/%Y:%H:%M:%S")} +0000] '
                f'"{method} {endpoint} HTTP/1.1" {status} {size}\n')

print("Generated 100,000 log entries")
PYEOF

python3 generate_logs.py 2>/dev/null || python generate_logs.py

# Preview the generated logs
head -5 access_logs.txt

# Upload to HDFS
hdfs dfs -put access_logs.txt /user/student/data/raw/
```

### Step 2. Task A — Count Requests per HTTP Status Code

Write a mapper and reducer that count how many requests resulted in each HTTP status code (200, 404, 500, etc.).

**Mapper (`status_mapper.py`):**

```bash
cat > status_mapper.py << 'PYEOF'
#!/usr/bin/env python3
"""Mapper: extract HTTP status code from each log line."""
import sys

for line in sys.stdin:
    line = line.strip()
    try:
        # Log format: IP - - [timestamp] "METHOD /path HTTP/1.1" STATUS SIZE
        parts = line.split('"')
        after_request = parts[2].strip().split()
        status = after_request[0]
        print(f"{status}\t1")
    except (IndexError, ValueError):
        continue
PYEOF

chmod +x status_mapper.py
```

**Reducer** — reuse `reducer.py` from the Word Count example (it already sums counts by key).

```bash
# Test locally
head -20 access_logs.txt | python3 status_mapper.py | sort | python3 reducer.py

# Run on Hadoop
hadoop jar $STREAMING_JAR \
  -input /user/student/data/raw/access_logs.txt \
  -output /user/student/data/processed/status-count \
  -mapper "python3 status_mapper.py" \
  -reducer "python3 reducer.py" \
  -file status_mapper.py \
  -file reducer.py

# View results
hdfs dfs -cat /user/student/data/processed/status-count/part-00000
```

### Step 3. Task B — Top IP Addresses by Request Count (Independent Work)

> **Task 2.2 (Independent):** Write a MapReduce job that counts the number of requests from each IP address. You need to:
>
> 1. Write `ip_mapper.py` — extract the IP address from each log line and emit `(ip, 1)`
> 2. Reuse `reducer.py` for counting
> 3. Test locally first, then run on Hadoop
> 4. Find the top 5 most active IP addresses from the output
>
> **Hint:** the IP address is the first field in each log line.

### Step 4. Task C — Count Requests per Endpoint (Independent Work)

> **Task 2.3 (Independent):** Write a MapReduce job that counts requests per endpoint (URL path). You need to:
>
> 1. Write `endpoint_mapper.py` — extract the endpoint from the request string (e.g., `/api/login`) and emit `(endpoint, 1)`
> 2. Run the job and analyze which endpoints receive the most traffic
>
> **Hint:** the endpoint is inside the quoted request string, e.g., `"GET /api/login HTTP/1.1"`.

---

## 2.4. Analyzing Job Execution (15 min)

After running your jobs, examine the execution details:

```bash
# List all completed applications
yarn application -list -appStates FINISHED

# Get detailed info about a specific job (replace with your app ID)
# yarn application -status application_XXXX_XXXX

# View job counters (map input records, output records, etc.)
# mapred job -counter job_XXXX_XXXX
```

In the **ResourceManager Web UI** (http://localhost:8088), click on your completed application and examine:

- How many map tasks ran?
- How many reduce tasks ran?
- How long did each phase take?
- What were the input/output record counts?

> **Task 2.4:** For one of your MapReduce jobs, provide a screenshot from the Web UI showing the job details (number of mappers/reducers, elapsed time, counters).

---

## Session 2 — Control Questions

1. What is Hadoop Streaming and why is it useful?
2. Why do we test mapper and reducer locally before running on Hadoop?
3. What is the role of the Shuffle & Sort phase between mapper and reducer?
4. Why must mapper output be `key\tvalue` format (tab-separated)?

---

## Session 2 — Deliverables

| # | Deliverable | Points |
|---|-------------|--------|
| 1 | Word Count job results + screenshots (Task 2.1) | 1.0 |
| 2 | Status Code count — working code + output | 0.5 |
| 3 | IP Address count — working code + top 5 IPs (Task 2.2) | 1.0 |
| 4 | Endpoint count — working code + output (Task 2.3) | 1.0 |
| 5 | Job execution analysis screenshot (Task 2.4) | 0.5 |
| | **Total** | **4** |

---

# Session 3: Monitoring, Troubleshooting, and Real-World Data (2 hours)

**Points: 3**

---

## 3.1. Hadoop Cluster Monitoring Deep Dive (30 min)

### NameNode Web UI (http://localhost:9870)

Open the NameNode UI and explore the following sections. Record your findings.

**Overview page:**
- Total HDFS capacity and percentage used
- Number of live/dead DataNodes
- Number of files and blocks
- Heap memory usage

**Datanodes tab:**
- DataNode hostname and status
- Last contact (heartbeat)
- Capacity, used, remaining
- Number of blocks stored

**Utilities → Browse the file system:**
- Navigate to `/user/student/data/`
- Click on a file to see: owner, group, size, replication, block size
- Note the block IDs

> **Task 3.1:** Take screenshots of:
> 1. NameNode Overview page
> 2. The file browser showing your uploaded files with their sizes and replication

### ResourceManager Web UI (http://localhost:8088)

Explore:
- **Cluster Metrics** — memory total, memory used, vCores
- **Applications** — list of all your submitted MapReduce jobs
- Click on any completed application:
  - **Attempt** → **Logs** to view mapper/reducer logs
  - Note the container IDs and their completion status

> **Task 3.2:** Find the logs of a mapper from one of your MapReduce jobs. Take a screenshot showing the mapper's stdout or stderr log output.

### YARN Command-Line Monitoring

```bash
# Cluster resource overview
yarn top

# Or check node resources
yarn node -list -showDetails

# Application logs (replace with your app ID)
# yarn logs -applicationId application_XXXX_XXXX
```

---

## 3.2. Processing a Real-World Dataset (60 min)

Now apply everything you've learned to process a real dataset.

### Dataset: World Cities

We will use a cities dataset with population data.

```bash
# Generate a realistic cities dataset
cat > generate_cities.py << 'PYEOF'
#!/usr/bin/env python3
"""Generate a CSV dataset of world cities with population and coordinates."""

cities_data = [
    ("Kyiv", "Ukraine", "Europe", 2952301, 50.45, 30.52),
    ("Kharkiv", "Ukraine", "Europe", 1421125, 49.99, 36.23),
    ("Odesa", "Ukraine", "Europe", 1015826, 46.47, 30.73),
    ("Dnipro", "Ukraine", "Europe", 980948, 48.46, 35.04),
    ("Lviv", "Ukraine", "Europe", 721301, 49.84, 24.03),
    ("Zaporizhzhia", "Ukraine", "Europe", 722713, 47.85, 35.12),
    ("Warsaw", "Poland", "Europe", 1860281, 52.23, 21.01),
    ("Krakow", "Poland", "Europe", 800653, 50.06, 19.94),
    ("Berlin", "Germany", "Europe", 3644826, 52.52, 13.41),
    ("Munich", "Germany", "Europe", 1471508, 48.14, 11.58),
    ("Hamburg", "Germany", "Europe", 1841179, 53.55, 10.00),
    ("Paris", "France", "Europe", 2161000, 48.86, 2.35),
    ("Lyon", "France", "Europe", 513275, 45.76, 4.83),
    ("London", "United Kingdom", "Europe", 8982000, 51.51, -0.13),
    ("Manchester", "United Kingdom", "Europe", 553230, 53.48, -2.24),
    ("New York", "United States", "North America", 8336817, 40.71, -74.01),
    ("Los Angeles", "United States", "North America", 3979576, 34.05, -118.24),
    ("Chicago", "United States", "North America", 2693976, 41.88, -87.63),
    ("Houston", "United States", "North America", 2320268, 29.76, -95.37),
    ("Washington", "United States", "North America", 689545, 38.91, -77.04),
    ("Toronto", "Canada", "North America", 2731571, 43.65, -79.38),
    ("Montreal", "Canada", "North America", 1762949, 45.50, -73.57),
    ("Tokyo", "Japan", "Asia", 13960000, 35.68, 139.69),
    ("Osaka", "Japan", "Asia", 2753862, 34.69, 135.50),
    ("Beijing", "China", "Asia", 21542000, 39.90, 116.40),
    ("Shanghai", "China", "Asia", 24870895, 31.23, 121.47),
    ("Mumbai", "India", "Asia", 12442373, 19.08, 72.88),
    ("Delhi", "India", "Asia", 16787941, 28.70, 77.10),
    ("Seoul", "South Korea", "Asia", 9776000, 37.57, 126.98),
    ("Sydney", "Australia", "Oceania", 5312163, 33.87, 151.21),
    ("Melbourne", "Australia", "Oceania", 5078193, 37.81, 144.96),
    ("São Paulo", "Brazil", "South America", 12325232, 23.55, -46.63),
    ("Rio de Janeiro", "Brazil", "South America", 6748000, 22.91, -43.17),
    ("Buenos Aires", "Argentina", "South America", 3075646, 34.60, -58.38),
    ("Cairo", "Egypt", "Africa", 9540000, 30.04, 31.24),
    ("Lagos", "Nigeria", "Africa", 15946000, 6.52, 3.38),
    ("Nairobi", "Kenya", "Africa", 4397073, 1.29, 36.82),
    ("Cape Town", "South Africa", "Africa", 4618000, 33.93, 18.42),
    ("Istanbul", "Turkey", "Europe", 15462452, 41.01, 28.98),
    ("Moscow", "Russia", "Europe", 12506468, 55.76, 37.62),
]

with open("cities.csv", "w") as f:
    f.write("city,country,continent,population,latitude,longitude\n")
    for city, country, continent, pop, lat, lng in cities_data:
        f.write(f"{city},{country},{continent},{pop},{lat},{lng}\n")

print(f"Generated {len(cities_data)} cities")
PYEOF

python3 generate_cities.py 2>/dev/null || python generate_cities.py

# Preview
head -5 cities.csv

# Upload to HDFS
hdfs dfs -mkdir -p /user/student/project/input
hdfs dfs -put cities.csv /user/student/project/input/
```

### Task A: Total Population per Country

Write a MapReduce job that calculates total population per country.

```bash
cat > country_pop_mapper.py << 'PYEOF'
#!/usr/bin/env python3
"""Mapper: emit (country, population) for each city."""
import sys

for line in sys.stdin:
    line = line.strip()
    # Skip header
    if line.startswith("city,"):
        continue
    try:
        parts = line.split(",")
        country = parts[1]
        population = parts[3]
        print(f"{country}\t{population}")
    except (IndexError, ValueError):
        continue
PYEOF

cat > sum_reducer.py << 'PYEOF'
#!/usr/bin/env python3
"""Reducer: sum values per key."""
import sys

current_key = None
current_sum = 0

for line in sys.stdin:
    line = line.strip()
    try:
        key, value = line.split('\t', 1)
        value = int(value)
    except ValueError:
        continue

    if key == current_key:
        current_sum += value
    else:
        if current_key is not None:
            print(f"{current_key}\t{current_sum}")
        current_key = key
        current_sum = value

if current_key is not None:
    print(f"{current_key}\t{current_sum}")
PYEOF

chmod +x country_pop_mapper.py sum_reducer.py

# Test locally
cat cities.csv | python3 country_pop_mapper.py | sort | python3 sum_reducer.py

# Run on Hadoop
hadoop jar $STREAMING_JAR \
  -input /user/student/project/input/cities.csv \
  -output /user/student/project/output/country-population \
  -mapper "python3 country_pop_mapper.py" \
  -reducer "python3 sum_reducer.py" \
  -file country_pop_mapper.py \
  -file sum_reducer.py

# View results
hdfs dfs -cat /user/student/project/output/country-population/part-00000
```

### Task B: Total Population per Continent (Independent)

> **Task 3.3:** Write a MapReduce job that calculates total population per continent.
> 1. Write `continent_mapper.py` — emit `(continent, population)` for each city
> 2. Reuse `sum_reducer.py`
> 3. Run on Hadoop and record the results
> 4. Which continent has the highest total population in the dataset?

### Task C: Find the Largest City per Country (Independent, Bonus)

> **Task 3.4 (Bonus):** Write a MapReduce job that finds the city with the largest population in each country.
> 1. Write `max_city_mapper.py` — emit `(country, population:city_name)`
> 2. Write `max_reducer.py` — for each country, output only the city with the highest population
> 3. Run and verify the results
>
> **Hint:** in the reducer, compare population values and keep track of the maximum.

---

## 3.3. Cleanup and Data Management (15 min)

Practice managing HDFS data:

```bash
# Check total disk usage
hdfs dfs -du -h /user/student/

# Remove intermediate outputs to free space
hdfs dfs -rm -r /user/student/data/processed/wordcount-output
hdfs dfs -rm -r /user/student/data/processed/status-count

# Verify cleanup
hdfs dfs -du -h /user/student/

# Empty trash (HDFS has a trash feature similar to recycle bin)
hdfs dfs -expunge
```

When you're done with all sessions:

```bash
# Exit the container
exit

# Stop the container (preserves data for later)
docker stop hadoop-sandbox

# To resume later:
# docker start -i hadoop-sandbox

# To remove completely (all data will be lost):
# docker rm hadoop-sandbox
```

---

## Session 3 — Control Questions

1. What information can you find in the NameNode Web UI that you cannot get from the command line?
2. How do you find the logs of a failed MapReduce task?
3. What would you change in your MapReduce job if the input file was 100 GB instead of 50 MB?
4. Why is it important to test mapper and reducer locally before running on the cluster?

---

## Session 3 — Deliverables

| # | Deliverable | Points |
|---|-------------|--------|
| 1 | NameNode and ResourceManager UI screenshots (Tasks 3.1, 3.2) | 1.0 |
| 2 | Population per continent — working code + results (Task 3.3) | 1.0 |
| 3 | Control questions answers | 0.5 |
| 4 | Largest city per country — bonus (Task 3.4) | 0.5 |
| | **Total** | **3** |

---

## Summary of All Sessions

| Session | Focus | Key Skills | Points |
|---------|-------|------------|--------|
| 1 | Environment + HDFS | Configuration, file operations, block inspection | 3 |
| 2 | MapReduce Programming | Hadoop Streaming, mapper/reducer in Python, log analysis | 4 |
| 3 | Monitoring + Real Data | Web UIs, YARN logs, dataset processing pipeline | 3 |
| **Total** | | | **10** |

---

## Independent Work (12 hours)

**Topic:** MapReduce practical tasks. Working with HDFS. Preparing reports.

### Tasks:

1. **Lab Report** (all sessions combined):
   - Compile all screenshots, code, and outputs into a single report
   - Include brief explanations of what each command/job does
   - Answer all control questions from all three sessions

2. **Additional MapReduce Job** (choose one):
   - **Option A:** Write a MapReduce job that computes the average population of cities per continent
   - **Option B:** Write a MapReduce job that processes the access logs to find the busiest hour of the day (most requests)
   - **Option C:** Write a MapReduce job that finds all IP addresses that generated 404 errors and counts how many 404s each IP caused

3. **Reflection** (0.5 page):
   - What was the most challenging part of working with Hadoop?
   - How does running a job on a single-node Docker cluster differ from a real multi-node cluster?
   - Name one advantage and one disadvantage of Hadoop Streaming vs writing MapReduce in Java

---

## Recommended Resources

- [Lesson 4 — Apache Hadoop Ecosystem](../lesson1-4/README.md)
- Hadoop Streaming Documentation: https://hadoop.apache.org/docs/stable/hadoop-streaming/HadoopStreaming.html
- HDFS Commands Guide: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/FileSystemShell.html
- YARN Commands: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YarnCommands.html
- Docker Hadoop Image: https://hub.docker.com/r/sequenceiq/hadoop-docker
