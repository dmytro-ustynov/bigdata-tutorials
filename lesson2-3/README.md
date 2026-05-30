# Lesson 2-3. Cybersecurity Analytics with Apache Spark (Practical)

> View [Ukrainian version](README_ua.md)

**Discipline:** BIG DATA (Processing of Very Large Data Sets)

**Content Module 2:** Apache Spark and Machine Learning on Big Data

**Type:** Practical

**Duration:** 6 hours

---

## ⚠️ This lesson lives in a separate repository

The full practical materials, source code, Docker environment, and step-by-step
instructions for Lesson 2-3 are maintained in a dedicated repository:

### 👉 [dmytro-ustynov/spark-tutorial](https://github.com/dmytro-ustynov/spark-tutorial)

Clone it to get started:

```bash
git clone https://github.com/dmytro-ustynov/spark-tutorial.git
cd spark-tutorial
```

---

## Overview

This practical simulates real-world cybersecurity events and teaches students how
to detect attacks using Apache Spark **Structured Streaming** analytics. The whole
lab environment (Spark, Kafka, PostgreSQL, log generator, Jupyter Lab) ships as
containers — **only Docker is required**.

## Learning Objectives

After completing the lab, students should be able to:

- process real-time security event streams with Spark Structured Streaming;
- detect brute-force attacks using time-based aggregations;
- identify DDoS attacks through traffic-pattern analysis;
- store analytical results in PostgreSQL;
- handle late-arriving data and implement watermarking;
- build scalable threat-detection systems.

## Prerequisites

- Completed [Lesson 2-1](../lesson2-1/README.md) (Spark theory) and
  [Lesson 2-2](../lesson2-2/README.md) (Spark setup & DataFrame API)
- **Docker & Docker Compose**
- Python 3.7+ (only if using the local Spark option)

## Where to start in the spark-tutorial repo

- [`README.md`](https://github.com/dmytro-ustynov/spark-tutorial/blob/main/README.md) — project overview & architecture
- [`GETTING_STARTED.md`](https://github.com/dmytro-ustynov/spark-tutorial/blob/main/GETTING_STARTED.md) — environment setup
- [`DETECTION_GUIDE.md`](https://github.com/dmytro-ustynov/spark-tutorial/blob/main/DETECTION_GUIDE.md) — attack-detection walkthrough
- [`examples/`](https://github.com/dmytro-ustynov/spark-tutorial/tree/main/examples) — reference solutions
- [`student-work/`](https://github.com/dmytro-ustynov/spark-tutorial/tree/main/student-work) — workspace for assignments

---

## Resources

- 🔗 **Lesson repository:** https://github.com/dmytro-ustynov/spark-tutorial
- [Apache Spark — Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
