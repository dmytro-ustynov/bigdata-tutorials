# Заняття 2-2. Налаштування Apache Spark

> Подивитись [версію англійською](README.md)

**Дисципліна:** BIG DATA (Обробка надвеликих масивів даних)

**Змістовий модуль 2:** Apache Spark та машинне навчання на великих даних

**Тип:** Практичне заняття

**Тривалість:** 4 години (2 сесії × 2 години)

**Максимальна оцінка:** 6 балів

---

## Навчальні цілі

Після завершення обох сесій здобувачі повинні:

- встановлювати Apache Spark / PySpark на MacOS, Linux або Windows;
- запускати SparkSession і працювати з веб-інтерфейсом Spark UI;
- розрізняти SparkContext і SparkSession у робочому коді;
- застосовувати трансформації та дії RDD до реального набору даних;
- використовувати DataFrame API та Spark SQL для запитів до структурованих даних;
- пояснювати, чому Spark швидший за MapReduce на тому самому навантаженні.

---

## Передумови

- Завершено заняття 2-1 (теорія: архітектура, RDD, трансформації та дії)
- Встановлено Python 3.10+
- Встановлено Java 11 або 17 (`java -version` повинна виводити версію)
- ~2 ГБ вільного місця на диску (Spark + набір даних)
- Опціонально: встановлено Docker (для прев'ю `spark-tutorial` у Сесії 2)

---

## Набір даних

Будемо використовувати **журнал доступу NASA-HTTP** з [Internet Traffic Archive](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html) — два місяці (липень і серпень 1995) усіх HTTP-запитів до веб-сервера NASA Kennedy Space Center. Це класичний навчальний набір даних для Spark / Hadoop:

- ~3,4 мільйона запитів загалом (~205 МБ у розпакованому вигляді)
- Стандартний формат NCSA Common Log
- Реальні «брудні» дані: відсутні поля, зіпсовані рядки, різноманітні URL

Кожен рядок виглядає так:

```
piweba3y.prodigy.com - - [01/Aug/1995:00:00:09 -0400] "GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0" 200 4085
```

---

## Оцінювання

| Сесія | Фокус | Бали |
|---------|-------|--------|
| Сесія 1 | Встановлення Spark, перша SparkSession, SparkContext проти SparkSession | 3 |
| Сесія 2 | RDD + DataFrame + Spark SQL на NASA-логах, прев'ю spark-tutorial | 3 |
| **Разом** | | **6** |

Кожна сесія оцінюється за:
- Робочим кодом (`.py` файли або notebook)
- Скриншотами: вивід PySpark shell, Spark Web UI, результати запитів
- Короткими письмовими відповідями на контрольні питання в кінці сесії

---

# Сесія 1: Встановлення Spark та перші кроки (2 години)

**Балів: 3**

---

## 1.1. Встановлення Apache Spark (30 хв)

Можете обрати **один** з трьох шляхів встановлення. Виберіть той, що відповідає вашій машині — усі три дають робочий `pyspark` для решти заняття.

### Шлях A — `pip install pyspark` (рекомендовано для MacOS / Linux / Windows)

Найпростіший варіант. PySpark постачається з вбудованим дистрибутивом Spark, тож окремо завантажувати Spark не потрібно.

```bash
# Спочатку перевірте Python та Java
python3 --version    # 3.10 або новіша
java -version        # 11 або 17

# Встановіть PySpark у віртуальне середовище
python3 -m venv ~/spark-env
source ~/spark-env/bin/activate         # Windows: ~/spark-env/Scripts/activate
pip install --upgrade pip
pip install pyspark==3.5.1 jupyter
```

Перевірка:

```bash
pyspark --version
# Welcome to
#       ____              __
#      / __/__  ___ _____/ /__
#     _\ \/ _ \/ _ `/ __/  '_/
#    /___/ .__/\_,_/_/ /_/\_\   version 3.5.1
```

### Шлях B — Завантажити повний дистрибутив Spark (MacOS / Linux)

Використовуйте, якщо хочете повний `spark-shell` (Scala) і ланцюжок інструментів `spark-submit`.

```bash
# 1. Завантаження
cd ~
curl -O https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
mv spark-3.5.1-bin-hadoop3 spark

# 2. Додати до PATH (дописати у ~/.zshrc або ~/.bashrc)
echo 'export SPARK_HOME=~/spark' >> ~/.zshrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.zshrc
echo 'export PYSPARK_PYTHON=python3' >> ~/.zshrc
source ~/.zshrc

# 3. Перевірка
spark-shell --version
pyspark --version
```

### Шлях C — Контейнеризований Spark (Windows-користувачі з Docker, або хто не любить встановлень)

Клонуйте репозиторій [`spark-tutorial`](https://github.com/dmytro-ustynov/spark-tutorial) і використайте його образ Jupyter+Spark. Глибше попрацюєте з ним у Занятті 2-3.

```bash
git clone https://github.com/dmytro-ustynov/spark-tutorial.git
cd spark-tutorial
docker compose up -d jupyter
# Відкрийте http://localhost:8888 у браузері
```

---

### ✅ Контрольна точка 1.1

Подайте скриншот `pyspark --version` (або, для Шляху C, запущеного контейнера Jupyter), що показує **Spark 3.5.x**.

---

## 1.2. Перша SparkSession (20 хв)

Створіть робочу директорію для цього заняття:

```bash
mkdir -p ~/lesson2-2 && cd ~/lesson2-2
```

Збережіть наступне як `hello_spark.py`:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HelloSpark")
    .master("local[*]")              # Використати всі локальні CPU-ядра
    .config("spark.ui.port", "4040") # Порт Web UI
    .getOrCreate()
)

# SparkSession прозоро обгортає SparkContext
sc = spark.sparkContext

print(f"Spark version: {spark.version}")
print(f"Application ID: {sc.applicationId}")
print(f"Default parallelism: {sc.defaultParallelism}")
print(f"Master: {sc.master}")

# Швидкий smoke-тест
nums = sc.parallelize(range(1, 1_000_001))
print(f"Sum of 1..1_000_000 = {nums.sum()}")

# Тримаємо процес живим, щоб подивитися Spark UI
input("Натисніть Enter для виходу (Spark UI: http://localhost:4040)...")
spark.stop()
```

Запустіть:

```bash
python hello_spark.py
```

Очікуваний вивід (числа можуть відрізнятись):

```
Spark version: 3.5.1
Application ID: local-1718000000000
Default parallelism: 8
Master: local[*]
Sum of 1..1_000_000 = 500000500000
Натисніть Enter для виходу (Spark UI: http://localhost:4040)...
```

Поки скрипт призупинений, відкрийте **http://localhost:4040** у браузері. Ви побачите Spark Web UI з додатком `HelloSpark`, одним завершеним job'ом і вкладкою executors з вашим локальним executor'ом.

---

### ✅ Контрольна точка 1.2

Подайте:
- Консольний вивід `hello_spark.py`
- Скриншот вкладки **Jobs** Spark Web UI з завершеним job'ом

---

## 1.3. SparkContext проти SparkSession на практиці (30 хв)

У Занятті 2-1 ви вивчили концептуальну різницю. Тепер побачимо її в коді.

Збережіть як `context_vs_session.py`:

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext

# --- Сучасний спосіб: створити SparkSession (рекомендовано) ---
spark = SparkSession.builder.appName("ContextDemo").master("local[*]").getOrCreate()

# Отримати SparkContext із сесії — це той самий JVM-широкий singleton
sc = spark.sparkContext
print("Same SparkContext instance:", sc is SparkContext.getOrCreate())

# RDD API — доступний через SparkContext
rdd = sc.parallelize([("apple", 3), ("banana", 5), ("apple", 2)])
print("RDD reduceByKey:", rdd.reduceByKey(lambda a, b: a + b).collect())

# DataFrame API — доступний через SparkSession
df = spark.createDataFrame(
    [("apple", 3), ("banana", 5), ("apple", 2)],
    ["fruit", "qty"],
)
df.groupBy("fruit").sum("qty").show()

# SQL API — також через SparkSession
df.createOrReplaceTempView("fruits")
spark.sql("SELECT fruit, SUM(qty) AS total FROM fruits GROUP BY fruit").show()

spark.stop()
```

Запустіть. Ключові висновки:

| Що ви хочете зробити | Використовуйте |
|--|--|
| Низькорівневі операції з RDD | `spark.sparkContext` (або `sc`) |
| DataFrame, SQL, Hive, structured streaming | `spark` (SparkSession) безпосередньо |
| Читання/запис файлів у будь-якому форматі | `spark.read.*` / `df.write.*` |
| Керування життєвим циклом | `spark.stop()` (зупиняє і обгорнутий SparkContext) |

**Питання для роздумів:** Чому Spark 2.0 об'єднав їх в одну точку входу? Підказка — що відбувалося б, якби у вас був окремий `SparkContext`, `SQLContext` та `HiveContext`, і кожен кешував би власні метадані?

---

### ✅ Контрольна точка 1.3

Подайте `context_vs_session.py` та його вивід. Коротко (2–3 речення) дайте відповідь на питання вище.

---

## 1.4. Контрольні питання — Сесія 1 (10 хв)

Дайте відповіді у звіті:

1. Що означає `local[*]` як master URL Spark? Коли використовувати `local[4]` проти `local[*]`?
2. Що робить `getOrCreate()` і чому йому надають перевагу перед `SparkSession()`?
3. Що відбувається зі Spark UI на порту 4040 після того, як ви викликаєте `spark.stop()`?
4. Назвіть дві операції, які можна виконати лише через `SparkContext` (не через `SparkSession`).

---

# Сесія 2: RDD та DataFrame на реальних даних (2 години)

**Балів: 3**

---

## 2.1. Завантаження журналів NASA HTTP (10 хв)

```bash
cd ~/lesson2-2
mkdir -p data && cd data

# Липень 1995 (~21 МБ стиснутий, ~167 МБ розпакований)
curl -O https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
gunzip NASA_access_log_Jul95.gz

# Серпень 1995 (менший — сервер впав посеред місяця)
curl -O https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
gunzip NASA_access_log_Aug95.gz

# Перевірка
wc -l NASA_access_log_*95
# Має вивести ~1,9 млн рядків для липня, ~1,5 млн для серпня
head -3 NASA_access_log_Jul95
```

> **Порада:** якщо `ita.ee.lbl.gov` заблоковано, ті самі файли є на Kaggle як [NASA HTTP Access Log dataset](https://www.kaggle.com/datasets/souhagaa/nasa-access-log-dataset-1995). Завантажте обидва `.gz`-файли, помістіть у `~/lesson2-2/data/` і розпакуйте `gunzip`.

---

## 2.2. RDD Word Count (порівняння з Hadoop MapReduce) (25 хв)

Згадайте Word Count, який ми запускали на Hadoop у Занятті 1-5. Та сама логіка у Spark — порахувати, скільки запитів зробив кожен клієнт.

Збережіть як `rdd_top_clients.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NASA-RDD").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# 1. Завантажити обидва місяці як один RDD сирих рядків
lines = sc.textFile("data/NASA_access_log_Jul95,data/NASA_access_log_Aug95")
print(f"Total lines: {lines.count():,}")

# 2. Витягти host (перше поле, розділене пробілом)
hosts = lines.map(lambda line: line.split(" ", 1)[0])

# 3. Пари (host, 1) → reduceByKey → топ-10
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

Запустіть:

```bash
python rdd_top_clients.py
```

Має з'явитись щось на кшталт:

```
Total lines: 3,461,613

Top 10 client hosts:
   17,572  piweba3y.prodigy.com
   11,591  piweba4y.prodigy.com
    9,868  piweba1y.prodigy.com
    ...
```

**Обговорення:** порівняйте з MapReduce-задачею Hadoop із Заняття 1-5:
- Скільки рядків коду потребували Mapper + Reducer?
- Скільки часу займала Hadoop-задача порівняно з цією Spark-задачею?
- Де Spark «виграє» — об'єм коду, час виконання, обидва?

---

### ✅ Контрольна точка 2.2

Подайте `rdd_top_clients.py` та вивід (топ-10 хостів). Коротко зазначте час виконання (`time python rdd_top_clients.py`).

---

## 2.3. DataFrame API — парсинг логів у колонки (30 хв)

Код на RDD приємний, але дані залишаються лише рядками. DataFrame API дозволяє розбирати лог на типізовані колонки і виконувати SQL над ним.

Збережіть як `df_log_analysis.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, to_timestamp, count, avg, sum as _sum, desc
)

spark = SparkSession.builder.appName("NASA-DF").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Завантажити обидва файли одночасно
raw = spark.read.text("data/NASA_access_log_Jul95,data/NASA_access_log_Aug95")
print(f"Raw lines: {raw.count():,}")

# Регулярний вираз для NCSA Common Log:
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
    # Відкинути зіпсовані рядки (regex повернув пустий host)
    .filter(col("host") != "")
    # Перетворити типи
    .withColumn("ts", to_timestamp("ts_raw", "dd/MMM/yyyy:HH:mm:ss Z"))
    .withColumn("bytes", col("bytes_raw").cast("long"))
    .drop("ts_raw", "bytes_raw")
    .cache()  # запитуватимемо цей DataFrame кілька разів
)

print(f"Parsed records: {logs.count():,}")
logs.printSchema()
logs.show(5, truncate=False)

# --- Q1: Розподіл HTTP status кодів ---
print("\n--- Q1: Status code distribution ---")
logs.groupBy("status").count().orderBy(desc("count")).show()

# --- Q2: Топ-10 найбільш запитуваних шляхів ---
print("\n--- Q2: Top 10 paths ---")
(logs.groupBy("path")
     .agg(count("*").alias("hits"), _sum("bytes").alias("total_bytes"))
     .orderBy(desc("hits"))
     .show(10, truncate=False))

# --- Q3: Помилки 404 — які шляхи спричиняють найбільше? ---
print("\n--- Q3: Top 10 paths returning 404 ---")
(logs.filter(col("status") == 404)
     .groupBy("path").count()
     .orderBy(desc("count"))
     .show(10, truncate=False))

# --- Q4: Трафік за годинами доби ---
print("\n--- Q4: Hits per hour of day ---")
from pyspark.sql.functions import hour
(logs.groupBy(hour("ts").alias("hour"))
     .count()
     .orderBy("hour")
     .show(24))

spark.stop()
```

Запустіть. Зверніть увагу:
- Ми використовуємо `.cache()` для розпарсеного DataFrame, щоб чотири запити повторно використали той самий результат у пам'яті, замість повторного парсингу 3,4 млн рядків чотири рази.
- Регулярний вираз відкидає зіпсовані рядки (у NASA-логах є кілька тисяч таких — не-ASCII шляхи, обрізані рядки тощо).

---

### ✅ Контрольна точка 2.3

Подайте `df_log_analysis.py` та скриншот вкладки **Storage** Spark Web UI, що показує закешований DataFrame.

---

## 2.4. Spark SQL на тих самих даних (15 хв)

Ті самі дані, синтаксис SQL. Додайте до попереднього скрипту (або збережіть як `sql_log_analysis.py`):

```python
logs.createOrReplaceTempView("logs")

# Скільки байтів NASA віддала загалом?
spark.sql("""
    SELECT
        ROUND(SUM(bytes) / 1024.0 / 1024.0 / 1024.0, 2) AS total_gb,
        COUNT(*)                                        AS total_requests,
        ROUND(AVG(bytes), 0)                            AS avg_bytes_per_request
    FROM logs
    WHERE status = 200
""").show()

# Денний обсяг запитів
spark.sql("""
    SELECT
        DATE(ts)        AS day,
        COUNT(*)        AS hits,
        COUNT(DISTINCT host) AS unique_hosts
    FROM logs
    GROUP BY DATE(ts)
    ORDER BY day
""").show(31)

# Показати план запиту — Spark SQL оптимізується Catalyst
spark.sql("SELECT host, COUNT(*) FROM logs GROUP BY host ORDER BY 2 DESC LIMIT 10") \
     .explain(mode="formatted")
```

Вивід `explain()` показує **фізичний план**: зверніть увагу на `HashAggregate` замість shuffle-then-group, і projection «протягнута вниз», тож сканується лише колонка `host`.

---

## 2.5. Прев'ю: spark-tutorial (Streaming + Kafka) (10 хв)

Наступне заняття (2-3) будуватиметься на контейнеризованому середовищі зі Spark Structured Streaming, Kafka та PostgreSQL. Запустіть його зараз, щоб бути готовими:

```bash
cd ~
git clone https://github.com/dmytro-ustynov/spark-tutorial.git
cd spark-tutorial

# Запустити Kafka, PostgreSQL, генератор логів, Jupyter+Spark
docker compose up -d

# Перевірити стан усіх сервісів
./lab-control.sh status
```

Відкрийте у браузері, щоб переконатися, що лабораторія працює:

- **Kafka UI** — http://localhost:8080 — має показувати топік `security-events` з повідомленнями
- **Статус генератора логів** — http://localhost:3000/status
- **Jupyter Lab** — http://localhost:8888 (token у `docker compose logs jupyter`)

На цьому Сесія 2 завершується. Наш перший streaming-запит до цього Kafka-топіка ми напишемо у Занятті 2-3.

---

### ✅ Контрольна точка 2.5

Подайте скриншот Kafka UI з повідомленнями у топіку `security-events`.

---

## 2.6. Контрольні питання — Сесія 2 (10 хв)

Дайте відповіді у звіті:

1. У чому різниця між `cache()` та `persist()`? Який рівень зберігання `cache()` обирає за замовчуванням?
2. Ми використали `regexp_extract` замість того, щоб писати парсер на Python всередині `.map()`. Чому це краще для продуктивності? (Підказка: Catalyst, JVM, серіалізація.)
3. У DataFrame-запитах, де Spark виконує **shuffle**? Як ви побачите це в Web UI?
4. NASA-логи мають загалом ~205 МБ. Що зміниться, якщо набір даних буде 205 ГБ — чи ви змінюватимете код, кластер, чи обидва?

---

## Самостійна робота

**Тема:** Налаштування Apache Spark, RDD проти DataFrame, порівняння з MapReduce.

### Завдання

1. **Практичне розширення** (обов'язково):
   - Знайдіть **клієнтський хост з найбільшою кількістю унікальних відвіданих шляхів** у наборі даних NASA.
   - Знайдіть **5-хвилинне вікно з найвищою частотою запитів** (підказка: функція `window()`).
   - Подайте код, вивід і пояснення в 1 абзац — який API (RDD / DataFrame / SQL) ви використали і чому.

2. **Письмовий звіт** (обов'язково, ~2 сторінки):
   - Порівняйте кількість рядків коду та час виконання вашої RDD-задачі топ-клієнтів (розділ 2.2) з еквівалентною MapReduce-задачею Hadoop із Заняття 1-5.
   - Поясніть `local[*]`, `local[N]`, `yarn` та `k8s://...` master URL і коли кожен доречний.
   - Опишіть одну ситуацію, де перехід на низькорівневий RDD API досі є правильним вибором у 2026 році.

### Критерії оцінювання

| Компонент | Бали |
|-----------|--------|
| Контрольні точки Сесій 1 + 2 (5 точок × 1 бал) | 5 |
| Контрольні питання (Сесії 1 + 2) | 1 |
| Незалежне практичне розширення | +1 (бонус) |
| **Разом** | **6 (+1 бонус)** |

---

## Рекомендовані ресурси

- [Заняття 2-1 — Apache Spark (теорія)](../lesson2-1/README_ua.md)
- [Заняття 1-5 — Hadoop MapReduce (практика)](../lesson1-5/README_ua.md)
- Документація Apache Spark — https://spark.apache.org/docs/latest/
- Довідник PySpark API — https://spark.apache.org/docs/latest/api/python/
- Spark RDD Programming Guide — https://spark.apache.org/docs/latest/rdd-programming-guide.html
- Spark SQL Guide — https://spark.apache.org/docs/latest/sql-programming-guide.html
- Опис набору даних NASA-HTTP — https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
- Репозиторій spark-tutorial — https://github.com/dmytro-ustynov/spark-tutorial
- Damji J.S. та ін. — *Learning Spark*, 2-ге видання (O'Reilly, 2020), розділи 1–4
