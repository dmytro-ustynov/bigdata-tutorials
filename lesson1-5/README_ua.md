# Заняття 1.5. Налаштування та робота з Apache Hadoop

> Подивитись [версію англійською](README.md)

**Дисципліна:** BIG DATA (Обробка надвеликих масивів даних)

**Змістовий модуль 1:** Інженерія великих наборів даних

**Тип:** Практичне заняття

**Тривалість:** 6 годин (3 заняття × 2 години)

**Максимальний бал:** 10 балів

---

## Навчальні цілі

Після завершення всіх трьох занять здобувачі повинні вміти:

- розгортати та конфігурувати кластер Hadoop за допомогою Docker;
- виконувати операції з HDFS: створення каталогів, завантаження/вивантаження файлів, управління правами та реплікацією;
- писати та запускати програми MapReduce за допомогою Hadoop Streaming (Python);
- аналізувати виконання MapReduce-завдань через логи та лічильники YARN;
- моніторити стан кластера за допомогою веб-інтерфейсів Hadoop;
- обробляти реальний набір даних за допомогою повного конвеєра Hadoop.

---

## Передумови

- Завершені заняття 1.1–1.4 (теорія MapReduce, HDFS, YARN, екосистема Hadoop)
- Docker встановлено та запущено (щонайменше 4 ГБ вільної оперативної пам'яті)
- Python 3.x встановлено на локальній машині
- Базові навички роботи з командним рядком

---

## Оцінювання

| Заняття | Фокус | Бали |
|---------|-------|------|
| Заняття 1 | Налаштування Hadoop + операції з HDFS | 3 |
| Заняття 2 | Написання та запуск MapReduce-завдань | 4 |
| Заняття 3 | Моніторинг, обробка реальних даних | 3 |
| **Разом** | | **10** |

Кожне заняття оцінюється на основі:
- Виконання всіх обов'язкових завдань (скріншоти + вивід команд)
- Подання робочого коду (заняття 2–3)
- Відповіді на контрольні запитання наприкінці кожного заняття

---

# Заняття 1: Середовище Hadoop та поглиблена робота з HDFS (2 години)

**Бали: 3**

---

## 1.1. Налаштування середовища Hadoop (30 хв)

На занятті 1.4 ми коротко працювали з Docker-контейнером. Тепер налаштуємо більш повноцінне середовище та розберемося з конфігурацією.

### Крок 1. Запуск контейнера Hadoop

```bash
# Завантажити образ (пропустити, якщо вже зроблено на занятті 1.4)
docker pull sequenceiq/hadoop-docker:2.7.1

# Видалити старий контейнер, якщо існує
docker rm -f hadoop-sandbox 2>/dev/null

# Запустити з усіма необхідними портами
docker run -it \
  --name hadoop-sandbox \
  -p 9870:50070 \
  -p 8088:8088 \
  -p 19888:19888 \
  -p 8042:8042 \
  sequenceiq/hadoop-docker:2.7.1 \
  /etc/bootstrap.sh -bash
```

### Крок 2. Дослідження файлів конфігурації Hadoop

Всередині контейнера перегляньте ключові файли конфігурації:

```bash
# Основні налаштування Hadoop
cat $HADOOP_PREFIX/etc/hadoop/core-site.xml
```

**Ключові властивості `core-site.xml`:**

| Властивість | Значення | Призначення |
|-------------|----------|-------------|
| `fs.defaultFS` | `hdfs://localhost:9000` | Адреса NameNode |
| `hadoop.tmp.dir` | `/tmp/hadoop-*` | Каталог тимчасових даних |

```bash
# Налаштування HDFS
cat $HADOOP_PREFIX/etc/hadoop/hdfs-site.xml
```

**Ключові властивості `hdfs-site.xml`:**

| Властивість | Значення | Призначення |
|-------------|----------|-------------|
| `dfs.replication` | `1` | Фактор реплікації (1 для одного вузла) |
| `dfs.blocksize` | `134217728` | Розмір блоку в байтах (128 МБ) |
| `dfs.namenode.name.dir` | `file:///...` | Де NameNode зберігає метадані |
| `dfs.datanode.data.dir` | `file:///...` | Де DataNode зберігає блоки |

```bash
# Налаштування YARN
cat $HADOOP_PREFIX/etc/hadoop/yarn-site.xml
```

**Ключові властивості `yarn-site.xml`:**

| Властивість | Значення | Призначення |
|-------------|----------|-------------|
| `yarn.resourcemanager.hostname` | `localhost` | Адреса ResourceManager |
| `yarn.nodemanager.resource.memory-mb` | varies | Пам'ять, доступна для контейнерів |
| `yarn.nodemanager.aux-services` | `mapreduce_shuffle` | Необхідно для MapReduce |

> **Завдання 1.1:** Зробіть скріншот вмісту `hdfs-site.xml`, що показує фактор реплікації та розмір блоку.

### Крок 3. Перевірка стану кластера

```bash
# Детальний звіт HDFS
hdfs dfsadmin -report

# Статус безпечного режиму
hdfs dfsadmin -safemode get

# Вузли YARN
yarn node -list

# Інформація про чергу YARN
yarn queue -status default
```

> **Завдання 1.2:** Запишіть вивід `hdfs dfsadmin -report`. Зверніть увагу: загальна ємність, використаний простір, кількість DataNode.

---

## 1.2. Поглиблені операції з HDFS (50 хв)

### Управління каталогами та файлами

```bash
# Створити структуру каталогів
hdfs dfs -mkdir -p /user/student/data/raw
hdfs dfs -mkdir -p /user/student/data/processed
hdfs dfs -mkdir -p /user/student/scripts

# Перелік з деталями (права, власник, розмір)
hdfs dfs -ls -R /user/student/
```

### Створення та завантаження файлів

```bash
# Створити кілька тестових файлів локально
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

# Завантажити окремі файли
hdfs dfs -put file1.txt /user/student/data/raw/
hdfs dfs -put file2.txt /user/student/data/raw/
hdfs dfs -put file3.txt /user/student/data/raw/

# Завантажити кілька файлів одночасно
# hdfs dfs -put file1.txt file2.txt file3.txt /user/student/data/raw/

# Перевірити завантаження
hdfs dfs -ls /user/student/data/raw/
```

### Читання та вивантаження файлів

```bash
# Показати вміст файлу
hdfs dfs -cat /user/student/data/raw/file1.txt

# Показати перші/останні рядки (корисно для великих файлів)
hdfs dfs -head /user/student/data/raw/file1.txt
hdfs dfs -tail /user/student/data/raw/file1.txt

# Завантажити файл з HDFS на локальну ФС
hdfs dfs -get /user/student/data/raw/file1.txt ./downloaded_file1.txt
cat ./downloaded_file1.txt

# Об'єднати кілька файлів HDFS в один локальний файл
hdfs dfs -getmerge /user/student/data/raw/ ./merged_all.txt
cat ./merged_all.txt
```

### Операції з файлами

```bash
# Копіювати файл у межах HDFS
hdfs dfs -cp /user/student/data/raw/file1.txt /user/student/data/processed/file1_copy.txt

# Перемістити (перейменувати) файл у межах HDFS
hdfs dfs -mv /user/student/data/processed/file1_copy.txt /user/student/data/processed/renamed.txt

# Перевірити розміри файлів
hdfs dfs -du -h /user/student/data/

# Підрахувати файли та каталоги
hdfs dfs -count /user/student/data/

# Видалити файл
hdfs dfs -rm /user/student/data/processed/renamed.txt

# Видалити каталог рекурсивно
# hdfs dfs -rm -r /user/student/data/processed/
```

### Права та власність

```bash
# Змінити права (аналогічно Unix chmod)
hdfs dfs -chmod 755 /user/student/data/raw/file1.txt

# Змінити власника
hdfs dfs -chown student:student /user/student/data/raw/file1.txt

# Переглянути права
hdfs dfs -ls /user/student/data/raw/
```

### Інформація про блоки та перевірка файлової системи

```bash
# Детальна інформація про блоки файлу
hdfs fsck /user/student/data/raw/file1.txt -files -blocks -locations

# Повна перевірка файлової системи
hdfs fsck /

# Статус файлу (розмір блоку, реплікація)
hdfs dfs -stat "%b %r %n" /user/student/data/raw/file1.txt
# %b = розмір файлу в байтах, %r = фактор реплікації, %n = ім'я файлу
```

> **Завдання 1.3:** Виконайте наступні операції та зробіть скріншоти:
> 1. Створіть структуру каталогів `/user/student/project/input` та `/user/student/project/output`
> 2. Створіть текстовий файл щонайменше з 10 рядків тексту на будь-яку тему
> 3. Завантажте його в `/user/student/project/input/`
> 4. Запустіть `hdfs fsck` для завантаженого файлу — зверніть увагу на кількість блоків та фактор реплікації
> 5. Використайте `hdfs dfs -du -h` для показу використання диску `/user/student/`

---

## 1.3. Робота з більшими файлами (30 хв)

Згенеруємо більший файл, щоб побачити, як HDFS працює з даними більшого масштабу.

```bash
# Згенерувати текстовий файл ~50 МБ з випадковими словами
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

# Перевірити розмір локального файлу
ls -lh large_file.txt

# Завантажити в HDFS
hdfs dfs -put large_file.txt /user/student/data/raw/

# Перевірити, як HDFS розбив файл
hdfs fsck /user/student/data/raw/large_file.txt -files -blocks -locations

# Порівняння використання диску
hdfs dfs -du -h /user/student/data/raw/
```

> **Завдання 1.4:** Дайте відповіді на наступні запитання у звіті:
> 1. Який розмір згенерованого файлу в байтах?
> 2. Скільки блоків HDFS він займає? Чому?
> 3. Якби фактор реплікації був 3, скільки загального обсягу зберігання зайняв би цей файл?

---

## Заняття 1 — Контрольні запитання

Перед завершенням дайте відповіді на ці запитання (письмово або усно викладачу):

1. Яке призначення кожного файлу конфігурації Hadoop (`core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`)?
2. Яка різниця між `hdfs dfs -put` та `hdfs dfs -copyFromLocal`?
3. Як HDFS визначає, на скільки блоків розбити файл?
4. Що показує `hdfs fsck` і коли він корисний?

---

## Заняття 1 — Результати для здачі

| # | Результат | Бали |
|---|-----------|------|
| 1 | Скріншот конфігурації `hdfs-site.xml` | 0.5 |
| 2 | Вивід `hdfs dfsadmin -report` з коментарями | 0.5 |
| 3 | Скріншоти операцій HDFS (Завдання 1.3) | 1.0 |
| 4 | Відповіді на запитання щодо великого файлу (Завдання 1.4) | 0.5 |
| 5 | Відповіді на контрольні запитання | 0.5 |
| | **Разом** | **3** |

---

# Заняття 2: Програмування MapReduce з Hadoop Streaming (2 години)

**Бали: 4**

---

## 2.1. Що таке Hadoop Streaming (15 хв)

Hadoop Streaming дозволяє писати MapReduce-завдання на **будь-якій мові** (Python, Ruby, Bash тощо) замість Java. Маппер та редюсер — це окремі програми, які читають зі `stdin` та пишуть у `stdout`.

```
Вхід (HDFS) → stdin → МАППЕР → stdout → [Shuffle & Sort] → stdin → РЕДЮСЕР → stdout → Вихід (HDFS)
```

Правила:
- **Маппер** читає рядки зі `stdin`, виводить пари `ключ\tзначення` у `stdout`
- Фреймворк **сортує** весь вивід маппера за ключем
- **Редюсер** читає пари `ключ\tзначення` зі `stdin` (згруповані за ключем), виводить результати у `stdout`
- Ключ та значення розділяються **символом табуляції** (`\t`)

---

## 2.2. Word Count з Hadoop Streaming (30 хв)

### Крок 1. Написати маппер

```bash
cat > mapper.py << 'PYEOF'
#!/usr/bin/env python3
"""Маппер: читає рядки зі stdin, видає (слово, 1) для кожного слова."""
import sys

for line in sys.stdin:
    line = line.strip().lower()
    words = line.split()
    for word in words:
        # Видалити пунктуацію
        word = ''.join(c for c in word if c.isalnum())
        if word:
            print(f"{word}\t1")
PYEOF

chmod +x mapper.py
```

### Крок 2. Написати редюсер

```bash
cat > reducer.py << 'PYEOF'
#!/usr/bin/env python3
"""Редюсер: читає пари (слово, кількість) зі stdin, підсумовує кількість для кожного слова."""
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

# Вивести останнє слово
if current_word is not None:
    print(f"{current_word}\t{current_count}")
PYEOF

chmod +x reducer.py
```

### Крок 3. Тестування локально перед запуском на Hadoop

Завжди тестуйте маппер та редюсер локально:

```bash
# Тест маппера
echo "Hello World Hello Hadoop World" | python3 mapper.py

# Очікуваний вивід:
# hello   1
# world   1
# hello   1
# hadoop  1
# world   1

# Тест повного конвеєру локально (імітація shuffle & sort через 'sort')
echo "Hello World Hello Hadoop World" | python3 mapper.py | sort | python3 reducer.py

# Очікуваний вивід:
# hadoop  1
# hello   2
# world   2
```

### Крок 4. Запуск на Hadoop

```bash
# Переконатися, що вхідні дані існують в HDFS
hdfs dfs -ls /user/student/data/raw/

# Знайти jar-файл Hadoop Streaming
STREAMING_JAR=$(find $HADOOP_PREFIX/share/hadoop/tools/lib/ \
  -name "hadoop-streaming-*.jar" | head -1)
echo "Streaming JAR: $STREAMING_JAR"

# Запустити MapReduce-завдання
hadoop jar $STREAMING_JAR \
  -input /user/student/data/raw/large_file.txt \
  -output /user/student/data/processed/wordcount-output \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -file mapper.py \
  -file reducer.py

# Переглянути результати
hdfs dfs -ls /user/student/data/processed/wordcount-output/
hdfs dfs -cat /user/student/data/processed/wordcount-output/part-00000
```

> **Завдання 2.1:** Запустіть завдання Word Count на великому файлі з Заняття 1. Зробіть скріншот:
> 1. Виводу терміналу з прогресом виконання (відсотки map/reduce)
> 2. Кінцевих результатів підрахунку слів
> 3. Веб-інтерфейсу ResourceManager (http://localhost:8088) із завершеним завданням

---

## 2.3. Власне MapReduce-завдання: аналізатор логів (45 хв)

Тепер напишіть власне MapReduce-завдання з нуля. Ми будемо аналізувати серверні логи доступу.

### Крок 1. Генерація тестових логів

```bash
cat > generate_logs.py << 'PYEOF'
#!/usr/bin/env python3
"""Генерація реалістичних серверних логів доступу."""
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

print("Згенеровано 100 000 записів логів")
PYEOF

python3 generate_logs.py 2>/dev/null || python generate_logs.py

# Попередній перегляд
head -5 access_logs.txt

# Завантажити в HDFS
hdfs dfs -put access_logs.txt /user/student/data/raw/
```

### Крок 2. Завдання A — Підрахунок запитів за HTTP-статусом

Напишіть маппер та редюсер, що рахують кількість запитів для кожного HTTP-коду стану (200, 404, 500 тощо).

**Маппер (`status_mapper.py`):**

```bash
cat > status_mapper.py << 'PYEOF'
#!/usr/bin/env python3
"""Маппер: витягти HTTP-код стану з кожного рядка логу."""
import sys

for line in sys.stdin:
    line = line.strip()
    try:
        # Формат логу: IP - - [timestamp] "METHOD /path HTTP/1.1" STATUS SIZE
        parts = line.split('"')
        after_request = parts[2].strip().split()
        status = after_request[0]
        print(f"{status}\t1")
    except (IndexError, ValueError):
        continue
PYEOF

chmod +x status_mapper.py
```

**Редюсер** — використайте `reducer.py` з прикладу Word Count (він вже підсумовує кількість за ключем).

```bash
# Тестування локально
head -20 access_logs.txt | python3 status_mapper.py | sort | python3 reducer.py

# Запуск на Hadoop
hadoop jar $STREAMING_JAR \
  -input /user/student/data/raw/access_logs.txt \
  -output /user/student/data/processed/status-count \
  -mapper "python3 status_mapper.py" \
  -reducer "python3 reducer.py" \
  -file status_mapper.py \
  -file reducer.py

# Переглянути результати
hdfs dfs -cat /user/student/data/processed/status-count/part-00000
```

### Крок 3. Завдання B — Топ IP-адрес за кількістю запитів (Самостійно)

> **Завдання 2.2 (Самостійно):** Напишіть MapReduce-завдання, що рахує кількість запитів від кожної IP-адреси. Потрібно:
>
> 1. Написати `ip_mapper.py` — витягти IP-адресу з кожного рядка логу та видати `(ip, 1)`
> 2. Використати `reducer.py` для підрахунку
> 3. Спочатку протестувати локально, потім запустити на Hadoop
> 4. Знайти 5 найактивніших IP-адрес з результатів
>
> **Підказка:** IP-адреса — це перше поле в кожному рядку логу.

### Крок 4. Завдання C — Підрахунок запитів за ендпоінтом (Самостійно)

> **Завдання 2.3 (Самостійно):** Напишіть MapReduce-завдання, що рахує запити за ендпоінтом (URL-шляхом). Потрібно:
>
> 1. Написати `endpoint_mapper.py` — витягти ендпоінт із рядка запиту (наприклад, `/api/login`) та видати `(endpoint, 1)`
> 2. Запустити завдання та проаналізувати, які ендпоінти отримують найбільше трафіку
>
> **Підказка:** ендпоінт знаходиться всередині рядка запиту в лапках, наприклад, `"GET /api/login HTTP/1.1"`.

---

## 2.4. Аналіз виконання завдання (15 хв)

Після запуску завдань перегляньте деталі виконання:

```bash
# Перелік всіх завершених додатків
yarn application -list -appStates FINISHED

# Детальна інформація про конкретне завдання (замініть на ваш ID додатку)
# yarn application -status application_XXXX_XXXX

# Перегляд лічильників завдання (вхідні записи, вихідні записи тощо)
# mapred job -counter job_XXXX_XXXX
```

У **веб-інтерфейсі ResourceManager** (http://localhost:8088) натисніть на завершений додаток та перегляньте:

- Скільки задач map було запущено?
- Скільки задач reduce було запущено?
- Скільки часу зайняла кожна фаза?
- Які були кількості вхідних/вихідних записів?

> **Завдання 2.4:** Для одного з ваших MapReduce-завдань надайте скріншот з веб-інтерфейсу, що показує деталі завдання (кількість маперів/редюсерів, час виконання, лічильники).

---

## Заняття 2 — Контрольні запитання

1. Що таке Hadoop Streaming і чому він корисний?
2. Чому ми тестуємо маппер та редюсер локально перед запуском на Hadoop?
3. Яка роль фази Shuffle & Sort між маппером та редюсером?
4. Чому вивід маппера повинен бути у форматі `ключ\tзначення` (розділений табуляцією)?

---

## Заняття 2 — Результати для здачі

| # | Результат | Бали |
|---|-----------|------|
| 1 | Результати Word Count + скріншоти (Завдання 2.1) | 1.0 |
| 2 | Підрахунок за статусом — робочий код + вивід | 0.5 |
| 3 | Підрахунок за IP — робочий код + топ-5 IP (Завдання 2.2) | 1.0 |
| 4 | Підрахунок за ендпоінтом — робочий код + вивід (Завдання 2.3) | 1.0 |
| 5 | Скріншот аналізу виконання завдання (Завдання 2.4) | 0.5 |
| | **Разом** | **4** |

---

# Заняття 3: Моніторинг, усунення проблем та реальні дані (2 години)

**Бали: 3**

---

## 3.1. Поглиблений моніторинг кластера Hadoop (30 хв)

### Веб-інтерфейс NameNode (http://localhost:9870)

Відкрийте інтерфейс NameNode та дослідіть наступні розділи. Запишіть свої спостереження.

**Сторінка Overview:**
- Загальна ємність HDFS та відсоток використання
- Кількість живих/мертвих DataNode
- Кількість файлів та блоків
- Використання пам'яті Heap

**Вкладка Datanodes:**
- Ім'я хоста та статус DataNode
- Останній контакт (heartbeat)
- Ємність, використано, залишилося
- Кількість збережених блоків

**Utilities → Browse the file system:**
- Перейдіть до `/user/student/data/`
- Натисніть на файл, щоб побачити: власник, група, розмір, реплікація, розмір блоку
- Зверніть увагу на ID блоків

> **Завдання 3.1:** Зробіть скріншоти:
> 1. Сторінки Overview NameNode
> 2. Браузера файлів з вашими завантаженими файлами та їх розмірами та реплікацією

### Веб-інтерфейс ResourceManager (http://localhost:8088)

Дослідіть:
- **Cluster Metrics** — загальна пам'ять, використана пам'ять, vCores
- **Applications** — перелік усіх ваших надісланих MapReduce-завдань
- Натисніть на будь-який завершений додаток:
  - **Attempt** → **Logs** для перегляду логів маппера/редюсера
  - Зверніть увагу на ID контейнерів та їхній статус завершення

> **Завдання 3.2:** Знайдіть логи маппера з одного з ваших MapReduce-завдань. Зробіть скріншот, що показує вивід stdout або stderr маппера.

### Моніторинг YARN з командного рядка

```bash
# Огляд ресурсів кластера
yarn top

# Або перевірити ресурси вузлів
yarn node -list -showDetails

# Логи додатку (замініть на ваш ID додатку)
# yarn logs -applicationId application_XXXX_XXXX
```

---

## 3.2. Обробка реального набору даних (60 хв)

Тепер застосуємо все вивчене для обробки реального набору даних.

### Набір даних: Міста світу

Ми використаємо набір даних про міста з даними про населення.

```bash
# Згенерувати реалістичний набір даних про міста
cat > generate_cities.py << 'PYEOF'
#!/usr/bin/env python3
"""Генерація CSV-набору даних міст світу з населенням та координатами."""

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
    ("Cape Town", "South Africa", "Africa", 33930000, 33.93, 18.42),
    ("Istanbul", "Turkey", "Europe", 15462452, 41.01, 28.98),
    ("Moscow", "Russia", "Europe", 12506468, 55.76, 37.62),
]

with open("cities.csv", "w") as f:
    f.write("city,country,continent,population,latitude,longitude\n")
    for city, country, continent, pop, lat, lng in cities_data:
        f.write(f"{city},{country},{continent},{pop},{lat},{lng}\n")

print(f"Згенеровано {len(cities_data)} міст")
PYEOF

python3 generate_cities.py 2>/dev/null || python generate_cities.py

# Попередній перегляд
head -5 cities.csv

# Завантажити в HDFS
hdfs dfs -mkdir -p /user/student/project/input
hdfs dfs -put cities.csv /user/student/project/input/
```

### Завдання A: Загальне населення по країнах

Напишіть MapReduce-завдання, що обчислює загальне населення по країнах.

```bash
cat > country_pop_mapper.py << 'PYEOF'
#!/usr/bin/env python3
"""Маппер: видати (країна, населення) для кожного міста."""
import sys

for line in sys.stdin:
    line = line.strip()
    # Пропустити заголовок
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
"""Редюсер: підсумувати значення за ключем."""
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

# Тестування локально
cat cities.csv | python3 country_pop_mapper.py | sort | python3 sum_reducer.py

# Запуск на Hadoop
hadoop jar $STREAMING_JAR \
  -input /user/student/project/input/cities.csv \
  -output /user/student/project/output/country-population \
  -mapper "python3 country_pop_mapper.py" \
  -reducer "python3 sum_reducer.py" \
  -file country_pop_mapper.py \
  -file sum_reducer.py

# Переглянути результати
hdfs dfs -cat /user/student/project/output/country-population/part-00000
```

### Завдання B: Загальне населення по континентах (Самостійно)

> **Завдання 3.3:** Напишіть MapReduce-завдання, що обчислює загальне населення по континентах.
> 1. Напишіть `continent_mapper.py` — видати `(континент, населення)` для кожного міста
> 2. Використайте `sum_reducer.py`
> 3. Запустіть на Hadoop та запишіть результати
> 4. Який континент має найбільше загальне населення у наборі даних?

### Завдання C: Знайти найбільше місто в кожній країні (Самостійно, Бонус)

> **Завдання 3.4 (Бонус):** Напишіть MapReduce-завдання, що знаходить місто з найбільшим населенням у кожній країні.
> 1. Напишіть `max_city_mapper.py` — видати `(країна, населення:назва_міста)`
> 2. Напишіть `max_reducer.py` — для кожної країни вивести лише місто з найбільшим населенням
> 3. Запустіть та перевірте результати
>
> **Підказка:** у редюсері порівнюйте значення населення та відстежуйте максимум.

---

## 3.3. Очищення та управління даними (15 хв)

Практика управління даними в HDFS:

```bash
# Перевірити загальне використання диску
hdfs dfs -du -h /user/student/

# Видалити проміжні результати для звільнення місця
hdfs dfs -rm -r /user/student/data/processed/wordcount-output
hdfs dfs -rm -r /user/student/data/processed/status-count

# Перевірити очищення
hdfs dfs -du -h /user/student/

# Очистити кошик (HDFS має функцію кошика, подібну до recycle bin)
hdfs dfs -expunge
```

Після завершення всіх занять:

```bash
# Вийти з контейнера
exit

# Зупинити контейнер (зберігає дані для подальшого використання)
docker stop hadoop-sandbox

# Для відновлення пізніше:
# docker start -i hadoop-sandbox

# Для повного видалення (всі дані будуть втрачені):
# docker rm hadoop-sandbox
```

---

## Заняття 3 — Контрольні запитання

1. Яку інформацію можна знайти у веб-інтерфейсі NameNode, яку не можна отримати з командного рядка?
2. Як знайти логи невдалої MapReduce-задачі?
3. Що б ви змінили у вашому MapReduce-завданні, якби вхідний файл був 100 ГБ замість 50 МБ?
4. Чому важливо тестувати маппер та редюсер локально перед запуском на кластері?

---

## Заняття 3 — Результати для здачі

| # | Результат | Бали |
|---|-----------|------|
| 1 | Скріншоти NameNode та ResourceManager UI (Завдання 3.1, 3.2) | 1.0 |
| 2 | Населення по континентах — робочий код + результати (Завдання 3.3) | 1.0 |
| 3 | Відповіді на контрольні запитання | 0.5 |
| 4 | Найбільше місто по країнах — бонус (Завдання 3.4) | 0.5 |
| | **Разом** | **3** |

---

## Підсумок усіх занять

| Заняття | Фокус | Ключові навички | Бали |
|---------|-------|-----------------|------|
| 1 | Середовище + HDFS | Конфігурація, файлові операції, перевірка блоків | 3 |
| 2 | Програмування MapReduce | Hadoop Streaming, маппер/редюсер на Python, аналіз логів | 4 |
| 3 | Моніторинг + реальні дані | Веб-інтерфейси, логи YARN, конвеєр обробки даних | 3 |
| **Разом** | | | **10** |

---

## Самостійна робота (12 годин)

**Тема:** Практичні завдання MapReduce. Робота з HDFS. Підготовка звітів.

### Завдання:

1. **Звіт з лабораторних робіт** (усі заняття разом):
   - Зберіть усі скріншоти, код та виводи в єдиний звіт
   - Включіть короткі пояснення, що робить кожна команда/завдання
   - Дайте відповіді на всі контрольні запитання з усіх трьох занять

2. **Додаткове MapReduce-завдання** (оберіть одне):
   - **Варіант A:** Напишіть MapReduce-завдання, що обчислює середнє населення міст по континентах
   - **Варіант B:** Напишіть MapReduce-завдання, що обробляє логи доступу для пошуку найбільш завантаженої години доби (найбільше запитів)
   - **Варіант C:** Напишіть MapReduce-завдання, що знаходить усі IP-адреси, які генерували помилки 404, та рахує кількість 404 для кожної IP

3. **Рефлексія** (0.5 сторінки):
   - Що було найскладнішим у роботі з Hadoop?
   - Чим відрізняється запуск завдання на одновузловому Docker-кластері від реального багатовузлового кластера?
   - Назвіть одну перевагу та один недолік Hadoop Streaming порівняно з написанням MapReduce на Java

---

## Рекомендовані ресурси

- [Заняття 4 — Екосистема Apache Hadoop](../lesson1-4/README.md)
- Документація Hadoop Streaming: https://hadoop.apache.org/docs/stable/hadoop-streaming/HadoopStreaming.html
- Довідник команд HDFS: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/FileSystemShell.html
- Команди YARN: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YarnCommands.html
- Docker-образ Hadoop: https://hub.docker.com/r/sequenceiq/hadoop-docker
