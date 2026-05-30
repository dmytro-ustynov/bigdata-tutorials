# Заняття 2-3. Аналітика кібербезпеки за допомогою Apache Spark (Практична)

> Read in [English](README.md)

**Дисципліна:** BIG DATA (Обробка надвеликих масивів даних)

**Змістовий модуль 2:** Apache Spark та машинне навчання на великих даних

**Тип:** Практичне заняття

**Тривалість:** 6 годин

---

## ⚠️ Це заняття розміщене в окремому репозиторії

Повні матеріали практичної роботи, вихідний код, Docker-середовище та покрокові
інструкції для Заняття 2-3 підтримуються в окремому репозиторії:

### 👉 [dmytro-ustynov/spark-tutorial](https://github.com/dmytro-ustynov/spark-tutorial)

Клонуйте його, щоб розпочати:

```bash
git clone https://github.com/dmytro-ustynov/spark-tutorial.git
cd spark-tutorial
```

---

## Огляд

Ця практична робота симулює реальні події кібербезпеки та навчає студентів
виявляти атаки за допомогою аналітики **Structured Streaming** в Apache Spark. Усе
середовище лабораторної роботи (Spark, Kafka, PostgreSQL, генератор логів,
Jupyter Lab) постачається у вигляді контейнерів — **потрібен лише Docker**.

## Навчальні цілі

Після виконання лабораторної роботи студенти повинні вміти:

- обробляти потоки подій безпеки в реальному часі за допомогою Spark Structured Streaming;
- виявляти атаки методом перебору (brute-force) за допомогою агрегацій за часом;
- ідентифікувати DDoS-атаки через аналіз патернів трафіку;
- зберігати результати аналітики в PostgreSQL;
- обробляти дані, що надходять із запізненням, та реалізовувати watermarking;
- будувати масштабовані системи виявлення загроз.

## Передумови

- Завершені [Заняття 2-1](../lesson2-1/README_ua.md) (теорія Spark) та
  [Заняття 2-2](../lesson2-2/README_ua.md) (налаштування Spark та DataFrame API)
- **Docker та Docker Compose**
- Python 3.7+ (лише у разі використання локального варіанту Spark)

## З чого почати в репозиторії spark-tutorial

- [`README.md`](https://github.com/dmytro-ustynov/spark-tutorial/blob/main/README.md) — огляд проєкту та архітектура
- [`GETTING_STARTED.md`](https://github.com/dmytro-ustynov/spark-tutorial/blob/main/GETTING_STARTED.md) — налаштування середовища
- [`DETECTION_GUIDE.md`](https://github.com/dmytro-ustynov/spark-tutorial/blob/main/DETECTION_GUIDE.md) — покрокове виявлення атак
- [`examples/`](https://github.com/dmytro-ustynov/spark-tutorial/tree/main/examples) — еталонні рішення
- [`student-work/`](https://github.com/dmytro-ustynov/spark-tutorial/tree/main/student-work) — робоча область для завдань

---

## Ресурси

- 🔗 **Репозиторій заняття:** https://github.com/dmytro-ustynov/spark-tutorial
- [Apache Spark — Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
