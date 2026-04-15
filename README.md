
## Быстрый старт

### 1. Создайте файл `.env`

```env
POSTGRES_DB=star
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

### 2. Запустите стек

```bash
docker compose up -d --build
```
### 3. Запустите ETL-пайплайн

Пайплайны запускаются последовательно:

```bash
# Шаг 1: Загрузка CSV → ODS (Postgres, БД ods)
docker exec -it spark-master \
/opt/spark/bin/spark-submit /opt/spark-app/staging.py

# Шаг 2: Построение звёздной схемы ODS → DDS (Postgres, БД star)
docker exec -it spark-master \
  /opt/spark/bin/spark-submit /opt/spark-app/dds.py

# Шаг 3: Построение витрин DDS → Marts (ClickHouse, БД reports)
docker exec -it spark-master \
  /opt/spark/bin/spark-submit /opt/spark-app/data_mart.py
```
