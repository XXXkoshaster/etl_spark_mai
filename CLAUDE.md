# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Stack & Services

The project is a containerised PySpark ETL playground that builds a star schema in Postgres and projects analytic marts into ClickHouse. All services run via [docker-compose.yml](docker-compose.yml):

- `postgres` (5432) — source DB seeded from [init_db/](init_db/) on first start; star schema DDL/DML lives there.
- `clickhouse` (8123) — target for the marts pipeline. Init scripts mount from `docker/clickhouse/init`.
- `spark-master` / `spark-worker` (7077, 8080/8081, 4040) — built from [Dockerfile.spark](Dockerfile.spark), which bakes in the Postgres and ClickHouse JDBC jars under `/opt/spark/jars/`. Mounts `./spark-app` to `/opt/spark-app`.
- `jupyter` (8888) — built from [Dockerfile](Dockerfile); a separate PySpark image with JupyterLab. Connects to the master via `PYSPARK_MASTER`.
- `pgadmin` (8050).

A `.env` file at the repo root supplies `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD`; these are forwarded into the Postgres and Spark containers and re-injected into the YAML config at runtime by `load_config`.

## Common Commands

```bash
# Bring up the whole stack (rebuilds Spark/Jupyter images on first run)
docker compose up -d --build

# Run the star-schema ETL (Postgres → Postgres)
docker exec -it spark-master \
  /opt/spark/bin/spark-submit /opt/spark-app/star_pg.py

# Run the marts pipeline (Postgres → ClickHouse)
docker exec -it spark-master \
  /opt/spark/bin/spark-submit /opt/spark-app/marts.py

# Tail logs / debug
docker compose logs -f spark-master
docker exec -it postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

There is no test suite, linter config, or build script in the repo — running pipelines via `spark-submit` is the only way they execute. Postgres bootstrap (DDL + DML) only runs on a fresh `./data` volume.

## Architecture

The ETL framework lives in [spark-app/core.py](spark-app/core.py) and is intentionally split into three orthogonal abstractions so that new pipelines can be added by writing only the per-table transform classes:

1. **Commands** (`DimensionCommand`, `FactCommand`, `MartCommand`) — each subclass owns a `name` and an `execute(...)` that returns a `DataFrame`. Dimensions run first and receive only the raw frame; facts run next and receive the dict of already-built dimension frames via the `dims=` argument so they can join against surrogate keys; marts receive the full dict of source tables (already loaded by the pipeline).
2. **`TableRegistry`** — buckets registered commands into dimensions / facts / marts and preserves registration order. `ETLPipeline` and `MartPipeline` iterate the registry to drive their stages.
3. **`LoadStrategy`** (`PostgresStrategy`, `ClickHouseStrategy`) — encapsulates JDBC `read` / `load`. Pipelines accept `source` and `target` strategies independently, which is what lets `marts.py` read from Postgres and write to ClickHouse with no other changes.

`ETLPipeline.run()` is `extract → transform (dims, then facts) → load`. `MartPipeline.run()` is `extract (read each named source table) → transform (run each mart) → load`. Both call `target.load(df, name)` per table; mode defaults to `overwrite` with truncate, so re-running a pipeline replaces the destination tables.

### Concrete pipelines

- [spark-app/star_pg.py](spark-app/star_pg.py) — defines `DimCustomer/Seller/Product/Store/Supplier/PetCategory/Date` and `FactSales`, then wires them into `ETLPipeline` with `PostgresStrategy` for both source and target. The source table is the raw `mock_data` (loaded into Postgres from [csv_data/](csv_data/) — see `init_db` for the expected column names). Surrogate keys are generated with `monotonically_increasing_id() + 1`; the fact joins each dimension on the original natural keys to attach those surrogates before projecting only the keys + measures.
- [spark-app/marts.py](spark-app/marts.py) — defines six `MartCommand`s (`mart_sales_by_product/customer/time/store/supplier`, `mart_product_quality`) that join the star tables already in Postgres and write aggregates to ClickHouse via `ClickHouseStrategy`. Source tables to read are passed explicitly to `MartPipeline`.
- The schema these pipelines target is in [init_db/ddl_star.sql](init_db/ddl_star.sql); [init_db/dml_star.sql](init_db/dml_star.sql) is the SQL-only equivalent of `star_pg.py` and is what runs at Postgres bootstrap.

### Adding a new table

1. Subclass the right command type in `star_pg.py` or `marts.py`, set `name`, implement `execute`.
2. `.register(...)` it on the `TableRegistry` in `main()`. Order matters for the ETL pipeline: dimensions must be registered before any fact that joins them.
3. If a mart needs a new source table, add its name to the `source_tables` list passed to `MartPipeline`.

### Known rough edges to be aware of

- `ClickHouseStrategy` in [spark-app/core.py](spark-app/core.py) currently builds a `jdbc:postgresql://...` URL and never passes the `dbtable` option in `read`/`load` — it will not actually work against ClickHouse without fixes. The repo has an `error_clickhouse.log` documenting failures here.
- `config.yaml` sets `spark.master: local`, but the compose stack runs a real master at `spark://spark-master:7077`. Override via the YAML or env if you want jobs to land on the worker rather than running in-process.
- `spark-app/export_to.py` is shown as deleted in `git status`; do not reintroduce it without checking with the user.
