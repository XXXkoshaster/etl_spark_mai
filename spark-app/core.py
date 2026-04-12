from __future__ import annotations

import os
from abc import ABC, abstractmethod

import yaml
from pyspark.sql import SparkSession, DataFrame


def load_config(path: str = "config.yaml") -> dict:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, path)) as f:
        cfg = yaml.safe_load(f)

    cfg["postgres_dds"]["database"] = os.environ["POSTGRES_DB"]
    cfg["postgres_dds"]["user"] = os.environ["POSTGRES_USER"]
    cfg["postgres_dds"]["password"] = os.environ["POSTGRES_PASSWORD"]

    cfg["postgres_ods"]["database"] = os.environ.get("POSTGRES_ODS_DB", "ods")
    cfg["postgres_ods"]["user"] = os.environ["POSTGRES_USER"]
    cfg["postgres_ods"]["password"] = os.environ["POSTGRES_PASSWORD"]

    cfg["clickhouse"]["user"] = os.environ.get("CLICKHOUSE_USER", "default")
    cfg["clickhouse"]["password"] = os.environ.get("CLICKHOUSE_PASSWORD", "")

    return cfg


class TableCommand(ABC):
    name: str

    @abstractmethod
    def execute(self, df: DataFrame, dims: dict[str, DataFrame] | None = None) -> DataFrame:
        ...


class DimensionCommand(TableCommand):
    @abstractmethod
    def execute(self, df: DataFrame, dims: dict[str, DataFrame] | None = None) -> DataFrame:
        ...


class FactCommand(TableCommand):
    @abstractmethod
    def execute(self, df: DataFrame, dims: dict[str, DataFrame] | None = None) -> DataFrame:
        ...


class MartCommand(ABC):
    name: str

    @abstractmethod
    def execute(self, tables: dict[str, DataFrame]) -> DataFrame:
        ...

class TableRegistry:
    def __init__(self):
        self._dimensions: list[DimensionCommand] = []
        self._facts: list[FactCommand] = []
        self._marts: list[MartCommand] = []

    def register(self, executeer: TableCommand | MartCommand):
        if isinstance(executeer, DimensionCommand):
            self._dimensions.append(executeer)
        elif isinstance(executeer, FactCommand):
            self._facts.append(executeer)
        elif isinstance(executeer, MartCommand):
            self._marts.append(executeer)
        return self

    @property
    def dimensions(self) -> list[DimensionCommand]:
        return list(self._dimensions)

    @property
    def facts(self) -> list[FactCommand]:
        return list(self._facts)

    @property
    def marts(self) -> list[MartCommand]:
        return list(self._marts)

class LoadStrategy(ABC):
    def configure(self, builder):
        return builder

    @abstractmethod
    def load(self, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        ...

    @abstractmethod
    def read(self, spark: SparkSession, table_name: str) -> DataFrame:
        ...


class CsvStrategy(LoadStrategy):
    def __init__(self, base_path: str):
        self._base_path = base_path

    def read(self, spark: SparkSession, table_name: str) -> DataFrame:
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .option("escape", '"')
            .csv(self._base_path)
        )

    def load(self, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        df.write.csv(
            f"{self._base_path}/{table_name}",
            header=True, mode=mode,
        )


class PostgresStrategy(LoadStrategy):
    def __init__(self, pg_cfg: dict):
        self._url = f"jdbc:postgresql://{pg_cfg['host']}:{pg_cfg['port']}/{pg_cfg['database']}"
        self._props = {
            "user": pg_cfg["user"],
            "password": pg_cfg["password"],
            "driver": pg_cfg["driver"],
            "truncate": "true",
            "cascadeTruncate": "true",
        }

    def read(self, spark: SparkSession, table_name: str) -> DataFrame:
        return spark.read.jdbc(url=self._url, table=table_name, properties=self._props)

    def load(self, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        df.write.jdbc(url=self._url, table=table_name, mode=mode, properties=self._props)

class ClickHouseStrategy(LoadStrategy):
    def __init__(self, ch_cfg: dict):
        self._url = (
            f"jdbc:ch://{ch_cfg['host']}:{ch_cfg['port']}"
            f"/{ch_cfg['database']}"
        )
        self._props = {
            "user": ch_cfg["user"],
            "password": ch_cfg["password"],
            "driver": ch_cfg["driver"],
        }

    def read(self, spark: SparkSession, table_name: str) -> DataFrame:
        return spark.read.jdbc(url=self._url, table=table_name, properties=self._props)

    def load(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        df.write \
            .format("jdbc") \
            .option("url", self._url) \
            .option("dbtable", table_name) \
            .option("user", self._props["user"]) \
            .option("password", self._props["password"]) \
            .option("driver", self._props["driver"]) \
            .mode(mode) \
            .save()

class ETLPipeline:
    def __init__(
        self,
        spark: SparkSession,
        registry: TableRegistry,
        source: LoadStrategy,
        target: LoadStrategy,
        source_table: str,
    ):
        self.spark = spark
        self.registry = registry
        self.source = source
        self.target = target
        self.source_table = source_table

    def run(self):
        raw = self._extract()
        tables = self._transform(raw)
        self._load(tables)

    def _extract(self) -> DataFrame:
        df = self.source.read(self.spark, self.source_table)
        df.cache()
        return df

    def _transform(self, df: DataFrame) -> dict[str, DataFrame]:
        tables: dict[str, DataFrame] = {}
        for dim in self.registry.dimensions:
            tables[dim.name] = dim.execute(df)
        for fact in self.registry.facts:
            tables[fact.name] = fact.execute(df, dims=tables)
        return tables

    def _load(self, tables: dict[str, DataFrame]) -> None:
        for name, df in tables.items():
            self.target.load(df, name)


class MartPipeline:
    def __init__(
        self,
        spark: SparkSession,
        registry: TableRegistry,
        source: LoadStrategy,
        target: LoadStrategy,
        source_tables: list[str],
    ):
        self.spark = spark
        self.registry = registry
        self.source = source
        self.target = target
        self.source_tables = source_tables

    def run(self):
        tables = self._extract()
        marts = self._transform(tables)
        self._load(marts)

    def _extract(self) -> dict[str, DataFrame]:
        return {name: self.source.read(self.spark, name) for name in self.source_tables}

    def _transform(self, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
        return {mart.name: mart.execute(tables) for mart in self.registry.marts}

    def _load(self, marts: dict[str, DataFrame]) -> None:
        for name, df in marts.items():
            self.target.load(df, name)
