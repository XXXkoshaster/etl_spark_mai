from __future__ import annotations

from pyspark.sql import SparkSession

from core import load_config, CsvStrategy, PostgresStrategy


def main():
    cfg = load_config()

    spark = (
        SparkSession.builder
        .master(cfg["spark"]["master"])
        .appName("csv_to_ods")
        .getOrCreate()
    )

    source = CsvStrategy("/opt/spark-data/csv")
    target = PostgresStrategy(cfg["postgres_ods"])

    df = source.read(spark, "mock_data")
    target.load(df, "mock_data", mode="overwrite")

    print(f"Loaded {df.count()} rows into ods.mock_data")
    spark.stop()


if __name__ == "__main__":
    main()
