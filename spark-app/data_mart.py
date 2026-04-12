from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, round as spark_round, countDistinct,
)

from core import (
    load_config, MartCommand, TableRegistry,
    PostgresStrategy, MartPipeline, ClickHouseStrategy,
)

class SalesByProduct(MartCommand):
    name = "mart_sales_by_product"

    def execute(self, tables):
        fact = tables["fact_sales"]
        product = tables["dim_product"]

        return (
            fact
            .join(product, "product_id")
            .groupBy("product_id", "name", "category", "brand", "rating", "reviews")
            .agg(
                spark_sum("sale_quantity").alias("total_quantity"),
                spark_round(spark_sum("sale_total_price"), 2).alias("total_revenue"),
                count("sale_id").alias("orders_count"),
            )
        )

class SalesByCustomer(MartCommand):
    name = "mart_sales_by_customer"

    def execute(self, tables):
        fact = tables["fact_sales"]
        customer = tables["dim_customer"]

        return (
            fact
            .join(customer, "customer_id")
            .groupBy("customer_id", "first_name", "last_name", "country")
            .agg(
                count("sale_id").alias("orders_count"),
                spark_round(spark_sum("sale_total_price"), 2).alias("total_spent"),
                spark_round(avg("sale_total_price"), 2).alias("avg_order_value"),
            )
        )

class SalesByTime(MartCommand):
    name = "mart_sales_by_time"

    def execute(self, tables):
        fact = tables["fact_sales"]
        date = tables["dim_date"]

        return (
            fact
            .join(date, "date_id")
            .groupBy("year", "month")
            .agg(
                count("sale_id").alias("orders_count"),
                spark_round(spark_sum("sale_total_price"), 2).alias("total_revenue"),
                spark_sum("sale_quantity").alias("total_quantity"),
                spark_round(avg("sale_total_price"), 2).alias("avg_order_value"),
            )
            .orderBy("year", "month")
        )


class SalesByStore(MartCommand):
    name = "mart_sales_by_store"

    def execute(self, tables):
        fact = tables["fact_sales"]
        store = tables["dim_store"]

        return (
            fact
            .join(store, "store_id")
            .groupBy("store_id", "name", "city", "country")
            .agg(
                count("sale_id").alias("orders_count"),
                spark_round(spark_sum("sale_total_price"), 2).alias("total_revenue"),
                spark_round(avg("sale_total_price"), 2).alias("avg_order_value"),
            )
        )

class SalesBySupplier(MartCommand):
    name = "mart_sales_by_supplier"

    def execute(self, tables):
        fact = tables["fact_sales"]
        supplier = tables["dim_supplier"].alias("s")
        product = tables["dim_product"].alias("p")

        return (
            fact
            .join(supplier, "supplier_id")
            .join(product, "product_id")
            .groupBy(
                col("supplier_id"),
                col("s.name").alias("supplier_name"),
                col("s.country").alias("supplier_country"),
            )
            .agg(
                spark_round(spark_sum("sale_total_price"), 2).alias("total_revenue"),
                spark_round(avg(col("p.price")), 2).alias("avg_product_price"),
                count("sale_id").alias("orders_count"),
            )
        )

class ProductQuality(MartCommand):
    name = "mart_product_quality"

    def execute(self, tables):
        fact = tables["fact_sales"]
        product = tables["dim_product"]

        return (
            product
            .join(fact, "product_id", how="left")
            .groupBy("product_id", "name", "category", "rating", "reviews")
            .agg(
                spark_sum("sale_quantity").alias("total_quantity"),
                spark_round(spark_sum("sale_total_price"), 2).alias("total_revenue"),
                count("sale_id").alias("orders_count"),
                countDistinct("customer_id").alias("unique_customers"),
            )
        )

def main():
    cfg = load_config()

    source = PostgresStrategy(cfg["postgres"])
    target = ClickHouseStrategy(cfg["clickhouse"])

    builder = (
        SparkSession.builder
        .master(cfg["spark"]["master"])
        .appName(cfg["spark"]["app_name"])
    )
    
    spark = builder.getOrCreate()

    registry = (
        TableRegistry()
        .register(SalesByProduct())
        .register(SalesByCustomer())
        .register(SalesByTime())
        .register(SalesByStore())
        .register(SalesBySupplier())
        .register(ProductQuality())
    )

    pipeline = MartPipeline(
        spark=spark,
        registry=registry,
        source=source,
        target=target,
        source_tables=[
            "fact_sales",
            "dim_customer",
            "dim_product",
            "dim_date",
            "dim_store",
            "dim_supplier",
        ],
    )

    pipeline.run()
    print("Marts pipeline completed.")
    spark.stop()


if __name__ == "__main__":
    main()
