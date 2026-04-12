from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, dayofmonth, month, year,
    monotonically_increasing_id,
)

from core import (
    load_config, DimensionCommand, FactCommand,
    TableRegistry, PostgresStrategy, ETLPipeline,
)

class DimCustomer(DimensionCommand):
    name = "dim_customer"

    def execute(self, df, dims=None):
        return (
            df.select(
                col("customer_first_name").alias("first_name"),
                col("customer_last_name").alias("last_name"),
                col("customer_age").alias("age"),
                col("customer_email").alias("email"),
                col("customer_country").alias("country"),
                col("customer_postal_code").alias("postal_code"),
                col("customer_pet_type").alias("pet_type"),
                col("customer_pet_name").alias("pet_name"),
                col("customer_pet_breed").alias("pet_breed"),
            )
            .dropDuplicates()
            .withColumn("customer_id", monotonically_increasing_id() + 1)
        )


class DimSeller(DimensionCommand):
    name = "dim_seller"

    def execute(self, df, dims=None):
        return (
            df.select(
                col("seller_first_name").alias("first_name"),
                col("seller_last_name").alias("last_name"),
                col("seller_email").alias("email"),
                col("seller_country").alias("country"),
                col("seller_postal_code").alias("postal_code"),
            )
            .dropDuplicates()
            .withColumn("seller_id", monotonically_increasing_id() + 1)
        )


class DimProduct(DimensionCommand):
    name = "dim_product"

    def execute(self, df, dims=None):
        return (
            df.select(
                col("product_name").alias("name"),
                col("product_category").alias("category"),
                col("product_price").alias("price"),
                col("product_quantity").alias("quantity"),
                col("product_weight").alias("weight"),
                col("product_color").alias("color"),
                col("product_size").alias("size"),
                col("product_brand").alias("brand"),
                col("product_material").alias("material"),
                col("product_description").alias("description"),
                col("product_rating").alias("rating"),
                col("product_reviews").alias("reviews"),
                col("product_release_date").alias("release_date"),
                col("product_expiry_date").alias("expiry_date"),
            )
            .dropDuplicates(["name", "category"])
            .withColumn("product_id", monotonically_increasing_id() + 1)
        )


class DimStore(DimensionCommand):
    name = "dim_store"

    def execute(self, df, dims=None):
        return (
            df.select(
                col("store_name").alias("name"),
                col("store_location").alias("location"),
                col("store_city").alias("city"),
                col("store_state").alias("state"),
                col("store_country").alias("country"),
                col("store_phone").alias("phone"),
                col("store_email").alias("email"),
            )
            .dropDuplicates(["name", "email"])
            .withColumn("store_id", monotonically_increasing_id() + 1)
        )


class DimSupplier(DimensionCommand):
    name = "dim_supplier"

    def execute(self, df, dims=None):
        return (
            df.select(
                col("supplier_name").alias("name"),
                col("supplier_contact").alias("contact"),
                col("supplier_email").alias("email"),
                col("supplier_phone").alias("phone"),
                col("supplier_address").alias("address"),
                col("supplier_city").alias("city"),
                col("supplier_country").alias("country"),
            )
            .dropDuplicates(["name", "email"])
            .withColumn("supplier_id", monotonically_increasing_id() + 1)
        )


class DimPetCategory(DimensionCommand):
    name = "dim_pet_category"

    def execute(self, df, dims=None):
        return (
            df.select(col("pet_category").alias("category"))
            .filter(col("category").isNotNull())
            .dropDuplicates()
            .withColumn("pet_category_id", monotonically_increasing_id() + 1)
        )


class DimDate(DimensionCommand):
    name = "dim_date"

    def execute(self, df, dims=None):
        return (
            df.select(to_date(col("sale_date"), "M/d/yyyy").alias("sale_date"))
            .filter(col("sale_date").isNotNull())
            .dropDuplicates()
            .withColumn("day", dayofmonth(col("sale_date")))
            .withColumn("month", month(col("sale_date")))
            .withColumn("year", year(col("sale_date")))
            .withColumn("date_id", monotonically_increasing_id() + 1)
        )


class FactSales(FactCommand):
    name = "fact_sales"

    def execute(self, df, dims=None):
        d = dims
        mock = df.withColumn("sale_date_parsed", to_date(col("sale_date"), "M/d/yyyy"))

        return (
            mock
            .join(d["dim_customer"],
                  (col("customer_first_name") == d["dim_customer"]["first_name"])
                  & (col("customer_last_name") == d["dim_customer"]["last_name"])
                  & (col("customer_email") == d["dim_customer"]["email"]))
            .join(d["dim_seller"],
                  (col("seller_first_name") == d["dim_seller"]["first_name"])
                  & (col("seller_last_name") == d["dim_seller"]["last_name"])
                  & (col("seller_email") == d["dim_seller"]["email"]))
            .join(d["dim_product"],
                  (col("product_name") == d["dim_product"]["name"])
                  & (col("product_category") == d["dim_product"]["category"]))
            .join(d["dim_store"],
                  (col("store_name") == d["dim_store"]["name"])
                  & (col("store_email") == d["dim_store"]["email"]))
            .join(d["dim_supplier"],
                  (col("supplier_name") == d["dim_supplier"]["name"])
                  & (col("supplier_email") == d["dim_supplier"]["email"]))
            .join(d["dim_pet_category"],
                  col("pet_category") == d["dim_pet_category"]["category"])
            .join(d["dim_date"],
                  col("sale_date_parsed") == d["dim_date"]["sale_date"])
            .select(
                d["dim_customer"]["customer_id"],
                d["dim_seller"]["seller_id"],
                d["dim_product"]["product_id"],
                d["dim_store"]["store_id"],
                d["dim_supplier"]["supplier_id"],
                d["dim_pet_category"]["pet_category_id"],
                d["dim_date"]["date_id"],
                col("sale_quantity"),
                col("sale_total_price"),
            )
            .withColumn("sale_id", monotonically_increasing_id() + 1)
        )


def main():
    cfg = load_config()

    spark = (
        SparkSession.builder
        .master(cfg["spark"]["master"])
        .appName(cfg["spark"]["app_name"])
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    registry = (
        TableRegistry()
        .register(DimCustomer())
        .register(DimSeller())
        .register(DimProduct())
        .register(DimStore())
        .register(DimSupplier())
        .register(DimPetCategory())
        .register(DimDate())
        .register(FactSales())
    )

    source = PostgresStrategy(cfg["postgres_ods"])
    target = PostgresStrategy(cfg["postgres_dds"])

    pipeline = ETLPipeline(
        spark=spark,
        registry=registry,
        source=source,
        target=target,
        source_table=cfg["etl"]["source_table"],
    )

    pipeline.run()
    print("ETL completed.")
    spark.stop()


if __name__ == "__main__":
    main()
