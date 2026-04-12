CREATE TABLE IF NOT EXISTS reports.mart_sales_by_product
(
    product_id    Int64,
    name          String,
    category      String,
    brand         String,
    rating        Float64,
    reviews       Int64,
    total_quantity Int64,
    total_revenue Float64,
    orders_count  Int64
) ENGINE = MergeTree()
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS reports.mart_sales_by_customer
(
    customer_id     Int64,
    first_name      String,
    last_name       String,
    country         String,
    orders_count    Int64,
    total_spent     Float64,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS reports.mart_sales_by_time
(
    year            Int32,
    month           Int32,
    orders_count    Int64,
    total_revenue   Float64,
    total_quantity  Int64,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS reports.mart_sales_by_store
(
    store_id        Int64,
    name            String,
    city            String,
    country         String,
    orders_count    Int64,
    total_revenue   Float64,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY store_id;

CREATE TABLE IF NOT EXISTS reports.mart_sales_by_supplier
(
    supplier_id      Int64,
    supplier_name    String,
    supplier_country String,
    total_revenue    Float64,
    avg_product_price Float64,
    orders_count     Int64
) ENGINE = MergeTree()
ORDER BY supplier_id;

CREATE TABLE IF NOT EXISTS reports.mart_product_quality
(
    product_id       Int64,
    name             String,
    category         String,
    rating           Float64,
    reviews          Int64,
    total_quantity   Int64,
    total_revenue    Float64,
    orders_count     Int64,
    unique_customers Int64
) ENGINE = MergeTree()
ORDER BY product_id;
