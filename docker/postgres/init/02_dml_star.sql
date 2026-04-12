INSERT INTO dim_customer (first_name, last_name, age, email, country, postal_code, pet_type, pet_name, pet_breed)
SELECT 
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM mock_data;

INSERT INTO dim_seller (first_name, last_name, email, country, postal_code)
SELECT 
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM mock_data;

INSERT INTO dim_product (name, category, price, quantity, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date)
SELECT 
    product_name,
    product_category,
    product_price,
    product_quantity,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date
FROM mock_data;

INSERT INTO dim_store (name, location, city, state, country, phone, email)
SELECT 
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM mock_data;

INSERT INTO dim_supplier (name, contact, email, phone, address, city, country)
SELECT 
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM mock_data;

INSERT INTO dim_pet_category (category)
SELECT  pet_category
FROM mock_data
WHERE pet_category IS NOT NULL;

INSERT INTO dim_date (sale_date, day, month, year)
SELECT 
    TO_DATE(sale_date, 'MM/DD/YYYY'),
    EXTRACT(DAY FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::INT,
    EXTRACT(MONTH FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::INT,
    EXTRACT(YEAR FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::INT
FROM mock_data
WHERE sale_date IS NOT NULL;

INSERT INTO fact_sales (customer_id, seller_id, product_id, store_id, supplier_id, pet_category_id, date_id, sale_quantity, sale_total_price)
SELECT
    dc.customer_id,
    ds.seller_id,
    dp.product_id,
    dst.store_id,
    dsup.supplier_id,
    dpc.pet_category_id,
    dd.date_id,
    m.sale_quantity,
    m.sale_total_price
FROM mock_data m
JOIN dim_customer dc
    ON  m.customer_first_name = dc.first_name
    AND m.customer_last_name = dc.last_name
    AND m.customer_email = dc.email
JOIN dim_seller ds
    ON  m.seller_first_name = ds.first_name
    AND m.seller_last_name  = ds.last_name
    AND m.seller_email = ds.email
JOIN dim_product dp
    ON  m.product_name = dp.name
    AND m.product_category = dp.category
JOIN dim_store dst
    ON  m.store_name  = dst.name
    AND m.store_email = dst.email
JOIN dim_supplier dsup
    ON  m.supplier_name  = dsup.name
    AND m.supplier_email = dsup.email
JOIN dim_pet_category dpc
    ON  m.pet_category = dpc.category
JOIN dim_date dd
    ON  TO_DATE(m.sale_date, 'MM/DD/YYYY') = dd.sale_date;
