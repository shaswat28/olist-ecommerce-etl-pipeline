CREATE DATABASE IF NOT EXISTS olist_db;
USE olist_db;

-- dimension table (descriptive data)
CREATE TABLE dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name_english VARCHAR(100)
);

-- fact table (quantitative data)
CREATE TABLE fact_sales (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    sale_amount DECIMAL(10, 2),
    order_purchase_timestamp DATETIME,
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);