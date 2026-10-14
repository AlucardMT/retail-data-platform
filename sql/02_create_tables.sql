-- Customers
CREATE TABLE IF NOT EXISTS retail.customers (
    customer_id VARCHAR(32) PRIMARY KEY,
    customer_unique_id VARCHAR(32) NOT NULL,
    customer_zip_code_prefix VARCHAR(5) NOT NULL,
    customer_city TEXT NOT NULL,
    customer_state VARCHAR(2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_customers_unique_id
    ON retail.customers(customer_unique_id);


-- Products
CREATE TABLE IF NOT EXISTS retail.products (
    product_id VARCHAR(32) PRIMARY KEY,
    product_category_name TEXT,
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);


-- Orders
CREATE TABLE IF NOT EXISTS retail.orders (
    order_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL
        REFERENCES retail.customers(customer_id),
    order_status TEXT NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON retail.orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_orders_purchase_timestamp
    ON retail.orders(order_purchase_timestamp);
