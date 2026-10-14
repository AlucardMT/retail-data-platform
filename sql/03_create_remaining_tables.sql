
-- ==========================================
-- Sellers
-- ==========================================

CREATE TABLE IF NOT EXISTS retail.sellers (
    seller_id VARCHAR(32) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(5) NOT NULL,
    seller_city TEXT NOT NULL,
    seller_state VARCHAR(2) NOT NULL
);


-- ==========================================
-- Order Items
-- ==========================================

CREATE TABLE IF NOT EXISTS retail.order_items (
    order_id VARCHAR(32) NOT NULL
        REFERENCES retail.orders(order_id),

    order_item_id INTEGER NOT NULL,

    product_id VARCHAR(32) NOT NULL
        REFERENCES retail.products(product_id),

    seller_id VARCHAR(32) NOT NULL
        REFERENCES retail.sellers(seller_id),

    shipping_limit_date TIMESTAMP NOT NULL,

    price NUMERIC(12,2) NOT NULL,

    freight_value NUMERIC(12,2) NOT NULL,

    PRIMARY KEY (order_id, order_item_id)
);


-- ==========================================
-- Order Payments
-- ==========================================

CREATE TABLE IF NOT EXISTS retail.order_payments (
    order_id VARCHAR(32) NOT NULL
        REFERENCES retail.orders(order_id),

    payment_sequential INTEGER NOT NULL,

    payment_type TEXT NOT NULL,

    payment_installments INTEGER NOT NULL,

    payment_value NUMERIC(12,2) NOT NULL,

    PRIMARY KEY (order_id, payment_sequential)
);


-- ==========================================
-- Order Reviews
-- ==========================================

CREATE TABLE IF NOT EXISTS retail.order_reviews (
    review_id VARCHAR(32) NOT NULL,

    order_id VARCHAR(32) NOT NULL
        REFERENCES retail.orders(order_id),

    review_score INTEGER NOT NULL,

    review_comment_title TEXT,

    review_comment_message TEXT,

    review_creation_date TIMESTAMP NOT NULL,

    review_answer_timestamp TIMESTAMP NOT NULL,

    PRIMARY KEY (review_id, order_id)
);


-- ==========================================
-- Product Category Translations
-- ==========================================

CREATE TABLE IF NOT EXISTS retail.category_translations (
    product_category_name TEXT PRIMARY KEY,

    product_category_name_english TEXT NOT NULL
);


-- ==========================================
-- Additional indexes
-- ==========================================

CREATE INDEX IF NOT EXISTS idx_order_items_product_id
    ON retail.order_items(product_id);

CREATE INDEX IF NOT EXISTS idx_order_items_seller_id
    ON retail.order_items(seller_id);

CREATE INDEX IF NOT EXISTS idx_order_payments_order_id
    ON retail.order_payments(order_id);

CREATE INDEX IF NOT EXISTS idx_order_reviews_order_id
    ON retail.order_reviews(order_id);

