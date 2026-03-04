-- Olist E-commerce Data Warehouse - PostgreSQL Implementation
-- Complete schema with staging, dimensions, and facts
-- First connect to any database other than the one you want to drop
\c postgres;

-- Drop the database if it already exists
DROP DATABASE IF EXISTS olist_datawarehouse;

-- Create the database again
CREATE DATABASE olist_datawarehouse;

-- Connect to the newly created database
\c olist_datawarehouse;


-- Create schemas for different layers (IF NOT EXISTS handles existing schemas)
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dimensions;
CREATE SCHEMA IF NOT EXISTS facts;
CREATE SCHEMA IF NOT EXISTS marts;

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- STAGING AREA TABLES
-- ============================================================================

-- Staging table for customers
DROP TABLE IF EXISTS staging.stg_customers CASCADE;
CREATE TABLE staging.stg_customers (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(10),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64),
    source_file VARCHAR(100)
);

-- Staging table for products
DROP TABLE IF EXISTS staging.stg_products CASCADE;
CREATE TABLE staging.stg_products (
    product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g DECIMAL(10,2),
    product_length_cm DECIMAL(10,2),
    product_height_cm DECIMAL(10,2),
    product_width_cm DECIMAL(10,2),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64),
    source_file VARCHAR(100)
);

-- Staging table for sellers
DROP TABLE IF EXISTS staging.stg_sellers CASCADE;
CREATE TABLE staging.stg_sellers (
    seller_id VARCHAR(50),
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(10),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64),
    source_file VARCHAR(100)
);

-- Staging table for orders
DROP TABLE IF EXISTS staging.stg_orders CASCADE;
CREATE TABLE staging.stg_orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64),
    source_file VARCHAR(100)
);

-- Staging table for order items (with special attention to order_item_id)
DROP TABLE IF EXISTS staging.stg_order_items CASCADE;
CREATE TABLE staging.stg_order_items (
    order_id VARCHAR(50),
    order_item_id INTEGER, -- Sequential number identifying items in same order
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64),
    source_file VARCHAR(100)
);

-- Staging table for payments
DROP TABLE IF EXISTS staging.stg_payments CASCADE;
CREATE TABLE staging.stg_payments (
    order_id VARCHAR(50),
    payment_sequential INTEGER,
    payment_type VARCHAR(50),
    payment_installments INTEGER,
    payment_value DECIMAL(10,2),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64),
    source_file VARCHAR(100)
);

-- Staging table for reviews
DROP TABLE IF EXISTS staging.stg_reviews CASCADE;
CREATE TABLE staging.stg_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64),
    source_file VARCHAR(100)
);

-- Staging table for product category translation
DROP TABLE IF EXISTS staging.stg_product_categories CASCADE;
CREATE TABLE staging.stg_product_categories (
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64),
    source_file VARCHAR(100)
);

-- ============================================================================
-- DIMENSION TABLES (Fixed for proper ERD visualization)
-- ============================================================================

-- Date Dimension (Type 0 - Static)
DROP TABLE IF EXISTS dimensions.dim_date CASCADE;
CREATE TABLE dimensions.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    day INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
    day_of_week INTEGER NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    week_of_year INTEGER CHECK (week_of_year BETWEEN 1 AND 53),
    month_name VARCHAR(20) NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL DEFAULT FALSE,
    is_holiday BOOLEAN NOT NULL DEFAULT FALSE,
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER NOT NULL CHECK (fiscal_quarter BETWEEN 1 AND 4),
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Customer Dimension (Type 1 SCD - Overwrite)
DROP TABLE IF EXISTS dimensions.dim_customer CASCADE;
CREATE TABLE dimensions.dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100) NOT NULL,
    customer_state VARCHAR(10) NOT NULL,
    customer_region VARCHAR(20) NOT NULL,
    
    -- SCD Type 1 metadata
    record_hash VARCHAR(64) NOT NULL,
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Constraints (removed zip_prefix check - data has variable length codes)
    CONSTRAINT chk_customer_state_length CHECK (LENGTH(customer_state) = 2)
);

-- Product Dimension (Type 2 SCD - Track History)
DROP TABLE IF EXISTS dimensions.dim_product CASCADE;
CREATE TABLE dimensions.dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_category_name VARCHAR(100),
    product_category_english VARCHAR(100),
    product_name_lenght INTEGER CHECK (product_name_lenght >= 0),
    product_description_lenght INTEGER CHECK (product_description_lenght >= 0),
    product_photos_qty INTEGER CHECK (product_photos_qty >= 0),
    product_weight_g DECIMAL(10,2) CHECK (product_weight_g >= 0),
    product_length_cm DECIMAL(10,2) CHECK (product_length_cm >= 0),
    product_height_cm DECIMAL(10,2) CHECK (product_height_cm >= 0),
    product_width_cm DECIMAL(10,2) CHECK (product_width_cm >= 0),
    
    -- Calculated fields
    product_volume_cm3 DECIMAL(15,2) CHECK (product_volume_cm3 >= 0),
    product_size_category VARCHAR(20) CHECK (product_size_category IN ('Small', 'Medium', 'Large', 'Extra Large', 'Unknown')),
    
    -- SCD Type 2 metadata
    effective_start_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31'::TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    record_hash VARCHAR(64) NOT NULL,
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- SCD Type 2 constraints
    CONSTRAINT chk_product_effective_dates CHECK (effective_start_date <= effective_end_date),
    CONSTRAINT uq_product_current UNIQUE (product_id, is_current) DEFERRABLE INITIALLY DEFERRED
);

-- Seller Dimension (Type 1 SCD)
DROP TABLE IF EXISTS dimensions.dim_seller CASCADE;
CREATE TABLE dimensions.dim_seller (
    seller_sk SERIAL PRIMARY KEY,
    seller_id VARCHAR(50) NOT NULL UNIQUE,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100) NOT NULL,
    seller_state VARCHAR(10) NOT NULL,
    seller_region VARCHAR(20) NOT NULL,
    
    -- SCD Type 1 metadata
    record_hash VARCHAR(64) NOT NULL,
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Constraints (removed zip_prefix check - data has variable length codes)
    CONSTRAINT chk_seller_state_length CHECK (LENGTH(seller_state) = 2)
);

-- Order Status Dimension (Type 1 SCD)
DROP TABLE IF EXISTS dimensions.dim_order_status CASCADE;
CREATE TABLE dimensions.dim_order_status (
    order_status_sk SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    order_status VARCHAR(50) NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    
    -- Calculated delivery metrics
    approval_delay_hours INTEGER CHECK (approval_delay_hours >= 0),
    delivery_delay_days INTEGER,
    total_delivery_days INTEGER CHECK (total_delivery_days >= 0),
    is_delivered_on_time BOOLEAN,
    delivery_performance_category VARCHAR(20) CHECK (delivery_performance_category IN ('Early', 'On Time', 'Slightly Late', 'Very Late', 'Unknown')),
    
    -- SCD Type 1 metadata
    record_hash VARCHAR(64) NOT NULL,
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Business logic constraints
    CONSTRAINT chk_order_timestamps CHECK (
        order_purchase_timestamp <= COALESCE(order_approved_at, order_purchase_timestamp + INTERVAL '1 year')
        AND order_purchase_timestamp <= COALESCE(order_estimated_delivery_date, order_purchase_timestamp + INTERVAL '1 year')
    ),
    CONSTRAINT chk_order_status_values CHECK (order_status IN ('delivered', 'shipped', 'processing', 'approved', 'created', 'invoiced', 'canceled', 'unavailable'))
);

-- ============================================================================
-- FACT TABLES (WITHOUT foreign key constraints initially)
-- ============================================================================

-- Order Items Fact (Main transactional fact)
DROP TABLE IF EXISTS facts.fact_order_items CASCADE;
CREATE TABLE facts.fact_order_items (
    order_item_sk BIGSERIAL PRIMARY KEY,
    
    -- Natural keys
    order_id VARCHAR(50) NOT NULL,
    order_item_id INTEGER NOT NULL CHECK (order_item_id > 0),
    
    -- Foreign keys to dimensions (NO CONSTRAINTS YET - will be added after ETL)
    customer_sk INTEGER NOT NULL,
    product_sk INTEGER NOT NULL,
    seller_sk INTEGER NOT NULL,
    order_status_sk INTEGER NOT NULL,
    order_date_key INTEGER,
    shipping_date_key INTEGER,
    
    -- Measures (additive facts)
    item_price DECIMAL(10,2) NOT NULL CHECK (item_price >= 0),
    freight_value DECIMAL(10,2) NOT NULL CHECK (freight_value >= 0),
    total_item_value DECIMAL(10,2) NOT NULL CHECK (total_item_value >= 0),
    
    -- Degenerate dimensions
    shipping_limit_date TIMESTAMP,
    
    -- Audit fields
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite unique constraint
    CONSTRAINT uq_order_item UNIQUE(order_id, order_item_id)
);

-- Payment Fact (Transaction fact)
DROP TABLE IF EXISTS facts.fact_payments CASCADE;
CREATE TABLE facts.fact_payments (
    payment_sk BIGSERIAL PRIMARY KEY,
    
    -- Natural keys
    order_id VARCHAR(50) NOT NULL,
    payment_sequential INTEGER NOT NULL CHECK (payment_sequential > 0),
    
    -- Foreign keys (NO CONSTRAINTS YET)
    order_date_key INTEGER,
    order_status_sk INTEGER NOT NULL,
    
    -- Measures
    payment_installments INTEGER NOT NULL CHECK (payment_installments >= 0),  -- Changed to >= 0 to allow 0 installments
    payment_value DECIMAL(10,2) NOT NULL CHECK (payment_value >= 0),
    
    -- Degenerate dimensions
    payment_type VARCHAR(50) NOT NULL CHECK (payment_type IN ('credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined')),
    
    -- Audit fields
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Business constraints
    CONSTRAINT uq_payment UNIQUE(order_id, payment_sequential)
);

-- Review Fact (Event fact)
DROP TABLE IF EXISTS facts.fact_reviews CASCADE;
CREATE TABLE facts.fact_reviews (
    review_sk BIGSERIAL PRIMARY KEY,
    
    -- Natural keys
    review_id VARCHAR(50) NOT NULL UNIQUE,
    order_id VARCHAR(50) NOT NULL,
    
    -- Foreign keys (NO CONSTRAINTS YET)
    customer_sk INTEGER NOT NULL,
    order_status_sk INTEGER NOT NULL,
    review_date_key INTEGER,
    answer_date_key INTEGER,
    
    -- Measures
    review_score INTEGER NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    
    -- Degenerate dimensions
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    
    -- Calculated fields
    review_category VARCHAR(20) NOT NULL CHECK (review_category IN ('Poor', 'Fair', 'Good', 'Excellent', 'Unknown')),
    response_time_hours INTEGER CHECK (response_time_hours >= 0),
    
    -- Audit fields
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- AGGREGATE TABLES (Data Marts) - WITH PROPER CONSTRAINTS
-- ============================================================================

-- Monthly Sales Aggregate
DROP TABLE IF EXISTS marts.agg_monthly_sales CASCADE;
CREATE TABLE marts.agg_monthly_sales (
    sales_agg_sk SERIAL PRIMARY KEY,
    year_month INTEGER NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name VARCHAR(20) NOT NULL,
    
    -- Aggregated measures
    total_orders INTEGER NOT NULL CHECK (total_orders >= 0),
    total_items INTEGER NOT NULL CHECK (total_items >= 0),
    total_revenue DECIMAL(12,2) NOT NULL CHECK (total_revenue >= 0),
    total_freight DECIMAL(12,2) NOT NULL CHECK (total_freight >= 0),
    avg_order_value DECIMAL(10,2) CHECK (avg_order_value >= 0),
    avg_items_per_order DECIMAL(5,2) CHECK (avg_items_per_order >= 0),
    
    -- Customer metrics
    unique_customers INTEGER NOT NULL CHECK (unique_customers >= 0),
    new_customers INTEGER CHECK (new_customers >= 0),
    
    -- Product metrics
    unique_products INTEGER NOT NULL CHECK (unique_products >= 0),
    
    -- Geographic metrics
    unique_states INTEGER CHECK (unique_states >= 0),
    
    -- Last updated
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Product Performance Aggregate
DROP TABLE IF EXISTS marts.agg_product_performance CASCADE;
CREATE TABLE marts.agg_product_performance (
    product_perf_sk SERIAL PRIMARY KEY,
    product_sk INTEGER NOT NULL,
    
    -- Time period
    last_12_months_revenue DECIMAL(12,2) CHECK (last_12_months_revenue >= 0),
    last_12_months_orders INTEGER CHECK (last_12_months_orders >= 0),
    last_12_months_items INTEGER CHECK (last_12_months_items >= 0),
    
    -- Performance metrics
    avg_price DECIMAL(10,2) CHECK (avg_price >= 0),
    avg_review_score DECIMAL(3,2) CHECK (avg_review_score BETWEEN 1.0 AND 5.0),
    total_reviews INTEGER CHECK (total_reviews >= 0),
    
    -- Rankings
    revenue_rank INTEGER CHECK (revenue_rank > 0),
    volume_rank INTEGER CHECK (volume_rank > 0),
    
    -- Last updated
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraint
    CONSTRAINT fk_product_perf_product FOREIGN KEY (product_sk) 
        REFERENCES dimensions.dim_product(product_sk) 
        ON DELETE CASCADE ON UPDATE CASCADE,
    
    -- Unique constraint
    CONSTRAINT uq_product_performance UNIQUE (product_sk)
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Staging table indexes
CREATE INDEX idx_stg_customers_load_ts ON staging.stg_customers(load_timestamp);
CREATE INDEX idx_stg_products_load_ts ON staging.stg_products(load_timestamp);
CREATE INDEX idx_stg_orders_load_ts ON staging.stg_orders(load_timestamp);
CREATE INDEX idx_stg_order_items_load_ts ON staging.stg_order_items(load_timestamp);

-- Dimension indexes
CREATE INDEX idx_dim_customer_id ON dimensions.dim_customer(customer_id);
CREATE INDEX idx_dim_product_id ON dimensions.dim_product(product_id);
CREATE INDEX idx_dim_product_current ON dimensions.dim_product(product_id, is_current);
CREATE INDEX idx_dim_seller_id ON dimensions.dim_seller(seller_id);
CREATE INDEX idx_dim_date_full_date ON dimensions.dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dimensions.dim_date(year, month);

-- Fact table indexes
CREATE INDEX idx_fact_order_items_order_id ON facts.fact_order_items(order_id);
CREATE INDEX idx_fact_order_items_customer_sk ON facts.fact_order_items(customer_sk);
CREATE INDEX idx_fact_order_items_product_sk ON facts.fact_order_items(product_sk);
CREATE INDEX idx_fact_order_items_order_date ON facts.fact_order_items(order_date_key);
CREATE INDEX idx_fact_payments_order_id ON facts.fact_payments(order_id);
CREATE INDEX idx_fact_reviews_order_id ON facts.fact_reviews(order_id);

-- ============================================================================
-- HELPER FUNCTIONS AND PROCEDURES
-- ============================================================================

-- Function to calculate Brazilian regions
CREATE OR REPLACE FUNCTION get_brazilian_region(state VARCHAR(2))
RETURNS VARCHAR(20) AS $$
BEGIN
    RETURN CASE 
        WHEN state IN ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
        WHEN state IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
        WHEN state IN ('DF', 'GO', 'MS', 'MT') THEN 'Centro-Oeste'
        WHEN state IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
        WHEN state IN ('PR', 'RS', 'SC') THEN 'Sul'
        ELSE 'Unknown'
    END;
END;
$$ LANGUAGE plpgsql;

-- Function to categorize product sizes
CREATE OR REPLACE FUNCTION get_product_size_category(
    length_cm DECIMAL, height_cm DECIMAL, width_cm DECIMAL
) RETURNS VARCHAR(20) AS $$
DECLARE
    volume DECIMAL;
BEGIN
    IF length_cm IS NULL OR height_cm IS NULL OR width_cm IS NULL THEN
        RETURN 'Unknown';
    END IF;
    
    volume := length_cm * height_cm * width_cm;
    
    RETURN CASE 
        WHEN volume <= 1000 THEN 'Small'
        WHEN volume <= 10000 THEN 'Medium'
        WHEN volume <= 50000 THEN 'Large'
        ELSE 'Extra Large'
    END;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- DATA QUALITY CONSTRAINTS
-- ============================================================================

-- Add constraints for data quality
ALTER TABLE facts.fact_order_items 
ADD CONSTRAINT chk_positive_price CHECK (item_price >= 0),
ADD CONSTRAINT chk_positive_freight CHECK (freight_value >= 0),
ADD CONSTRAINT chk_valid_order_item_id CHECK (order_item_id > 0);

ALTER TABLE facts.fact_payments
ADD CONSTRAINT chk_positive_payment CHECK (payment_value >= 0),
ADD CONSTRAINT chk_valid_installments CHECK (payment_installments >= 0);  -- Changed to >= 0

ALTER TABLE facts.fact_reviews
ADD CONSTRAINT chk_valid_score CHECK (review_score BETWEEN 1 AND 5);

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON SCHEMA staging IS 'Staging area for raw data ingestion';
COMMENT ON SCHEMA dimensions IS 'Dimension tables for the star schema';
COMMENT ON SCHEMA facts IS 'Fact tables containing business metrics';
COMMENT ON SCHEMA marts IS 'Pre-aggregated data marts for reporting';

COMMENT ON TABLE facts.fact_order_items IS 'Main fact table - one record per item in an order. order_item_id represents sequential numbering (1,2,3...) of items within the same order_id';
COMMENT ON COLUMN facts.fact_order_items.order_item_id IS 'Sequential number identifying the position of items included in the same order (starts from 1)';

-- PostgreSQL uses SELECT instead of PRINT for output
SELECT 'PostgreSQL Data Warehouse schema created successfully! Run ETL first, then execute add_foreign_keys.sql' as status;

-- ============================================================================
-- FOREIGN KEY CONSTRAINTS (Added after all tables are created)
-- ============================================================================

-- Add foreign key constraints to fact_order_items
ALTER TABLE facts.fact_order_items
    ADD CONSTRAINT fk_order_items_customer 
    FOREIGN KEY (customer_sk) REFERENCES dimensions.dim_customer(customer_sk)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE facts.fact_order_items
    ADD CONSTRAINT fk_order_items_product 
    FOREIGN KEY (product_sk) REFERENCES dimensions.dim_product(product_sk)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE facts.fact_order_items
    ADD CONSTRAINT fk_order_items_seller 
    FOREIGN KEY (seller_sk) REFERENCES dimensions.dim_seller(seller_sk)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE facts.fact_order_items
    ADD CONSTRAINT fk_order_items_order_status 
    FOREIGN KEY (order_status_sk) REFERENCES dimensions.dim_order_status(order_status_sk)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE facts.fact_order_items
    ADD CONSTRAINT fk_order_items_order_date 
    FOREIGN KEY (order_date_key) REFERENCES dimensions.dim_date(date_key)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE facts.fact_order_items
    ADD CONSTRAINT fk_order_items_shipping_date 
    FOREIGN KEY (shipping_date_key) REFERENCES dimensions.dim_date(date_key)
    ON DELETE RESTRICT ON UPDATE CASCADE;

-- Add foreign key constraints to fact_payments
ALTER TABLE facts.fact_payments
    ADD CONSTRAINT fk_payments_order_date 
    FOREIGN KEY (order_date_key) REFERENCES dimensions.dim_date(date_key)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE facts.fact_payments
    ADD CONSTRAINT fk_payments_order_status 
    FOREIGN KEY (order_status_sk) REFERENCES dimensions.dim_order_status(order_status_sk)
    ON DELETE RESTRICT ON UPDATE CASCADE;

-- Add foreign key constraints to fact_reviews
ALTER TABLE facts.fact_reviews
    ADD CONSTRAINT fk_reviews_customer 
    FOREIGN KEY (customer_sk) REFERENCES dimensions.dim_customer(customer_sk)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE facts.fact_reviews
    ADD CONSTRAINT fk_reviews_order_status 
    FOREIGN KEY (order_status_sk) REFERENCES dimensions.dim_order_status(order_status_sk)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE facts.fact_reviews
    ADD CONSTRAINT fk_reviews_review_date 
    FOREIGN KEY (review_date_key) REFERENCES dimensions.dim_date(date_key)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE facts.fact_reviews
    ADD CONSTRAINT fk_reviews_answer_date 
    FOREIGN KEY (answer_date_key) REFERENCES dimensions.dim_date(date_key)
    ON DELETE RESTRICT ON UPDATE CASCADE;

-- ============================================================================
-- VERIFY FOREIGN KEY CONSTRAINTS
-- ============================================================================

-- Query to display all foreign key relationships
SELECT 
    tc.table_schema || '.' || tc.table_name AS table_full_name,
    tc.constraint_name,
    kcu.column_name,
    ccu.table_schema || '.' || ccu.table_name AS references_table,
    ccu.column_name AS references_column
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema = 'facts'
ORDER BY tc.table_name, tc.constraint_name;

-- PostgreSQL uses SELECT instead of PRINT for output
SELECT '✅ PostgreSQL Data Warehouse schema created successfully with foreign key constraints!' as status;
SELECT '📊 All relationships are now defined and will be visible in pgAdmin ERD.' as info;
SELECT '🚀 Next step: Run ETL pipeline to populate the data warehouse.' as next_step;
