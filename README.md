# 🏬 Olist E-commerce Data Warehouse

A complete data warehouse solution for Olist Brazilian e-commerce dataset with PostgreSQL backend, comprehensive ETL pipeline, and business intelligence capabilities.

## 🎯 Project Overview

This project implements a star schema data warehouse for analyzing e-commerce transactions, customer behavior, product performance, and business metrics from the Olist dataset.

### Key Features

- **Complete ETL Pipeline** with staging, dimensions, and facts
- **Star Schema Design** optimized for analytical queries with foreign key constraints
- **Data Quality Validation** with order_item_id sequential numbering
- **Business Intelligence Views** for common analytics
- **Comprehensive EDA** with data profiling and visualization

## 🚀 Quick Start Guide

### Prerequisites

- PostgreSQL 13+ installed and running
- Python 3.8+
- Olist dataset CSV files in `data/` folder

### Step 1: Install Python Dependencies

```bash
pip install pandas numpy psycopg2-binary python-dotenv matplotlib seaborn jupyter
```

### Step 2: Configure Database Connection

Create `src/.env` file with your PostgreSQL credentials:

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=olist_datawarehouse
DB_USER=postgres
DB_PASSWORD=your_password
```

### Step 3: Create Data Warehouse Schema

Run the schema creation script to set up all tables, constraints, and foreign keys:

```bash
# Create database and schema with foreign key constraints
psql -U postgres -d postgres -f sql/create_datawarehouse_postgresql.sql
```

This script will:

- ✅ Create database `olist_datawarehouse`
- ✅ Create schemas: `staging`, `dimensions`, `facts`, `marts`
- ✅ Create all dimension tables with SERIAL primary keys
- ✅ Create all fact tables with structure
- ✅ Add all foreign key constraints for referential integrity
- ✅ Create indexes for query performance
- ✅ Create helper functions for business logic

### Step 4: Run ETL Pipeline

Execute the ETL pipeline to populate the data warehouse:

```bash
# Load data from CSV files into the data warehouse
python src/etl_pipeline_postgresql.py
```

The ETL pipeline will:

1. **Extract**: Load CSV files into staging tables
2. **Transform**: Process and clean data for dimensions and facts
3. **Load**: Populate dimension and fact tables with resolved foreign keys
4. **Validate**: Ensure referential integrity is maintained

### Step 5: Verify Data Warehouse

```bash
# Check data warehouse status
psql -U postgres -d olist_datawarehouse -c "
SELECT
    'Dimensions' as layer,
    COUNT(*) as table_count
FROM information_schema.tables
WHERE table_schema = 'dimensions'
UNION ALL
SELECT
    'Facts' as layer,
    COUNT(*) as table_count
FROM information_schema.tables
WHERE table_schema = 'facts';
"

# Verify foreign key relationships
psql -U postgres -d olist_datawarehouse -c "
SELECT
    tc.table_name,
    tc.constraint_name,
    kcu.column_name,
    ccu.table_name AS references_table
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema = 'facts'
ORDER BY tc.table_name;
"
```

### Step 6: Generate ERD in pgAdmin

1. Open pgAdmin
2. Navigate to `olist_datawarehouse` database
3. Right-click → **Generate ERD**
4. All tables and relationships will be visualized

## 📁 Project Structures

```
CO4031/
├── 📊 data/                          # Raw CSV files from Olist dataset
│   ├── olist_customers_dataset.csv
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   └── product_category_name_translation.csv
├── 📓 notebooks/
│   └── EDA.ipynb                     # Exploratory Data Analysis
├── 🔧 src/
│   ├── .env                          # Database configuration (YOU CREATE THIS)
│   └── etl_pipeline_postgresql.py    # Main ETL pipeline
├── 🗄️ sql/
│   ├── create_datawarehouse_postgresql.sql  # Schema with FK constraints
│   └── add_foreign_keys.sql          # (DEPRECATED - FKs now in schema)
├── 📝 logs/                          # ETL execution logs
└── 📖 README.md
```

## 🗃️ Database Schema

### Data Warehouse Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    STAGING LAYER                            │
│  Raw data from CSV files with ETL metadata                 │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  DIMENSION LAYER                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ dim_customer │  │ dim_product  │  │ dim_seller   │     │
│  │  (Type 1)    │  │  (Type 2)    │  │  (Type 1)    │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│  ┌──────────────┐  ┌──────────────────────────────────┐   │
│  │  dim_date    │  │  dim_order_status               │   │
│  │  (Type 0)    │  │  (Type 1)                        │   │
│  └──────────────┘  └──────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                     FACT LAYER                              │
│  ┌────────────────────┐  ┌────────────────────┐           │
│  │ fact_order_items   │  │ fact_payments      │           │
│  │ (Transactional)    │  │ (Transactional)    │           │
│  └────────────────────┘  └────────────────────┘           │
│  ┌────────────────────┐                                    │
│  │ fact_reviews       │                                    │
│  │ (Event)            │                                    │
│  └────────────────────┘                                    │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   DATA MARTS LAYER                          │
│  Pre-aggregated business metrics for BI tools              │
└─────────────────────────────────────────────────────────────┘
```

### Dimension Tables (with SERIAL surrogate keys)

- **dim_customer** (Type 1 SCD) - Customer demographics and location
- **dim_product** (Type 2 SCD) - Product catalog with category translations
- **dim_seller** (Type 1 SCD) - Seller information and location
- **dim_date** (Type 0) - Date dimension for time-based analysis
- **dim_order_status** (Type 1 SCD) - Order lifecycle and delivery metrics

### Fact Tables (with foreign key constraints)

- **fact_order_items** - Order line items (one record per item)
- **fact_payments** - Payment transactions
- **fact_reviews** - Customer reviews and ratings

### Foreign Key Relationships

All fact tables have enforced foreign key constraints:

- `customer_sk` → `dim_customer(customer_sk)`
- `product_sk` → `dim_product(product_sk)`
- `seller_sk` → `dim_seller(seller_sk)`
- `order_status_sk` → `dim_order_status(order_status_sk)`
- `order_date_key` → `dim_date(date_key)`
- `shipping_date_key` → `dim_date(date_key)`

## 🔍 Key Business Rules Validated

1. **Order Item Sequencing**: `order_item_id` represents sequential numbering (1,2,3...) within each order
2. **Referential Integrity**: All foreign key relationships enforced at database level
3. **Data Quality**: Missing timestamps filled with business-logic defaults
4. **Surrogate Keys**: Auto-generated SERIAL keys for all dimensions

## 📊 Sample Analytics Queries

```sql
-- Monthly Revenue Trends
SELECT
    year_month,
    total_revenue,
    total_orders,
    avg_order_value
FROM marts.agg_monthly_sales
ORDER BY year_month DESC;

-- Top Product Categories by Revenue
SELECT
    p.product_category_english,
    COUNT(DISTINCT f.order_id) as total_orders,
    SUM(f.total_item_value) as revenue,
    AVG(f.total_item_value) as avg_order_value
FROM facts.fact_order_items f
JOIN dimensions.dim_product p ON f.product_sk = p.product_sk
WHERE p.is_current = true
GROUP BY p.product_category_english
ORDER BY revenue DESC
LIMIT 10;

-- Geographic Sales Distribution
SELECT
    c.customer_state,
    c.customer_region,
    COUNT(DISTINCT f.order_id) as total_orders,
    SUM(f.total_item_value) as revenue,
    AVG(f.total_item_value) as avg_order_value
FROM facts.fact_order_items f
JOIN dimensions.dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY c.customer_state, c.customer_region
ORDER BY revenue DESC;

-- Delivery Performance Analysis
SELECT
    os.delivery_performance_category,
    COUNT(*) as order_count,
    AVG(os.total_delivery_days) as avg_delivery_days,
    AVG(os.delivery_delay_days) as avg_delay_days
FROM dimensions.dim_order_status os
GROUP BY os.delivery_performance_category
ORDER BY order_count DESC;

-- Customer Satisfaction by Product Category
SELECT
    p.product_category_english,
    COUNT(*) as review_count,
    AVG(r.review_score) as avg_rating,
    SUM(CASE WHEN r.review_score >= 4 THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 as satisfaction_rate
FROM facts.fact_reviews r
JOIN facts.fact_order_items oi ON r.order_id = oi.order_id
JOIN dimensions.dim_product p ON oi.product_sk = p.product_sk
WHERE p.is_current = true
GROUP BY p.product_category_english
HAVING COUNT(*) >= 10
ORDER BY avg_rating DESC;
```

## 📈 Expected Results

After successful ETL execution:

- **~99,441 customers** across Brazil
- **~32,951 products** in 73 categories
- **~3,095 sellers** distributed geographically
- **~112,650 order items** in fact table
- **~103,886 payment records**
- **~99,224 customer reviews**
- **~842 dates** in date dimension

## 🔧 Troubleshooting

### Issue: Database Connection Failed

```bash
# Check PostgreSQL status
pg_ctl status

# Start PostgreSQL
pg_ctl start

# Verify connection parameters
psql -U postgres -c "SELECT version();"
```

### Issue: Foreign Key Constraint Violations

```bash
# Check for orphaned records
psql -U postgres -d olist_datawarehouse -c "
SELECT
    'fact_order_items' as table_name,
    COUNT(*) as orphaned_records
FROM facts.fact_order_items foi
WHERE NOT EXISTS (
    SELECT 1 FROM dimensions.dim_customer WHERE customer_sk = foi.customer_sk
);
"

# Re-run ETL pipeline to fix data issues
python src/etl_pipeline_postgresql.py
```

### Issue: Missing Data Files

```bash
# Verify all required CSV files are present
ls -la data/*.csv

# Required files:
# - olist_customers_dataset.csv
# - olist_orders_dataset.csv
# - olist_order_items_dataset.csv
# - olist_order_payments_dataset.csv
# - olist_order_reviews_dataset.csv
# - olist_products_dataset.csv
# - olist_sellers_dataset.csv
# - product_category_name_translation.csv
```

### Issue: ETL Pipeline Errors

```bash
# Check ETL logs for detailed error messages
cat logs/etl_execution_*.log | grep ERROR

# Common fixes:
# 1. Ensure database schema is created first
# 2. Verify .env file has correct credentials
# 3. Check data file encoding (should be UTF-8)
# 4. Ensure sufficient disk space and memory
```

## 🎯 Business Intelligence Integration

### Connecting BI Tools

**Power BI**

```
Data Source: PostgreSQL
Server: localhost:5432
Database: olist_datawarehouse
Authentication: Database credentials from .env
```

**Tableau**

```
Connect to: PostgreSQL
Server: localhost
Port: 5432
Database: olist_datawarehouse
```

**Metabase**

```
Database type: PostgreSQL
Host: localhost
Port: 5432
Database name: olist_datawarehouse
```

### Pre-built Views for Dashboards

- Use `marts.agg_monthly_sales` for time-series analysis
- Use `marts.agg_product_performance` for product analytics
- Join fact and dimension tables for custom analysis

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -am 'Add enhancement'`)
4. Push to branch (`git push origin feature/improvement`)
5. Create Pull Request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- **Olist** for providing the comprehensive e-commerce dataset
- **PostgreSQL** for robust data warehouse capabilities
- **Python Data Science Stack** for ETL and analysis tools

---

**📞 Need Help?** Check the troubleshooting section or open an issue on GitHub.

**🎉 Ready to analyze?** Open pgAdmin and generate your ERD to see the complete star schema with all relationships!
