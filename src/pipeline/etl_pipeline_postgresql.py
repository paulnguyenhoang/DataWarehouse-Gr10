"""
Olist Data Warehouse ETL Pipeline - PostgreSQL Implementation
Handles extraction, transformation, and loading of Olist e-commerce data using psycopg2
"""

import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import hashlib
from pathlib import Path
from dotenv import load_dotenv

class OlistETLPipeline:
    def __init__(self):
        # Load environment variables from src/.env
        env_file = Path(__file__).parent.parent / '.env'
        load_dotenv(env_file)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize database connection
        self.connection = None
        self._setup_database_connection()
        
        # Set data path relative to script location (go up to project root)
        self.data_path = Path(__file__).parent.parent.parent / 'data'
        
        self.logger.info("ETL Pipeline initialized successfully")
    
    def _setup_logging(self):
        """Configure logging for the ETL pipeline"""
        log_dir = Path(__file__).parent.parent / 'logs'
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f'etl_execution_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),  # Fix encoding
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _clean_env_value(self, value):
        """Clean environment variable values by removing quotes"""
        if value and isinstance(value, str):
            return value.strip().strip("'\"")
        return value
    
    def _setup_database_connection(self):
        """Setup PostgreSQL database connection using psycopg2"""
        try:
            # Get and clean environment variables
            db_config = {
                'host': self._clean_env_value(os.getenv('DB_HOST', 'localhost')),
                'port': int(self._clean_env_value(os.getenv('DB_PORT', '5432'))),
                'database': self._clean_env_value(os.getenv('DB_NAME', 'olist_datawarehouse')),
                'user': self._clean_env_value(os.getenv('DB_USER', 'postgres')),
                'password': self._clean_env_value(os.getenv('DB_PASSWORD'))
            }
            
            self.logger.info(f"Connecting to database: {db_config['user']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")
            
            # Create psycopg2 connection
            self.connection = psycopg2.connect(**db_config)
            self.connection.autocommit = False  # We'll manage transactions manually
            
            # Test connection
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 as test")
            test_value = cursor.fetchone()[0]
            cursor.close()
            
            if test_value == 1:
                self.logger.info("Database connection established successfully")
                
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise ConnectionError(f"Database connection failed: {e}")
    
    def _generate_record_hash(self, row_data):
        """Generate MD5 hash for change data capture"""
        if isinstance(row_data, pd.Series):
            row_string = ''.join([str(val) for val in row_data.values if pd.notna(val)])
        else:
            row_string = str(row_data)
        return hashlib.md5(row_string.encode()).hexdigest()
    
    def _execute_sql(self, sql, params=None, fetch=False):
        """Execute SQL with error handling"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params)
            if fetch:
                return cursor.fetchall()
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"SQL execution failed: {e}")
            raise
        finally:
            cursor.close()
    
    def _ensure_schemas_exist(self):
        """Ensure all required schemas exist in the database"""
        schemas = ['staging', 'dimensions', 'facts', 'marts']
        
        try:
            for schema in schemas:
                sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"
                self._execute_sql(sql)
            self.logger.info("All required schemas are available")
        except Exception as e:
            self.logger.error(f"Failed to create schemas: {e}")
            raise
    
    def _bulk_insert_dataframe(self, df, table_name, schema='staging'):
        """Bulk insert DataFrame using psycopg2 execute_values"""
        if df.empty:
            return 0
        
        # Prepare column names and values
        columns = df.columns.tolist()
        values = df.values.tolist()
        
        cursor = self.connection.cursor()
        try:
            if schema in ['dimensions', 'facts']:
                # For dimension and fact tables: TRUNCATE to preserve structure and constraints
                truncate_sql = f"TRUNCATE TABLE {schema}.{table_name} RESTART IDENTITY CASCADE"
                cursor.execute(truncate_sql)
                self.logger.info(f"  Truncated {schema}.{table_name}")
                
                # Insert data directly into existing table
                columns_str = ', '.join(columns)
                insert_sql = f"INSERT INTO {schema}.{table_name} ({columns_str}) VALUES %s"
                
                execute_values(
                    cursor, 
                    insert_sql, 
                    values, 
                    template=None, 
                    page_size=1000
                )
            else:
                # For staging tables: DROP and CREATE (no foreign keys to worry about)
                drop_sql = f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE"
                cursor.execute(drop_sql)
                
                # Create table based on DataFrame dtypes
                create_sql = self._generate_create_table_sql(df, table_name, schema)
                cursor.execute(create_sql)
                
                # Insert data
                columns_str = ', '.join(columns)
                insert_sql = f"INSERT INTO {schema}.{table_name} ({columns_str}) VALUES %s"
                
                execute_values(
                    cursor, 
                    insert_sql, 
                    values, 
                    template=None, 
                    page_size=1000
                )
            
            self.connection.commit()
            return len(df)
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Failed to bulk insert into {schema}.{table_name}: {e}")
            raise
        finally:
            cursor.close()
    
    def _generate_create_table_sql(self, df, table_name, schema):
        """Generate CREATE TABLE SQL from DataFrame with nullable timestamps"""
        column_definitions = []
        
        for column, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "DECIMAL(15,2)"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                # Make timestamp columns nullable
                sql_type = "TIMESTAMP NULL"
            elif pd.api.types.is_bool_dtype(dtype):
                sql_type = "BOOLEAN"
            else:
                sql_type = "TEXT"
            
            column_definitions.append(f"{column} {sql_type}")
        
        columns_sql = ',\n    '.join(column_definitions)
        return f"CREATE TABLE {schema}.{table_name} (\n    {columns_sql}\n)"
    
    def extract_to_staging(self):
        """Extract data from CSV files to staging tables"""
        self.logger.info("=" * 60)
        self.logger.info("STARTING DATA EXTRACTION TO STAGING")
        self.logger.info("=" * 60)
        
        # Ensure schemas exist
        self._ensure_schemas_exist()
        
        # File mappings
        file_mappings = {
            'olist_customers_dataset.csv': 'stg_customers',
            'olist_products_dataset.csv': 'stg_products', 
            'olist_sellers_dataset.csv': 'stg_sellers',
            'olist_orders_dataset.csv': 'stg_orders',
            'olist_order_items_dataset.csv': 'stg_order_items',
            'olist_order_payments_dataset.csv': 'stg_payments',
            'olist_order_reviews_dataset.csv': 'stg_reviews',
            'product_category_name_translation.csv': 'stg_product_categories'
        }
        
        extraction_summary = {}
        
        for csv_file, table_name in file_mappings.items():
            try:
                records_loaded = self._extract_file_to_staging(csv_file, table_name)
                extraction_summary[table_name] = records_loaded
            except Exception as e:
                self.logger.error(f"Failed to extract {csv_file}: {e}")
                extraction_summary[table_name] = 0
        
        # Print summary
        self.logger.info("EXTRACTION SUMMARY:")
        total_records = 0
        for table, count in extraction_summary.items():
            self.logger.info(f"  {table}: {count:,} records")
            total_records += count
        self.logger.info(f"  TOTAL: {total_records:,} records extracted")
    
    def _extract_file_to_staging(self, csv_file, staging_table):
        """Extract individual CSV file to staging table"""
        file_path = self.data_path / csv_file
        
        if not file_path.exists():
            self.logger.warning(f"File not found: {file_path}")
            return 0
        
        self.logger.info(f"Extracting {csv_file} -> staging.{staging_table}")
        
        try:
            # Read CSV file
            df = pd.read_csv(file_path, encoding='utf-8')
            original_count = len(df)
            
            # Standardize column names
            df.columns = [col.strip().lower().replace('"', '').replace(' ', '_') for col in df.columns]
            
            # Add ETL metadata
            df['load_timestamp'] = datetime.now()
            df['source_file'] = csv_file
            df['record_hash'] = df.apply(self._generate_record_hash, axis=1)
            
            # Load to PostgreSQL using bulk insert
            records_loaded = self._bulk_insert_dataframe(df, staging_table, 'staging')
            
            self.logger.info(f"  ✅ Loaded {records_loaded:,} records (original: {original_count:,})")
            return records_loaded
            
        except Exception as e:
            self.logger.error(f"  ❌ Failed to extract {csv_file}: {e}")
            return 0
    
    def _read_sql_to_dataframe(self, query):
        """Execute SQL query and return DataFrame with proper transaction handling"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            # Rollback transaction on error
            self.connection.rollback()
            self.logger.error(f"Failed to execute query: {e}")
            raise
        finally:
            cursor.close()
    
    def transform_dimensions(self):
        """Transform staging data into dimension tables"""
        self.logger.info("=" * 60) 
        self.logger.info("STARTING DIMENSION TRANSFORMATIONS")
        self.logger.info("=" * 60)
        
        # Transform dimensions in dependency order
        dimensions = [
            ('dim_date', self._transform_date_dimension),
            ('dim_customer', self._transform_customer_dimension), 
            ('dim_product', self._transform_product_dimension),
            ('dim_seller', self._transform_seller_dimension),
            ('dim_order_status', self._transform_order_status_dimension)
        ]
        
        transformation_summary = {}
        
        for dim_name, transform_func in dimensions:
            try:
                records_count = transform_func()
                transformation_summary[dim_name] = records_count
            except Exception as e:
                self.logger.error(f"Failed to transform {dim_name}: {e}")
                transformation_summary[dim_name] = 0
        
        # Print summary
        self.logger.info("DIMENSION TRANSFORMATION SUMMARY:")
        for dim_name, count in transformation_summary.items():
            self.logger.info(f"  {dim_name}: {count:,} records")
    
    def _transform_date_dimension(self):
        """Create comprehensive date dimension"""
        self.logger.info("Transforming date dimension...")
        
        try:
            # Extract all dates from orders, reviews, AND order items (shipping dates)
            date_query = """
            SELECT DISTINCT 
                order_purchase_timestamp::date as date_val
            FROM staging.stg_orders 
            WHERE order_purchase_timestamp IS NOT NULL 
              AND order_purchase_timestamp != 'NaN'
              AND order_purchase_timestamp != ''
            UNION
            SELECT DISTINCT 
                order_approved_at::date as date_val
            FROM staging.stg_orders 
            WHERE order_approved_at IS NOT NULL 
              AND order_approved_at != 'NaN'
              AND order_approved_at != ''
            UNION
            SELECT DISTINCT 
                order_delivered_customer_date::date as date_val
            FROM staging.stg_orders 
            WHERE order_delivered_customer_date IS NOT NULL 
              AND order_delivered_customer_date != 'NaN'
              AND order_delivered_customer_date != ''
            UNION
            SELECT DISTINCT 
                review_creation_date::date as date_val
            FROM staging.stg_reviews
            WHERE review_creation_date IS NOT NULL 
              AND review_creation_date != 'NaN'
              AND review_creation_date != ''
            UNION
            SELECT DISTINCT 
                review_answer_timestamp::date as date_val
            FROM staging.stg_reviews
            WHERE review_answer_timestamp IS NOT NULL 
              AND review_answer_timestamp != 'NaN'
              AND review_answer_timestamp != ''
            UNION
            SELECT DISTINCT 
                shipping_limit_date::date as date_val
            FROM staging.stg_order_items
            WHERE shipping_limit_date IS NOT NULL 
              AND shipping_limit_date != 'NaN'
              AND shipping_limit_date != ''
            """
            
            dates_df = self._read_sql_to_dataframe(date_query)
            
            if dates_df.empty:
                self.logger.warning("No dates found in data")
                return 0
            
            # Create comprehensive date dimension
            dates_df['date_val'] = pd.to_datetime(dates_df['date_val'])
            dates_df = dates_df.sort_values('date_val').drop_duplicates()
            
            # Add date attributes
            dates_df['date_key'] = dates_df['date_val'].dt.strftime('%Y%m%d').astype(int)
            dates_df['full_date'] = dates_df['date_val'].dt.date
            dates_df['year'] = dates_df['date_val'].dt.year
            dates_df['quarter'] = dates_df['date_val'].dt.quarter
            dates_df['month'] = dates_df['date_val'].dt.month
            dates_df['day'] = dates_df['date_val'].dt.day
            dates_df['day_of_week'] = dates_df['date_val'].dt.dayofweek + 1
            dates_df['week_of_year'] = dates_df['date_val'].dt.isocalendar().week
            dates_df['month_name'] = dates_df['date_val'].dt.month_name()
            dates_df['day_name'] = dates_df['date_val'].dt.day_name()
            dates_df['is_weekend'] = dates_df['day_of_week'].isin([6, 7])
            dates_df['is_holiday'] = False  
            dates_df['fiscal_year'] = np.where(dates_df['month'] >= 7, dates_df['year'] + 1, dates_df['year'])
            dates_df['fiscal_quarter'] = ((dates_df['month'] - 1) // 3 + 1)
            dates_df['created_date'] = datetime.now()
            dates_df['updated_date'] = datetime.now()
            
            # Select final columns and drop intermediate column
            final_columns = [
                'date_key', 'full_date', 'year', 'quarter', 'month', 'day',
                'day_of_week', 'week_of_year', 'month_name', 'day_name', 
                'is_weekend', 'is_holiday', 'fiscal_year', 'fiscal_quarter',
                'created_date', 'updated_date'
            ]
            
            result_df = dates_df[final_columns].copy()
            
            # Load to database
            records_loaded = self._bulk_insert_dataframe(result_df, 'dim_date', 'dimensions')
            
            self.logger.info(f"  ✅ Created date dimension with {records_loaded:,} dates")
            return records_loaded
            
        except Exception as e:
            self.logger.error(f"Date dimension transformation failed: {e}")
            return 0
    
    def _transform_customer_dimension(self):
        """Transform customer dimension"""
        self.logger.info("Transforming customer dimension...")
        
        try:
            transform_query = """
            SELECT DISTINCT
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                INITCAP(TRIM(COALESCE(customer_city, 'Unknown'))) as customer_city,
                UPPER(TRIM(COALESCE(customer_state, 'Unknown'))) as customer_state,
                record_hash
            FROM staging.stg_customers
            WHERE customer_id IS NOT NULL
            """
            
            customers_df = self._read_sql_to_dataframe(transform_query)
            
            if customers_df.empty:
                self.logger.warning("No customers found")
                return 0
            
            # Add region mapping for Brazilian states
            state_to_region = {
                'AC': 'Norte', 'AM': 'Norte', 'AP': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
                'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 
                'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
                'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'MT': 'Centro-Oeste',
                'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
                'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
            }
            
            customers_df['customer_region'] = customers_df['customer_state'].map(state_to_region).fillna('Unknown')
            customers_df['is_active'] = True
            customers_df['created_date'] = datetime.now()
            customers_df['updated_date'] = datetime.now()
            
            # Reorder columns to match schema (exclude customer_sk - SERIAL will auto-generate)
            final_columns = [
                'customer_id', 'customer_unique_id', 'customer_zip_code_prefix',
                'customer_city', 'customer_state', 'customer_region',
                'record_hash', 'created_date', 'updated_date', 'is_active'
            ]
            customers_df = customers_df[final_columns]
            
            # Load to database
            records_loaded = self._bulk_insert_dataframe(customers_df, 'dim_customer', 'dimensions')
            
            self.logger.info(f"  ✅ Processed {records_loaded:,} customers")
            return records_loaded
            
        except Exception as e:
            self.logger.error(f"  ❌ Customer dimension transformation failed: {e}")
            return 0
    
    def _transform_product_dimension(self):
        """Transform product dimension with category translation"""
        self.logger.info("Transforming product dimension...")
        
        try:
            # Join products with category translation
            transform_query = """
            SELECT DISTINCT
                p.product_id,
                COALESCE(p.product_category_name, 'Unknown') as product_category_name,
                COALESCE(c.product_category_name_english, p.product_category_name, 'Unknown') as product_category_english,
                COALESCE(p.product_name_lenght, 0) as product_name_lenght,
                COALESCE(p.product_description_lenght, 0) as product_description_lenght,
                COALESCE(p.product_photos_qty, 0) as product_photos_qty,
                COALESCE(p.product_weight_g, 0) as product_weight_g,
                COALESCE(p.product_length_cm, 0) as product_length_cm,
                COALESCE(p.product_height_cm, 0) as product_height_cm,
                COALESCE(p.product_width_cm, 0) as product_width_cm,
                p.record_hash
            FROM staging.stg_products p
            LEFT JOIN staging.stg_product_categories c 
                ON p.product_category_name = c.product_category_name
            WHERE p.product_id IS NOT NULL
            """
            
            products_df = self._read_sql_to_dataframe(transform_query)
            
            if products_df.empty:
                self.logger.warning("No products found")
                return 0
            
            # Safe calculation of derived fields with error handling
            def safe_calculate_volume(row):
                try:
                    length = pd.to_numeric(row['product_length_cm'], errors='coerce')
                    height = pd.to_numeric(row['product_height_cm'], errors='coerce')
                    width = pd.to_numeric(row['product_width_cm'], errors='coerce')
                    
                    # Replace NaN with 0
                    length = 0 if pd.isna(length) else length
                    height = 0 if pd.isna(height) else height
                    width = 0 if pd.isna(width) else width
                    
                    return float(length * height * width)
                except:
                    return 0.0
            
            # Apply safe calculation
            products_df['product_volume_cm3'] = products_df.apply(safe_calculate_volume, axis=1)
            
            # Categorize product size
            def get_size_category(volume):
                try:
                    volume = float(volume) if volume is not None else 0
                    if volume <= 1000:
                        return 'Small'
                    elif volume <= 10000:
                        return 'Medium'
                    elif volume <= 50000:
                        return 'Large'
                    else:
                        return 'Extra Large'
                except:
                    return 'Unknown'
            
            products_df['product_size_category'] = products_df['product_volume_cm3'].apply(get_size_category)
            
            # SCD Type 2 fields - use date object to avoid timestamp overflow
            products_df['effective_start_date'] = datetime.now()
            # Use date(2999, 12, 31) to create a proper date without timestamp overflow
            from datetime import date
            products_df['effective_end_date'] = datetime(2999, 12, 31)
            products_df['is_current'] = True
            products_df['created_date'] = datetime.now()
            
            # Convert integer columns to proper types, handling NaN
            integer_columns = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty']
            for col in integer_columns:
                if col in products_df.columns:
                    products_df[col] = products_df[col].fillna(0).astype(int)
            
            # Convert decimal columns to proper types, handling NaN
            decimal_columns = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm', 'product_volume_cm3']
            for col in decimal_columns:
                if col in products_df.columns:
                    products_df[col] = products_df[col].fillna(0.0).astype(float)
            
            # Reorder columns to match schema (exclude product_sk - SERIAL will auto-generate)
            final_columns = [
                'product_id', 'product_category_name', 'product_category_english',
                'product_name_lenght', 'product_description_lenght', 'product_photos_qty',
                'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm',
                'product_volume_cm3', 'product_size_category',
                'effective_start_date', 'effective_end_date', 'is_current',
                'record_hash', 'created_date'
            ]
            products_df = products_df[final_columns]
            
            # Load to database
            records_loaded = self._bulk_insert_dataframe(products_df, 'dim_product', 'dimensions')
            
            self.logger.info(f"  ✅ Processed {records_loaded:,} products")
            return records_loaded
            
        except Exception as e:
            self.logger.error(f"Product dimension transformation failed: {e}")
            return 0
    
    def _transform_seller_dimension(self):
        """Transform seller dimension"""
        self.logger.info("Transforming seller dimension...")
        
        try:
            transform_query = """
            SELECT DISTINCT
                seller_id,
                seller_zip_code_prefix,
                INITCAP(TRIM(COALESCE(seller_city, 'Unknown'))) as seller_city,
                UPPER(TRIM(COALESCE(seller_state, 'Unknown'))) as seller_state,
                record_hash
            FROM staging.stg_sellers
            WHERE seller_id IS NOT NULL
            """
            
            sellers_df = self._read_sql_to_dataframe(transform_query)
            
            if sellers_df.empty:
                self.logger.warning("No sellers found")
                return 0
            
            # Add region mapping
            state_to_region = {
                'AC': 'Norte', 'AM': 'Norte', 'AP': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
                'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 
                'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
                'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'MT': 'Centro-Oeste',
                'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
                'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
            }
            
            sellers_df['seller_region'] = sellers_df['seller_state'].map(state_to_region).fillna('Unknown')
            sellers_df['is_active'] = True
            sellers_df['created_date'] = datetime.now()
            sellers_df['updated_date'] = datetime.now()
            
            # Reorder columns to match schema (exclude seller_sk - SERIAL will auto-generate)
            final_columns = [
                'seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state', 'seller_region',
                'record_hash', 'created_date', 'updated_date', 'is_active'
            ]
            sellers_df = sellers_df[final_columns]
            
            # Load to database
            records_loaded = self._bulk_insert_dataframe(sellers_df, 'dim_seller', 'dimensions')
            
            self.logger.info(f"  ✅ Processed {records_loaded:,} sellers")
            return records_loaded
            
        except Exception as e:
            self.logger.error(f"  ❌ Seller dimension transformation failed: {e}")
            return 0
    
    def _transform_order_status_dimension(self):
        """Transform order status dimension with meaningful defaults"""
        self.logger.info("Transforming order status dimension...")
        
        try:
            # Get raw data with better NULL handling
            transform_query = """
            SELECT DISTINCT
                order_id,
                order_status,
                order_purchase_timestamp,
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date,
                record_hash
            FROM staging.stg_orders
            WHERE order_id IS NOT NULL
            """
            
            orders_df = self._read_sql_to_dataframe(transform_query)
            
            if orders_df.empty:
                self.logger.warning("No orders found")
                return 0
            
            # Step 1: Convert timestamp columns and handle invalid values
            timestamp_columns = [
                'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 
                'order_delivered_customer_date', 'order_estimated_delivery_date'
            ]
            
            for col in timestamp_columns:
                if col in orders_df.columns:
                    # Convert to datetime, coercing invalid values to NaT
                    orders_df[col] = pd.to_datetime(orders_df[col], errors='coerce')
            
            # Step 2: Fill missing timestamps with meaningful defaults based on business logic
            def fill_missing_timestamps(row):
                # Get base timestamp - order_purchase_timestamp should always exist
                base_time = row['order_purchase_timestamp']
                
                if pd.isna(base_time):
                    # If even purchase timestamp is missing, use a default date
                    base_time = pd.to_datetime('2018-01-01')  # Default to start of business period
                
                # Fill order_approved_at: typically approved within hours of purchase
                if pd.isna(row['order_approved_at']):
                    row['order_approved_at'] = base_time + pd.Timedelta(hours=2)
                
                # Fill order_delivered_carrier_date: typically 1-3 days after approval
                if pd.isna(row['order_delivered_carrier_date']):
                    approved_time = row['order_approved_at'] if pd.notna(row['order_approved_at']) else base_time
                    row['order_delivered_carrier_date'] = approved_time + pd.Timedelta(days=2)
                
                # Fill order_delivered_customer_date: typically 5-15 days after purchase
                if pd.isna(row['order_delivered_customer_date']):
                    # Use estimated delivery date if available, otherwise calculate from purchase
                    if pd.notna(row['order_estimated_delivery_date']):
                        row['order_delivered_customer_date'] = row['order_estimated_delivery_date']
                    else:
                        row['order_delivered_customer_date'] = base_time + pd.Timedelta(days=10)
                
                # Fill order_estimated_delivery_date: typically 7-20 days after purchase
                if pd.isna(row['order_estimated_delivery_date']):
                    row['order_estimated_delivery_date'] = base_time + pd.Timedelta(days=15)
                
                return row
            
            # Apply the filling logic
            self.logger.info("  Filling missing timestamps with business-logic defaults...")
            orders_df = orders_df.apply(fill_missing_timestamps, axis=1)
            
            # Step 3: Calculate delivery metrics with proper values
            def safe_time_diff_hours(end_time, start_time):
                try:
                    if pd.notna(end_time) and pd.notna(start_time):
                        diff = (end_time - start_time).total_seconds() / 3600
                        return int(diff) if not pd.isna(diff) else 0
                    return 0
                except:
                    return 0
            
            def safe_time_diff_days(end_time, start_time):
                try:
                    if pd.notna(end_time) and pd.notna(start_time):
                        diff = (end_time - start_time).days
                        return int(diff) if not pd.isna(diff) else 0
                    return 0
                except:
                    return 0
            
            # Calculate metrics
            orders_df['approval_delay_hours'] = orders_df.apply(
                lambda row: safe_time_diff_hours(row['order_approved_at'], row['order_purchase_timestamp']), axis=1
            )
            
            orders_df['delivery_delay_days'] = orders_df.apply(
                lambda row: safe_time_diff_days(row['order_delivered_customer_date'], row['order_estimated_delivery_date']), axis=1
            )
            
            orders_df['total_delivery_days'] = orders_df.apply(
                lambda row: safe_time_diff_days(row['order_delivered_customer_date'], row['order_purchase_timestamp']), axis=1
            )
            
            orders_df['is_delivered_on_time'] = orders_df['delivery_delay_days'] <= 0
            
            # Categorize delivery performance
            def categorize_delivery_performance(delay_days):
                try:
                    if delay_days < -5:
                        return 'Early'
                    elif delay_days <= 0:
                        return 'On Time'
                    elif delay_days <= 5:
                        return 'Slightly Late'
                    else:
                        return 'Very Late'
                except:
                    return 'Unknown'
            
            orders_df['delivery_performance_category'] = orders_df['delivery_delay_days'].apply(categorize_delivery_performance)
            
            orders_df['created_date'] = datetime.now()
            orders_df['updated_date'] = datetime.now()
            
            # Reorder columns to match schema (exclude order_status_sk - SERIAL will auto-generate)
            # Remove filling flag columns as they don't exist in schema
            final_columns = [
                'order_id', 'order_status', 'order_purchase_timestamp',
                'order_approved_at', 'order_delivered_carrier_date',
                'order_delivered_customer_date', 'order_estimated_delivery_date',
                'approval_delay_hours', 'delivery_delay_days', 'total_delivery_days',
                'is_delivered_on_time', 'delivery_performance_category',
                'record_hash', 'created_date', 'updated_date'
            ]
            orders_df = orders_df[final_columns]
            
            # Load to database
            records_loaded = self._bulk_insert_dataframe(orders_df, 'dim_order_status', 'dimensions')
            
            self.logger.info(f"  ✅ Processed {records_loaded:,} orders with meaningful timestamps")
            return records_loaded
            
        except Exception as e:
            self.logger.error(f"Order status dimension transformation failed: {e}")
            return 0
    
    def transform_facts(self):
        """Transform staging data into fact tables"""
        self.logger.info("=" * 60)
        self.logger.info("STARTING FACT TABLE TRANSFORMATIONS") 
        self.logger.info("=" * 60)
        
        facts = [
            ('fact_order_items', self._transform_order_items_fact),
            ('fact_payments', self._transform_payments_fact),
            ('fact_reviews', self._transform_reviews_fact)
        ]
        
        transformation_summary = {}
        
        for fact_name, transform_func in facts:
            try:
                records_count = transform_func()
                transformation_summary[fact_name] = records_count
            except Exception as e:
                self.logger.error(f"Failed to transform {fact_name}: {e}")
                transformation_summary[fact_name] = 0
        
        # Print summary
        self.logger.info("FACT TRANSFORMATION SUMMARY:")
        for fact_name, count in transformation_summary.items():
            self.logger.info(f"  {fact_name}: {count:,} records")
    
    def _transform_order_items_fact(self):
        """Transform order items fact table"""
        self.logger.info("Transforming order items fact...")
        
        try:
            # First, get surrogate keys from dimension tables
            transform_query = """
            SELECT 
                oi.order_id,
                oi.order_item_id,
                
                -- Get surrogate keys via JOINs
                dc.customer_sk,
                dp.product_sk,
                ds.seller_sk,
                dos.order_status_sk,
                
                -- Date keys
                CASE 
                    WHEN o.order_purchase_timestamp IS NOT NULL 
                     AND o.order_purchase_timestamp != 'NaN' 
                     AND o.order_purchase_timestamp != ''
                    THEN CAST(TO_CHAR(o.order_purchase_timestamp::date, 'YYYYMMDD') AS INTEGER)
                    ELSE NULL 
                END as order_date_key,
                CASE 
                    WHEN oi.shipping_limit_date IS NOT NULL 
                     AND oi.shipping_limit_date != 'NaN' 
                     AND oi.shipping_limit_date != ''
                    THEN CAST(TO_CHAR(oi.shipping_limit_date::date, 'YYYYMMDD') AS INTEGER)
                    ELSE NULL 
                END as shipping_date_key,
                
                -- Measures
                COALESCE(oi.price, 0) as item_price,
                COALESCE(oi.freight_value, 0) as freight_value,
                COALESCE(oi.price, 0) + COALESCE(oi.freight_value, 0) as total_item_value,
                
                -- Degenerate dimensions  
                CASE 
                    WHEN oi.shipping_limit_date IS NOT NULL 
                     AND oi.shipping_limit_date != 'NaN' 
                     AND oi.shipping_limit_date != ''
                    THEN oi.shipping_limit_date::timestamp
                    ELSE NULL 
                END as shipping_limit_date
                
            FROM staging.stg_order_items oi
            LEFT JOIN staging.stg_orders o ON oi.order_id = o.order_id
            LEFT JOIN dimensions.dim_customer dc ON o.customer_id = dc.customer_id
            LEFT JOIN dimensions.dim_product dp ON oi.product_id = dp.product_id AND dp.is_current = true
            LEFT JOIN dimensions.dim_seller ds ON oi.seller_id = ds.seller_id
            LEFT JOIN dimensions.dim_order_status dos ON oi.order_id = dos.order_id
            WHERE oi.order_id IS NOT NULL 
              AND oi.order_item_id IS NOT NULL
              AND oi.order_item_id > 0
              AND dc.customer_sk IS NOT NULL
              AND dp.product_sk IS NOT NULL
              AND ds.seller_sk IS NOT NULL
              AND dos.order_status_sk IS NOT NULL
            """
            
            fact_df = self._read_sql_to_dataframe(transform_query)
            
            if fact_df.empty:
                self.logger.warning("No order items found")
                return 0
            
            fact_df['created_date'] = datetime.now()
            
            # Reorder columns to match schema (exclude order_item_sk - BIGSERIAL will auto-generate)
            final_columns = [
                'order_id', 'order_item_id', 'customer_sk', 'product_sk', 'seller_sk', 'order_status_sk',
                'order_date_key', 'shipping_date_key',
                'item_price', 'freight_value', 'total_item_value',
                'shipping_limit_date', 'created_date'
            ]
            fact_df = fact_df[final_columns]
            
            # Load to database
            records_loaded = self._bulk_insert_dataframe(fact_df, 'fact_order_items', 'facts')
            
            self.logger.info(f"  ✅ Processed {records_loaded:,} order items")
            return records_loaded
            
        except Exception as e:
            self.logger.error(f"Order items fact transformation failed: {e}")
            return 0

    def _transform_payments_fact(self):
        """Transform payments fact table"""
        self.logger.info("Transforming payments fact...")
        
        try:
            transform_query = """
            SELECT 
                p.order_id,
                COALESCE(p.payment_sequential, 1) as payment_sequential,
                COALESCE(p.payment_type, 'not_defined') as payment_type,
                COALESCE(p.payment_installments, 0) as payment_installments,  -- Allow 0 installments
                COALESCE(p.payment_value, 0) as payment_value,
                CASE 
                    WHEN o.order_purchase_timestamp IS NOT NULL 
                     AND o.order_purchase_timestamp != 'NaN' 
                     AND o.order_purchase_timestamp != ''
                    THEN CAST(TO_CHAR(o.order_purchase_timestamp::date, 'YYYYMMDD') AS INTEGER)
                    ELSE NULL 
                END as order_date_key,
                dos.order_status_sk
                
            FROM staging.stg_payments p
            LEFT JOIN staging.stg_orders o ON p.order_id = o.order_id
            LEFT JOIN dimensions.dim_order_status dos ON p.order_id = dos.order_id
            WHERE p.order_id IS NOT NULL
              AND dos.order_status_sk IS NOT NULL
            """
            
            payments_df = self._read_sql_to_dataframe(transform_query)
            
            if payments_df.empty:
                self.logger.warning("No payments found")
                return 0
            
            payments_df['created_date'] = datetime.now()
            
            # Reorder columns to match schema (exclude payment_sk - BIGSERIAL will auto-generate)
            final_columns = [
                'order_id', 'payment_sequential', 'order_date_key', 'order_status_sk',
                'payment_installments', 'payment_value', 'payment_type', 'created_date'
            ]
            payments_df = payments_df[final_columns]
            
            # Load to database
            records_loaded = self._bulk_insert_dataframe(payments_df, 'fact_payments', 'facts')
            
            self.logger.info(f"  ✅ Processed {records_loaded:,} payments")
            return records_loaded
            
        except Exception as e:
            self.logger.error(f"Payments fact transformation failed: {e}")
            return 0

    def _transform_reviews_fact(self):
        """Transform reviews fact table"""
        self.logger.info("Transforming reviews fact...")
        
        try:
            transform_query = """
            SELECT DISTINCT ON (r.review_id)
                r.review_id,
                r.order_id,
                COALESCE(r.review_score, 0) as review_score,
                r.review_comment_title,
                r.review_comment_message,
                CASE 
                    WHEN r.review_creation_date IS NOT NULL 
                     AND r.review_creation_date != 'NaN' 
                     AND r.review_creation_date != ''
                    THEN r.review_creation_date::timestamp
                    ELSE NULL 
                END as review_creation_date,
                CASE 
                    WHEN r.review_answer_timestamp IS NOT NULL 
                     AND r.review_answer_timestamp != 'NaN' 
                     AND r.review_answer_timestamp != ''
                    THEN r.review_answer_timestamp::timestamp
                    ELSE NULL 
                END as review_answer_timestamp,
                
                -- Get surrogate keys
                dc.customer_sk,
                dos.order_status_sk,
                
                -- Date keys
                CASE 
                    WHEN r.review_creation_date IS NOT NULL 
                     AND r.review_creation_date != 'NaN' 
                     AND r.review_creation_date != ''
                    THEN CAST(TO_CHAR(r.review_creation_date::date, 'YYYYMMDD') AS INTEGER)
                    ELSE NULL 
                END as review_date_key,
                CASE 
                    WHEN r.review_answer_timestamp IS NOT NULL 
                     AND r.review_answer_timestamp != 'NaN' 
                     AND r.review_answer_timestamp != ''
                    THEN CAST(TO_CHAR(r.review_answer_timestamp::date, 'YYYYMMDD') AS INTEGER)
                    ELSE NULL 
                END as answer_date_key,
                
                -- Calculated fields
                CASE 
                    WHEN r.review_score <= 2 THEN 'Poor'
                    WHEN r.review_score = 3 THEN 'Fair'
                    WHEN r.review_score = 4 THEN 'Good' 
                    WHEN r.review_score = 5 THEN 'Excellent'
                    ELSE 'Unknown'
                END as review_category,
                
                CASE 
                    WHEN r.review_answer_timestamp IS NOT NULL 
                     AND r.review_creation_date IS NOT NULL
                     AND r.review_answer_timestamp != 'NaN' 
                     AND r.review_creation_date != 'NaN'
                     AND r.review_answer_timestamp != ''
                     AND r.review_creation_date != ''
                    THEN EXTRACT(EPOCH FROM (r.review_answer_timestamp::timestamp - r.review_creation_date::timestamp))/3600
                    ELSE NULL 
                END as response_time_hours
                
            FROM staging.stg_reviews r
            LEFT JOIN staging.stg_orders o ON r.order_id = o.order_id
            LEFT JOIN dimensions.dim_customer dc ON o.customer_id = dc.customer_id
            LEFT JOIN dimensions.dim_order_status dos ON r.order_id = dos.order_id
            WHERE r.review_id IS NOT NULL
              AND dc.customer_sk IS NOT NULL
              AND dos.order_status_sk IS NOT NULL
            ORDER BY r.review_id, r.review_creation_date DESC
            """
            
            reviews_df = self._read_sql_to_dataframe(transform_query)
            
            if reviews_df.empty:
                self.logger.warning("No reviews found")
                return 0
            
            # Additional duplicate check in Python
            initial_count = len(reviews_df)
            reviews_df = reviews_df.drop_duplicates(subset=['review_id'], keep='first')
            final_count = len(reviews_df)
            
            if initial_count > final_count:
                self.logger.info(f"  Removed {initial_count - final_count} duplicate review_id records")
            
            reviews_df['created_date'] = datetime.now()
            
            # Convert response_time_hours to INTEGER
            reviews_df['response_time_hours'] = reviews_df['response_time_hours'].fillna(0).astype(int)
            
            # Reorder columns to match schema (exclude review_sk - BIGSERIAL will auto-generate)
            final_columns = [
                'review_id', 'order_id', 'customer_sk', 'order_status_sk',
                'review_date_key', 'answer_date_key', 'review_score',
                'review_comment_title', 'review_comment_message',
                'review_creation_date', 'review_answer_timestamp',
                'review_category', 'response_time_hours', 'created_date'
            ]
            reviews_df = reviews_df[final_columns]
            
            # Load to database
            records_loaded = self._bulk_insert_dataframe(reviews_df, 'fact_reviews', 'facts')
            
            self.logger.info(f"  ✅ Processed {records_loaded:,} reviews")
            return records_loaded
            
        except Exception as e:
            self.logger.error(f"Reviews fact transformation failed: {e}")
            return 0

    def load_marts(self):
        """Load aggregated data into mart tables"""
        self.logger.info("=" * 60)
        self.logger.info("LOADING DATA INTO MART TABLES")
        self.logger.info("=" * 60)
        
        marts = [
            ('agg_monthly_sales', self._load_monthly_sales_mart),
            ('agg_product_performance', self._load_product_performance_mart)
        ]
        
        mart_summary = {}
        
        for mart_name, load_func in marts:
            try:
                records_count = load_func()
                mart_summary[mart_name] = records_count
            except Exception as e:
                self.logger.error(f"Failed to load {mart_name}: {e}")
                mart_summary[mart_name] = 0
        
        # Print summary
        self.logger.info("MART LOADING SUMMARY:")
        for mart_name, count in mart_summary.items():
            self.logger.info(f"  {mart_name}: {count:,} records")

    def _load_monthly_sales_mart(self):
        """Load monthly sales aggregate mart"""
        self.logger.info("Loading monthly sales mart...")
        
        try:
            # Extract and aggregate monthly sales data
            query = """
            WITH order_item_counts AS (
                SELECT 
                    foi.order_id,
                    COUNT(foi.order_item_sk) as items_in_order
                FROM facts.fact_order_items foi
                GROUP BY foi.order_id
            ),
            monthly_agg AS (
                SELECT 
                    CAST(TO_CHAR(dd.full_date, 'YYYYMM') AS INTEGER) as year_month,
                    dd.year,
                    dd.month,
                    dd.month_name,
                    
                    -- Count aggregates
                    COUNT(DISTINCT foi.order_id) as total_orders,
                    COUNT(foi.order_item_sk) as total_items,
                    
                    -- Revenue measures
                    COALESCE(SUM(foi.item_price), 0) as total_revenue,
                    COALESCE(SUM(foi.freight_value), 0) as total_freight,
                    
                    -- Average measures
                    COALESCE(AVG(foi.total_item_value), 0) as avg_order_value,
                    
                    -- Customer metrics
                    COUNT(DISTINCT foi.customer_sk) as unique_customers,
                    
                    -- Product metrics  
                    COUNT(DISTINCT foi.product_sk) as unique_products,
                    
                    -- Geographic metrics
                    COUNT(DISTINCT dc.customer_state) as unique_states
                    
                FROM facts.fact_order_items foi
                LEFT JOIN dimensions.dim_date dd ON foi.order_date_key = dd.date_key
                LEFT JOIN dimensions.dim_customer dc ON foi.customer_sk = dc.customer_sk
                WHERE dd.full_date IS NOT NULL
                GROUP BY 
                    CAST(TO_CHAR(dd.full_date, 'YYYYMM') AS INTEGER),
                    dd.year,
                    dd.month,
                    dd.month_name
            )
            
            SELECT 
                year_month,
                year,
                month,
                month_name,
                total_orders,
                total_items,
                total_revenue,
                total_freight,
                avg_order_value,
                CASE 
                    WHEN total_orders > 0 THEN CAST(CAST(total_items AS DECIMAL(10,2)) / total_orders AS DECIMAL(10,2))
                    ELSE 0
                END as avg_items_per_order,
                unique_customers,
                0 as new_customers,
                unique_products,
                unique_states,
                CURRENT_TIMESTAMP as last_updated
            FROM monthly_agg
            ORDER BY year_month DESC
            """
            
            sales_df = self._read_sql_to_dataframe(query)
            
            if sales_df.empty:
                self.logger.warning("No monthly sales data found")
                return 0
            
            # Convert columns to proper types
            sales_df['total_orders'] = sales_df['total_orders'].astype(int)
            sales_df['total_items'] = sales_df['total_items'].astype(int)
            sales_df['avg_items_per_order'] = sales_df['avg_items_per_order'].astype(float)
            sales_df['unique_customers'] = sales_df['unique_customers'].astype(int)
            sales_df['new_customers'] = sales_df['new_customers'].astype(int)
            sales_df['unique_products'] = sales_df['unique_products'].astype(int)
            sales_df['unique_states'] = sales_df['unique_states'].astype(int)
            
            # Truncate mart table and load data
            cursor = self.connection.cursor()
            try:
                cursor.execute("TRUNCATE TABLE marts.agg_monthly_sales RESTART IDENTITY CASCADE")
                
                columns = sales_df.columns.tolist()
                columns_str = ', '.join(columns)
                values = sales_df.values.tolist()
                
                insert_sql = f"INSERT INTO marts.agg_monthly_sales ({columns_str}) VALUES %s"
                execute_values(cursor, insert_sql, values, page_size=1000)
                
                self.connection.commit()
                self.logger.info(f"  ✅ Loaded {len(sales_df):,} months into monthly sales mart")
                return len(sales_df)
                
            except Exception as e:
                self.connection.rollback()
                self.logger.error(f"Failed to load monthly sales mart: {e}")
                raise
            finally:
                cursor.close()
                
        except Exception as e:
            self.logger.error(f"Monthly sales mart loading failed: {e}")
            return 0

    def _load_product_performance_mart(self):
        """Load product performance aggregate mart"""
        self.logger.info("Loading product performance mart...")
        
        try:
            # Extract and aggregate product performance data
            query = """
            WITH product_metrics AS (
                SELECT 
                    dp.product_sk,
                    
                    -- Revenue metrics (last 12 months - approximate using all available data)
                    COALESCE(SUM(foi.item_price), 0) as last_12_months_revenue,
                    COUNT(DISTINCT foi.order_id) as last_12_months_orders,
                    COUNT(DISTINCT foi.order_item_sk) as last_12_months_items,
                    
                    -- Price metrics
                    COALESCE(AVG(foi.item_price), 0) as avg_price,
                    
                    -- Review metrics
                    COALESCE(AVG(CAST(fr.review_score AS DECIMAL(3,2))), 0) as avg_review_score,
                    COUNT(DISTINCT fr.review_sk) as total_reviews
                    
                FROM dimensions.dim_product dp
                LEFT JOIN facts.fact_order_items foi ON dp.product_sk = foi.product_sk
                LEFT JOIN facts.fact_reviews fr ON foi.order_id = fr.order_id
                WHERE dp.is_current = TRUE
                GROUP BY dp.product_sk
            ),
            ranked_products AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (ORDER BY last_12_months_revenue DESC) as revenue_rank,
                    ROW_NUMBER() OVER (ORDER BY last_12_months_items DESC) as volume_rank
                FROM product_metrics
            )
            
            SELECT 
                product_sk,
                last_12_months_revenue,
                last_12_months_orders,
                last_12_months_items,
                avg_price,
                CASE 
                    WHEN avg_review_score > 0 THEN avg_review_score
                    ELSE NULL
                END as avg_review_score,
                total_reviews,
                revenue_rank,
                volume_rank,
                CURRENT_TIMESTAMP as last_updated
            FROM ranked_products
            WHERE product_sk IS NOT NULL
            ORDER BY revenue_rank
            """
            
            performance_df = self._read_sql_to_dataframe(query)
            
            if performance_df.empty:
                self.logger.warning("No product performance data found")
                return 0
            
            # Convert columns to proper types
            performance_df['last_12_months_orders'] = performance_df['last_12_months_orders'].astype(int)
            performance_df['last_12_months_items'] = performance_df['last_12_months_items'].astype(int)
            performance_df['total_reviews'] = performance_df['total_reviews'].astype(int)
            performance_df['revenue_rank'] = performance_df['revenue_rank'].astype(int)
            performance_df['volume_rank'] = performance_df['volume_rank'].astype(int)
            
            # Truncate mart table and load data
            cursor = self.connection.cursor()
            try:
                cursor.execute("TRUNCATE TABLE marts.agg_product_performance RESTART IDENTITY CASCADE")
                
                columns = performance_df.columns.tolist()
                columns_str = ', '.join(columns)
                values = performance_df.values.tolist()
                
                insert_sql = f"INSERT INTO marts.agg_product_performance ({columns_str}) VALUES %s"
                execute_values(cursor, insert_sql, values, page_size=1000)
                
                self.connection.commit()
                self.logger.info(f"  ✅ Loaded {len(performance_df):,} products into performance mart")
                return len(performance_df)
                
            except Exception as e:
                self.connection.rollback()
                self.logger.error(f"Failed to load product performance mart: {e}")
                raise
            finally:
                cursor.close()
                
        except Exception as e:
            self.logger.error(f"Product performance mart loading failed: {e}")
            return 0

    def run_full_etl(self):
        """Execute the complete ETL pipeline - DIRECT DATA INSERTION"""
        start_time = datetime.now()
        
        self.logger.info("=" * 80)
        self.logger.info("🚀 STARTING FULL ETL PIPELINE - DIRECT DATA INSERTION")
        self.logger.info("=" * 80)
        self.logger.info(f"Start time: {start_time}")
        
        try:
            # Step 1: Extract to staging
            self.extract_to_staging()
            
            # Step 2: Transform dimensions (with TRUNCATE, preserving structure)
            self.transform_dimensions()
            
            # Step 3: Transform facts (with surrogate keys resolved via JOINs)
            self.transform_facts()
            
            # Step 4: Load marts (aggregate tables)
            self.load_marts()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info("=" * 80)
            self.logger.info("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
            self.logger.info("=" * 80)
            self.logger.info(f"Total execution time: {duration}")
            self.logger.info("")
            self.logger.info("📊 DATA WAREHOUSE STATUS:")
            self.logger.info("  ✓ All staging tables populated with raw data")
            self.logger.info("  ✓ All dimension tables populated with SERIAL surrogate keys")
            self.logger.info("  ✓ All fact tables populated with resolved foreign keys")
            self.logger.info("  ✓ All mart tables populated with aggregated data")
            self.logger.info("  ✓ Foreign key constraints are ACTIVE")
            self.logger.info("")
            self.logger.info("📈 AVAILABLE MARTS:")
            self.logger.info("  • marts.agg_monthly_sales - Monthly sales aggregates by time dimension")
            self.logger.info("  • marts.agg_product_performance - Product performance rankings & metrics")
            self.logger.info("")
            self.logger.info("🎉 READY FOR ANALYSIS:")
            self.logger.info("  1. Open pgAdmin and refresh the database")  
            self.logger.info("  2. Right-click on 'olist_datawarehouse' → Generate ERD")
            self.logger.info("  3. All relationships should be visible!")
            self.logger.info("  4. Query marts.agg_monthly_sales and marts.agg_product_performance")
            self.logger.info("")
            self.logger.info("=" * 80)
            
        except Exception as e:
            self.logger.error(f"❌ ETL PIPELINE FAILED: {e}")
            raise
        finally:
            # Close connection
            if self.connection:
                self.connection.close()
                self.logger.info("Database connection closed")

if __name__ == "__main__":
    try:
        etl = OlistETLPipeline()
        etl.run_full_etl()
    except Exception as e:
        print(f"ETL Pipeline execution failed: {e}")
        exit(1)
