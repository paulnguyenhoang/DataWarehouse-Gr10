"""
Data Overview - System Information and Database Status
"""

import streamlit as st
import pandas as pd
import psycopg2
from pathlib import Path

def get_db_connection():
    """Create database connection"""
    env_file = Path(__file__).parent.parent / '.env'
    env_vars = {}
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip()
    
    conn = psycopg2.connect(
        host=env_vars.get('DB_HOST', 'localhost'),
        port=int(env_vars.get('DB_PORT', 5432)),
        database=env_vars.get('DB_NAME', 'olist_datawarehouse'),
        user=env_vars.get('DB_USER', 'postgres'),
        password=env_vars.get('DB_PASSWORD', '')
    )
    return conn

@st.cache_data(ttl=3600)
def get_table_stats():
    """Get statistics about tables in the database"""
    query = """
    SELECT 
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
    FROM pg_tables
    WHERE schemaname IN ('staging', 'dimensions', 'facts', 'marts')
    ORDER BY schemaname, tablename
    """
    
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to fetch table stats: {e}")
        return None

@st.cache_data(ttl=3600)
def get_row_counts():
    """Get row counts for each table"""
    tables = {
        'Staging': ['stg_customers', 'stg_products', 'stg_sellers', 'stg_orders', 'stg_order_items', 'stg_payments', 'stg_reviews', 'stg_product_categories'],
        'Dimensions': ['dim_date', 'dim_customer', 'dim_product', 'dim_seller', 'dim_order_status'],
        'Facts': ['fact_order_items', 'fact_payments', 'fact_reviews'],
        'Marts': ['agg_monthly_sales', 'agg_product_performance']
    }
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        results = []
        for layer, table_list in tables.items():
            for table in table_list:
                # Find schema
                schema = 'staging' if layer == 'Staging' else layer.lower()
                query = f"SELECT COUNT(*) FROM {schema}.{table}"
                
                try:
                    cursor.execute(query)
                    count = cursor.fetchone()[0]
                    results.append({
                        'Layer': layer,
                        'Table': table,
                        'Rows': count
                    })
                except:
                    results.append({
                        'Layer': layer,
                        'Table': table,
                        'Rows': 0
                    })
        
        conn.close()
        return pd.DataFrame(results)
    except Exception as e:
        st.error(f"Failed to fetch row counts: {e}")
        return None

def show():
    """Display data overview page"""
    
    st.markdown('<div class="header">📊 Data Warehouse Overview</div>', unsafe_allow_html=True)
    st.markdown("Monitor data warehouse structure, statistics, and table information")
    
    # Connection status
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        st.success("✅ Database Connection: Active")
        st.info(f"PostgreSQL: {version.split(',')[0]}")
    except Exception as e:
        st.error(f"❌ Database Connection: Failed - {e}")
        return
    
    st.divider()
    
    # Row counts
    st.subheader("📈 Table Row Counts")
    
    row_counts = get_row_counts()
    
    if row_counts is not None and not row_counts.empty:
        # Pivot for better display
        pivot_df = row_counts.pivot_table(index='Layer', columns='Table', values='Rows', aggfunc='sum')
        
        # Display summary
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            staging_rows = row_counts[row_counts['Layer'] == 'Staging']['Rows'].sum()
            st.metric("Staging Rows", f"{staging_rows:,.0f}")
        
        with col2:
            dim_rows = row_counts[row_counts['Layer'] == 'Dimensions']['Rows'].sum()
            st.metric("Dimension Rows", f"{dim_rows:,.0f}")
        
        with col3:
            fact_rows = row_counts[row_counts['Layer'] == 'Facts']['Rows'].sum()
            st.metric("Fact Rows", f"{fact_rows:,.0f}")
        
        with col4:
            mart_rows = row_counts[row_counts['Layer'] == 'Marts']['Rows'].sum()
            st.metric("Mart Rows", f"{mart_rows:,.0f}")
        
        st.divider()
        
        # Detailed table view
        st.subheader("📋 Detailed Table Statistics")
        
        for layer in ['Staging', 'Dimensions', 'Facts', 'Marts']:
            with st.expander(f"🔍 {layer} Tables"):
                layer_data = row_counts[row_counts['Layer'] == layer].sort_values('Table')
                
                display_df = layer_data[['Table', 'Rows']].copy()
                display_df.columns = ['Table Name', 'Row Count']
                display_df['Row Count'] = display_df['Row Count'].apply(lambda x: f"{x:,.0f}")
                
                st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Table sizes
    st.subheader("💾 Table Sizes")
    
    table_stats = get_table_stats()
    
    if table_stats is not None and not table_stats.empty:
        for schema in ['staging', 'dimensions', 'facts', 'marts']:
            schema_data = table_stats[table_stats['schemaname'] == schema]
            
            if not schema_data.empty:
                with st.expander(f"📦 {schema.upper()} Schema"):
                    display_df = schema_data[['tablename', 'size']].copy()
                    display_df.columns = ['Table', 'Size']
                    
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
                    
                    total_size = schema_data['size'].str.extract('(\d+)').astype(float).sum().values[0]
                    st.info(f"Total schema size: ~{total_size:.1f} MB")
    
    st.divider()
    
    # Architecture info
    st.subheader("🏗️ Data Warehouse Architecture")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**📍 Staging Layer**")
        st.write("""
        - Temporary storage for raw data
        - 8 staging tables
        - Loaded from CSV files
        - Includes ETL metadata (timestamps, hashes)
        """)
    
    with col2:
        st.write("**⭐ Dimension Layer**")
        st.write("""
        - 5 dimension tables
        - SERIAL surrogate keys
        - SCD Type 1 & 2 implementations
        - Includes calculated fields
        """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**📊 Fact Layer**")
        st.write("""
        - 3 fact tables
        - BIGSERIAL transaction IDs
        - Foreign keys to dimensions
        - Additive measures
        """)
    
    with col2:
        st.write("**📈 Mart Layer**")
        st.write("""
        - 2 aggregate tables
        - Pre-calculated metrics
        - Monthly & Product rankings
        - Ready for BI tools
        """)
    
    st.divider()
    
    # Schema diagram
    st.subheader("🔗 Data Flow")
    
    st.markdown("""
    ```
    CSV Files
        ↓
    [STAGING] (8 tables)
        ↓
    [DIMENSIONS] (5 tables) ←→ [FACTS] (3 tables)
                                   ↓
                             [MARTS] (2 tables)
                                   ↓
                        [DASHBOARDS & BI TOOLS]
    ```
    """)
