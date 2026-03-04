"""
Olist Data Warehouse Dashboard
Multi-page Streamlit application for analyzing business metrics
"""

import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import psycopg2
from pathlib import Path
from datetime import datetime
import os

# Configure page
st.set_page_config(
    page_title="Olist Data Warehouse Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
    }
    .header {
        color: #1f77b4;
        font-size: 28px;
        font-weight: bold;
        margin-bottom: 20px;
    }
    </style>
    """, unsafe_allow_html=True)

# Load environment variables
def load_env_config():
    """Load database config from .env file"""
    env_file = Path(__file__).parent / '.env'
    env_vars = {}
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip()
    return env_vars

@st.cache_resource
def get_db_connection():
    """Create and cache database connection"""
    config = load_env_config()
    try:
        conn = psycopg2.connect(
            host=config.get('DB_HOST', 'localhost'),
            port=int(config.get('DB_PORT', 5432)),
            database=config.get('DB_NAME', 'olist_datawarehouse'),
            user=config.get('DB_USER', 'postgres'),
            password=config.get('DB_PASSWORD', '')
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

def fetch_data(query):
    """Execute query and return DataFrame"""
    try:
        conn = get_db_connection()
        if conn:
            df = pd.read_sql_query(query, conn)
            return df
        return None
    except Exception as e:
        st.error(f"Query execution failed: {e}")
        return None

# Sidebar
with st.sidebar:
    st.title("🎯 Navigation")
    selected = option_menu(
        menu_title=None,
        options=["📈 Monthly Sales Dashboard", "🛍️ Product Performance Dashboard", "🎯 Customer Segmentation", "📊 Data Overview"],
        # icons=["graph-up", "shop", "people", "bar-chart"],
        menu_icon="cast",
        default_index=0,
        styles={
            "container": {"padding": "0!important", "background-color": "#333333"},
            "icon": {"color": "#1f77b4", "font-size": "20px"},
            "nav-link": {"font-size": "16px", "text-align": "left", "margin": "0px", "--hover-color": "#e0e0e0"},
            "nav-link-selected": {"background-color": "#27ae60", "color": "#ffffff"},
        }
    )
    
    st.divider()
    st.info("💡 Select a dashboard from the menu above to start analyzing data")

# Page routing
if selected == "📈 Monthly Sales Dashboard":
    from app_pages import monthly_sales_dashboard
    monthly_sales_dashboard.show()

elif selected == "🛍️ Product Performance Dashboard":
    from app_pages import product_performance_dashboard
    product_performance_dashboard.show()

elif selected == "🎯 Customer Segmentation":
    from app_pages import customer_segmentation_dashboard
    customer_segmentation_dashboard.main()

elif selected == "📊 Data Overview":
    from app_pages import data_overview
    data_overview.show()

# Footer
st.divider()
st.markdown("""
    <div style='text-align: center; color: gray; font-size: 12px; padding: 20px;'>
        <p>Olist E-commerce Data Warehouse Dashboard</p>
        <p>Powered by Streamlit & Plotly | Data Source: PostgreSQL</p>
        <p>Last Updated: """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """</p>
    </div>
    """, unsafe_allow_html=True)
