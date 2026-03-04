"""
Monthly Sales Dashboard - Time Series Analysis
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import psycopg2

def get_db_connection():
    """Create database connection"""
    from pathlib import Path
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
def fetch_monthly_sales():
    """Fetch monthly sales data from marts"""
    query = """
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
        avg_items_per_order,
        unique_customers,
        unique_products,
        unique_states
    FROM marts.agg_monthly_sales
    ORDER BY year_month DESC
    """
    
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to fetch data: {e}")
        return None

def show():
    """Display monthly sales dashboard"""
    
    st.markdown('<div class="header">📈 Monthly Sales Analysis</div>', unsafe_allow_html=True)
    st.markdown("Track sales trends, revenue patterns, and customer metrics over time")
    
    # Fetch data
    df = fetch_monthly_sales()
    
    if df is None or df.empty:
        st.warning("⚠️ No data available. Please run ETL pipeline first.")
        return
    
    # Sort by year_month ascending for time series
    df_sorted = df.sort_values('year_month')
    
    # Create year-month string for better display
    df_sorted['date_label'] = df_sorted['year'].astype(str) + '-' + df_sorted['month'].astype(str).str.zfill(2)
    
    # Key metrics
    st.subheader("🎯 Key Metrics Summary")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_orders = df['total_orders'].sum()
        st.metric("Total Orders", f"{total_orders:,.0f}", 
                 delta=f"{df.iloc[0]['total_orders']:,.0f} this month")
    
    with col2:
        total_revenue = df['total_revenue'].sum()
        st.metric("Total Revenue", f"R$ {total_revenue:,.2f}", 
                 delta=f"R$ {df.iloc[0]['total_revenue']:,.2f} this month")
    
    with col3:
        avg_order_value = df['avg_order_value'].mean()
        st.metric("Avg Order Value", f"R$ {avg_order_value:,.2f}",
                 delta=f"{((df.iloc[0]['avg_order_value'] / avg_order_value - 1) * 100):.1f}%" if avg_order_value > 0 else "N/A")
    
    with col4:
        unique_customers = df['unique_customers'].sum()
        st.metric("Unique Customers", f"{unique_customers:,.0f}",
                 delta=f"{df.iloc[0]['unique_customers']:,.0f} this month")
    
    with col5:
        unique_products = df['unique_products'].max()
        st.metric("Products Sold", f"{unique_products:,.0f}")
    
    st.divider()
    
    # Revenue trend
    st.subheader("💰 Revenue Trend")
    
    fig_revenue = go.Figure()
    
    fig_revenue.add_trace(go.Scatter(
        x=df_sorted['date_label'],
        y=df_sorted['total_revenue'],
        mode='lines+markers',
        name='Total Revenue',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=8),
        fill='tozeroy',
        fillcolor='rgba(31, 119, 180, 0.2)'
    ))
    
    fig_revenue.update_layout(
        title="Monthly Revenue Over Time",
        xaxis_title="Period",
        yaxis_title="Revenue (R$)",
        hovermode='x unified',
        height=400,
        template='plotly_white',
        showlegend=False
    )
    
    st.plotly_chart(fig_revenue, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    # Orders & Items
    with col1:
        st.subheader("📦 Orders & Items")
        
        fig_orders = go.Figure()
        
        fig_orders.add_trace(go.Bar(
            x=df_sorted['date_label'],
            y=df_sorted['total_orders'],
            name='Orders',
            marker=dict(color='#1f77b4')
        ))
        
        fig_orders.add_trace(go.Scatter(
            x=df_sorted['date_label'],
            y=df_sorted['total_items'],
            mode='lines+markers',
            name='Items',
            line=dict(color='#ff7f0e', width=3),
            yaxis='y2'
        ))
        
        fig_orders.update_layout(
            title="Orders vs Items Trend",
            xaxis_title="Period",
            yaxis=dict(title="Orders", side='left'),
            yaxis2=dict(title="Items", overlaying='y', side='right'),
            hovermode='x unified',
            height=400,
            template='plotly_white'
        )
        
        st.plotly_chart(fig_orders, use_container_width=True)
    
    # Customers & Geographic
    with col2:
        st.subheader("👥 Customer & Geographic Metrics")
        
        fig_geo = go.Figure()
        
        fig_geo.add_trace(go.Bar(
            x=df_sorted['date_label'],
            y=df_sorted['unique_customers'],
            name='Unique Customers',
            marker=dict(color='#2ca02c')
        ))
        
        fig_geo.add_trace(go.Scatter(
            x=df_sorted['date_label'],
            y=df_sorted['unique_states'],
            mode='lines+markers',
            name='States',
            line=dict(color='#d62728', width=3),
            yaxis='y2'
        ))
        
        fig_geo.update_layout(
            title="Customers vs Geographic Reach",
            xaxis_title="Period",
            yaxis=dict(title="Customers", side='left'),
            yaxis2=dict(title="States", overlaying='y', side='right'),
            hovermode='x unified',
            height=400,
            template='plotly_white'
        )
        
        st.plotly_chart(fig_geo, use_container_width=True)
    
    # Average metrics
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Average Order Value & Items per Order")
        
        fig_avg = go.Figure()
        
        fig_avg.add_trace(go.Bar(
            x=df_sorted['date_label'],
            y=df_sorted['avg_order_value'],
            name='Avg Order Value',
            marker=dict(color='#9467bd')
        ))
        
        fig_avg.add_trace(go.Scatter(
            x=df_sorted['date_label'],
            y=df_sorted['avg_items_per_order'],
            mode='lines+markers',
            name='Avg Items/Order',
            line=dict(color='#e377c2', width=3),
            yaxis='y2'
        ))
        
        fig_avg.update_layout(
            title="Average Metrics Over Time",
            xaxis_title="Period",
            yaxis=dict(title="Avg Order Value (R$)", side='left'),
            yaxis2=dict(title="Items per Order", overlaying='y', side='right'),
            hovermode='x unified',
            height=400,
            template='plotly_white'
        )
        
        st.plotly_chart(fig_avg, use_container_width=True)
    
    # Freight analysis
    with col2:
        st.subheader("🚚 Freight Costs Analysis")
        
        df_sorted['freight_pct'] = (df_sorted['total_freight'] / df_sorted['total_revenue'] * 100).round(2)
        
        fig_freight = go.Figure()
        
        fig_freight.add_trace(go.Bar(
            x=df_sorted['date_label'],
            y=df_sorted['total_freight'],
            name='Freight Value',
            marker=dict(color='#17becf')
        ))
        
        fig_freight.add_trace(go.Scatter(
            x=df_sorted['date_label'],
            y=df_sorted['freight_pct'],
            mode='lines+markers',
            name='Freight % of Revenue',
            line=dict(color='#7f7f7f', width=3),
            yaxis='y2'
        ))
        
        fig_freight.update_layout(
            title="Freight Costs & Percentage",
            xaxis_title="Period",
            yaxis=dict(title="Freight Value (R$)", side='left'),
            yaxis2=dict(title="Percentage (%)", overlaying='y', side='right'),
            hovermode='x unified',
            height=400,
            template='plotly_white'
        )
        
        st.plotly_chart(fig_freight, use_container_width=True)
    
    # Data table
    st.subheader("📋 Detailed Monthly Data")
    
    display_columns = ['year_month', 'total_orders', 'total_items', 'total_revenue', 
                       'avg_order_value', 'unique_customers', 'unique_products', 'unique_states']
    
    st.dataframe(
        df.sort_values('year_month', ascending=False)[display_columns],
        use_container_width=True,
        hide_index=True
    )
