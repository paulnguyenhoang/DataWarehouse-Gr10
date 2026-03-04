"""
Product Performance Dashboard - Product Analytics
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
def fetch_product_performance():
    """Fetch product performance data from marts"""
    query = """
    SELECT 
        app.product_sk,
        dp.product_id,
        dp.product_category_name,
        dp.product_category_english,
        app.last_12_months_revenue,
        app.last_12_months_orders,
        app.last_12_months_items,
        app.avg_price,
        app.avg_review_score,
        app.total_reviews,
        app.revenue_rank,
        app.volume_rank
    FROM marts.agg_product_performance app
    JOIN dimensions.dim_product dp ON app.product_sk = dp.product_sk
    WHERE app.revenue_rank <= 50
    ORDER BY app.revenue_rank ASC
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
    """Display product performance dashboard"""
    
    st.markdown('<div class="header">🛍️ Product Performance Analysis</div>', unsafe_allow_html=True)
    st.markdown("Analyze top performing products, revenue rankings, and customer reviews")
    
    # Fetch data
    df = fetch_product_performance()
    
    if df is None or df.empty:
        st.warning("⚠️ No data available. Please run ETL pipeline first.")
        return
    
    # Key metrics
    st.subheader("🎯 Key Product Metrics")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_products = len(df)
        st.metric("Top Products (Ranked)", f"{total_products:,.0f}")
    
    with col2:
        total_revenue = df['last_12_months_revenue'].sum()
        st.metric("Total Revenue", f"R$ {total_revenue:,.2f}")
    
    with col3:
        total_orders = df['last_12_months_orders'].sum()
        st.metric("Total Orders", f"{total_orders:,.0f}")
    
    with col4:
        avg_rating = df['avg_review_score'].mean()
        st.metric("Avg Rating", f"⭐ {avg_rating:.2f}/5.0")
    
    with col5:
        total_reviews = df['total_reviews'].sum()
        st.metric("Total Reviews", f"{total_reviews:,.0f}")
    
    st.divider()
    
    # Filters
    col1, col2 = st.columns(2)
    
    with col1:
        selected_rank = st.slider(
            "Select top N products to display",
            min_value=10,
            max_value=50,
            value=20,
            step=5
        )
    
    with col2:
        min_rating = st.slider(
            "Minimum review rating",
            min_value=0.0,
            max_value=5.0,
            value=0.0,
            step=0.5
        )
    
    # Filter data
    df_filtered = df[(df['revenue_rank'] <= selected_rank) & (df['avg_review_score'] >= min_rating)]
    
    # Top Products by Revenue
    st.subheader("💰 Top Products by Revenue")
    
    fig_revenue = go.Figure()
    
    df_top = df_filtered.nlargest(15, 'last_12_months_revenue')
    
    fig_revenue.add_trace(go.Bar(
        y=df_top['product_category_english'],
        x=df_top['last_12_months_revenue'],
        orientation='h',
        marker=dict(
            color=df_top['avg_review_score'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="Avg Rating")
        ),
        text=[f"R$ {x:,.0f}" for x in df_top['last_12_months_revenue']],
        textposition='outside',
        hovertemplate="<b>%{y}</b><br>Revenue: R$ %{x:,.2f}<br>Rating: %{marker.color:.2f}/5.0<extra></extra>"
    ))
    
    fig_revenue.update_layout(
        title="Top 15 Products by Revenue (last 12 months)",
        xaxis_title="Revenue (R$)",
        height=500,
        template='plotly_white',
        showlegend=False,
        yaxis=dict(autorange="reversed")
    )
    
    st.plotly_chart(fig_revenue, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    # Top Products by Orders
    with col1:
        st.subheader("📦 Top Products by Orders")
        
        df_orders = df_filtered.nlargest(10, 'last_12_months_orders')
        
        fig_orders = px.bar(
            df_orders,
            x='last_12_months_orders',
            y='product_category_english',
            orientation='h',
            color='avg_review_score',
            color_continuous_scale='Viridis',
            title="Top 10 Products by Order Count",
            labels={'last_12_months_orders': 'Orders', 'product_category_english': 'Product'}
        )
        
        fig_orders.update_layout(
            height=400,
            template='plotly_white',
            showlegend=True,
            yaxis=dict(autorange="reversed")
        )
        
        st.plotly_chart(fig_orders, use_container_width=True)
    
    # Average Price vs Average Rating
    with col2:
        st.subheader("💵 Price vs Rating Analysis")
        
        fig_scatter = px.scatter(
            df_filtered,
            x='avg_price',
            y='avg_review_score',
            size='last_12_months_orders',
            color='last_12_months_revenue',
            hover_data=['product_category_english'],
            color_continuous_scale='Viridis',
            title="Product Price vs Customer Rating",
            labels={'avg_price': 'Average Price (R$)', 'avg_review_score': 'Average Rating'},
            size_max=30
        )
        
        fig_scatter.update_layout(
            height=400,
            template='plotly_white',
            hovermode='closest'
        )
        
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    # Review Distribution
    with col1:
        st.subheader("⭐ Customer Review Distribution")
        
        fig_rating = go.Figure()
        
        rating_bins = pd.cut(df_filtered['avg_review_score'], bins=[0, 1, 2, 3, 4, 5])
        rating_counts = rating_bins.value_counts().sort_index()
        
        fig_rating.add_trace(go.Bar(
            x=['1.0-2.0', '2.0-3.0', '3.0-4.0', '4.0-5.0', '5.0'],
            y=rating_counts.values,
            marker=dict(color=['#d62728', '#ff7f0e', '#ffbb78', '#2ca02c', '#1f77b4'])
        ))
        
        fig_rating.update_layout(
            title="Products by Rating Range",
            xaxis_title="Rating Range",
            yaxis_title="Number of Products",
            height=400,
            template='plotly_white',
            showlegend=False
        )
        
        st.plotly_chart(fig_rating, use_container_width=True)
    
    # Revenue vs Order Volume
    with col2:
        st.subheader("📊 Revenue vs Volume Distribution")
        
        df_filtered['revenue_per_order'] = df_filtered['last_12_months_revenue'] / (df_filtered['last_12_months_orders'] + 1)
        
        fig_bubble = go.Figure()
        
        fig_bubble.add_trace(go.Scatter(
            x=df_filtered['last_12_months_orders'],
            y=df_filtered['revenue_per_order'],
            mode='markers',
            marker=dict(
                size=df_filtered['total_reviews'] / 5 + 5,
                color=df_filtered['avg_review_score'],
                colorscale='Viridis',
                showscale=True,
                colorbar=dict(title="Avg Rating"),
                line=dict(width=1, color='white')
            ),
            text=df_filtered['product_category_english'],
            hovertemplate="<b>%{text}</b><br>Orders: %{x:,.0f}<br>Revenue/Order: R$ %{y:,.2f}<extra></extra>"
        ))
        
        fig_bubble.update_layout(
            title="Orders vs Revenue Per Order (bubble size = reviews)",
            xaxis_title="Number of Orders",
            yaxis_title="Revenue per Order (R$)",
            height=400,
            template='plotly_white',
            hovermode='closest'
        )
        
        st.plotly_chart(fig_bubble, use_container_width=True)
    
    # Detailed Product Table
    st.subheader("📋 Top Products Details")
    
    display_df = df_filtered[[
        'revenue_rank', 'product_category_english', 'last_12_months_revenue',
        'last_12_months_orders', 'avg_price', 'avg_review_score', 'total_reviews'
    ]].rename(columns={
        'revenue_rank': 'Rank',
        'product_category_english': 'Product',
        'last_12_months_revenue': 'Revenue (R$)',
        'last_12_months_orders': 'Orders',
        'avg_price': 'Avg Price (R$)',
        'avg_review_score': 'Rating',
        'total_reviews': 'Reviews'
    })
    
    # Format numeric columns
    display_df['Revenue (R$)'] = display_df['Revenue (R$)'].apply(lambda x: f"R$ {x:,.2f}")
    display_df['Avg Price (R$)'] = display_df['Avg Price (R$)'].apply(lambda x: f"R$ {x:,.2f}")
    display_df['Rating'] = display_df['Rating'].apply(lambda x: f"⭐ {x:.2f}")
    
    st.dataframe(
        display_df.sort_values('Rank'),
        use_container_width=True,
        hide_index=True,
        height=500
    )
