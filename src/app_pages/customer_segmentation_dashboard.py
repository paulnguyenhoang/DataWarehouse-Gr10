"""
Customer Segmentation Dashboard Page
=====================================
Streamlit dashboard page for visualizing customer segments
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.customer_segmentation import CustomerSegmentation


def load_segmentation_data():
    """Load segmentation results from CSV"""
    try:
        df = pd.read_csv('outputs/customer_segments.csv')
        return df
    except FileNotFoundError:
        st.warning("⚠️ Segmentation data not found. Please run the pipeline first.")
        return None


def show_overview_metrics(df):
    """Display overview metrics"""
    st.subheader("📊 Segmentation Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Customers",
            value=f"{len(df):,}",
            delta=None
        )
    
    with col2:
        n_clusters = df['kmeans_cluster'].nunique()
        st.metric(
            label="Number of Segments",
            value=n_clusters,
            delta=None
        )
    
    with col3:
        total_revenue = df['monetary_value'].sum()
        st.metric(
            label="Total Revenue",
            value=f"${total_revenue:,.2f}",
            delta=None
        )
    
    with col4:
        avg_order_value = df['avg_order_value'].mean()
        st.metric(
            label="Avg Order Value",
            value=f"${avg_order_value:.2f}",
            delta=None
        )


def plot_rfm_distribution(df):
    """Plot RFM distributions"""
    st.subheader("📈 RFM Distribution Analysis")
    
    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=('Recency Distribution', 'Frequency Distribution', 'Monetary Distribution')
    )
    
    # Recency
    fig.add_trace(
        go.Histogram(x=df['recency_days'], name='Recency', marker_color='skyblue'),
        row=1, col=1
    )
    
    # Frequency
    fig.add_trace(
        go.Histogram(x=df['frequency_orders'], name='Frequency', marker_color='lightgreen'),
        row=1, col=2
    )
    
    # Monetary
    fig.add_trace(
        go.Histogram(x=df['monetary_value'], name='Monetary', marker_color='salmon'),
        row=1, col=3
    )
    
    fig.update_layout(height=400, showlegend=False)
    fig.update_xaxes(title_text="Days", row=1, col=1)
    fig.update_xaxes(title_text="Orders", row=1, col=2)
    fig.update_xaxes(title_text="Value ($)", row=1, col=3)
    
    st.plotly_chart(fig, use_container_width=True)


def plot_cluster_pca(df):
    """Plot PCA visualization of clusters"""
    st.subheader("🎯 Customer Segments (PCA Visualization)")
    
    fig = px.scatter(
        df,
        x='pca_1',
        y='pca_2',
        color='kmeans_cluster',
        hover_data=['customer_unique_id', 'recency_days', 'frequency_orders', 'monetary_value'],
        title='Customer Segments in 2D Space (PCA)',
        labels={'pca_1': 'First Principal Component', 'pca_2': 'Second Principal Component'},
        color_continuous_scale='viridis'
    )
    
    fig.update_traces(marker=dict(size=8, line=dict(width=0.5, color='white')))
    fig.update_layout(height=600)
    
    st.plotly_chart(fig, use_container_width=True)


def plot_segment_comparison(df):
    """Compare segments across RFM metrics"""
    st.subheader("📊 Segment Comparison")
    
    # Aggregate by cluster
    segment_stats = df.groupby('kmeans_cluster').agg({
        'customer_sk': 'count',
        'recency_days': 'mean',
        'frequency_orders': 'mean',
        'monetary_value': 'mean',
        'avg_review_score': 'mean'
    }).reset_index()
    
    segment_stats.columns = ['Cluster', 'Customers', 'Avg_Recency', 'Avg_Frequency', 'Avg_Monetary', 'Avg_Review']
    
    # Create radar chart
    fig = go.Figure()
    
    for cluster in segment_stats['Cluster'].unique():
        cluster_data = segment_stats[segment_stats['Cluster'] == cluster]
        
        # Normalize values for radar chart
        values = [
            1 / (cluster_data['Avg_Recency'].values[0] / 100 + 1),  # Lower recency is better
            cluster_data['Avg_Frequency'].values[0],
            cluster_data['Avg_Monetary'].values[0] / 100,
            cluster_data['Avg_Review'].values[0],
            cluster_data['Customers'].values[0] / 1000
        ]
        
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=['Recency', 'Frequency', 'Monetary', 'Review Score', 'Size'],
            fill='toself',
            name=f'Cluster {cluster}'
        ))
    
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, max(5, segment_stats['Avg_Frequency'].max())])),
        showlegend=True,
        height=500,
        title='Segment Characteristics (Radar Chart)'
    )
    
    st.plotly_chart(fig, use_container_width=True)


def show_segment_details(df):
    """Show detailed segment analysis"""
    st.subheader("🔍 Detailed Segment Analysis")
    
    # Select cluster
    clusters = sorted(df['kmeans_cluster'].unique())
    selected_cluster = st.selectbox("Select Segment:", clusters)
    
    # Filter data
    cluster_data = df[df['kmeans_cluster'] == selected_cluster]
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Customers", f"{len(cluster_data):,}")
    with col2:
        st.metric("Avg Recency", f"{cluster_data['recency_days'].mean():.0f} days")
    with col3:
        st.metric("Avg Frequency", f"{cluster_data['frequency_orders'].mean():.1f}")
    with col4:
        st.metric("Avg Monetary", f"${cluster_data['monetary_value'].mean():.2f}")
    
    # Segment characteristics
    st.write("#### Segment Characteristics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Top states
        st.write("**Top 10 States**")
        top_states = cluster_data['customer_state'].value_counts().head(10)
        fig = px.bar(
            x=top_states.values,
            y=top_states.index,
            orientation='h',
            labels={'x': 'Customers', 'y': 'State'}
        )
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Customer value distribution
        st.write("**Customer Value Distribution**")
        value_dist = cluster_data['customer_value'].value_counts()
        fig = px.pie(
            values=value_dist.values,
            names=value_dist.index,
            title=''
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Show sample customers
    st.write("#### Sample Customers")
    sample_cols = ['customer_unique_id', 'customer_state', 'recency_days', 
                   'frequency_orders', 'monetary_value', 'avg_review_score']
    st.dataframe(cluster_data[sample_cols].head(10), use_container_width=True)


def plot_geographic_distribution(df):
    """Plot geographic distribution"""
    st.subheader("🗺️ Geographic Distribution")
    
    # Customers by state and cluster
    geo_data = df.groupby(['customer_state', 'kmeans_cluster']).size().reset_index(name='count')
    
    fig = px.sunburst(
        geo_data,
        path=['customer_state', 'kmeans_cluster'],
        values='count',
        title='Customers by State and Segment'
    )
    fig.update_layout(height=600)
    
    st.plotly_chart(fig, use_container_width=True)


def show_business_insights(df):
    """Display business insights and recommendations"""
    st.subheader("💡 Business Insights & Recommendations")
    
    for cluster in sorted(df['kmeans_cluster'].unique()):
        cluster_data = df[df['kmeans_cluster'] == cluster]
        
        avg_recency = cluster_data['recency_days'].mean()
        avg_frequency = cluster_data['frequency_orders'].mean()
        avg_monetary = cluster_data['monetary_value'].mean()
        segment_label = cluster_data['segment_label'].iloc[0] if 'segment_label' in cluster_data else f"Cluster {cluster}"
        
        with st.expander(f"📌 {segment_label} ({len(cluster_data):,} customers)"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Key Metrics:**")
                st.write(f"- Avg Recency: {avg_recency:.0f} days")
                st.write(f"- Avg Frequency: {avg_frequency:.1f} orders")
                st.write(f"- Avg Monetary: ${avg_monetary:.2f}")
                st.write(f"- Total Revenue: ${cluster_data['monetary_value'].sum():,.2f}")
            
            with col2:
                st.write("**Recommended Actions:**")
                
                if avg_recency < 60 and avg_frequency > 3 and avg_monetary > 500:
                    st.success("✓ VIP treatment and exclusive offers")
                    st.success("✓ Personal account manager")
                    st.success("✓ Early access to new products")
                elif avg_frequency > 2 and avg_monetary > 300:
                    st.info("✓ Loyalty rewards program")
                    st.info("✓ Referral bonuses")
                    st.info("✓ Birthday discounts")
                elif avg_recency < 90 and avg_frequency <= 2:
                    st.info("✓ Engagement campaigns")
                    st.info("✓ Product recommendations")
                    st.info("✓ Educational content")
                elif avg_recency > 180:
                    st.warning("⚠️ Win-back campaigns")
                    st.warning("⚠️ Survey for feedback")
                    st.warning("⚠️ Special reactivation offers")
                else:
                    st.info("✓ Standard marketing")
                    st.info("✓ Cross-sell opportunities")


def run_new_segmentation():
    """Run new segmentation pipeline"""
    st.subheader("🚀 Run New Segmentation")
    
    st.write("Click the button below to run a new customer segmentation analysis:")
    
    if st.button("Run Segmentation Pipeline"):
        with st.spinner("Running segmentation pipeline... This may take a few minutes."):
            try:
                segmentation = CustomerSegmentation()
                success = segmentation.run_pipeline()
                
                if success:
                    st.success("✅ Segmentation completed successfully!")
                    st.balloons()
                    st.rerun()
                else:
                    st.error("❌ Segmentation failed. Check logs for details.")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")


def main():
    """Main function for customer segmentation dashboard"""
    st.title("🎯 Customer Segmentation Dashboard")
    st.markdown("---")
    
    # Sidebar
    st.sidebar.header("Navigation")
    page = st.sidebar.radio(
        "Select View:",
        ["Overview", "Segment Analysis", "Geographic Distribution", "Business Insights", "Run New Analysis"]
    )
    
    # Load data
    df = load_segmentation_data()
    
    if df is None:
        st.info("👉 Please run the segmentation pipeline first:")
        st.code("python src/pipeline/customer_segmentation.py", language="bash")
        run_new_segmentation()
        return
    
    # Show different pages
    if page == "Overview":
        show_overview_metrics(df)
        st.markdown("---")
        plot_rfm_distribution(df)
        st.markdown("---")
        plot_cluster_pca(df)
        st.markdown("---")
        plot_segment_comparison(df)
        
    elif page == "Segment Analysis":
        show_segment_details(df)
        
    elif page == "Geographic Distribution":
        plot_geographic_distribution(df)
        
    elif page == "Business Insights":
        show_business_insights(df)
        
    elif page == "Run New Analysis":
        run_new_segmentation()
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.info(f"""
    **Dataset Info:**
    - Total Customers: {len(df):,}
    - Segments: {df['kmeans_cluster'].nunique()}
    - Date Range: Latest data
    """)


if __name__ == "__main__":
    main()
