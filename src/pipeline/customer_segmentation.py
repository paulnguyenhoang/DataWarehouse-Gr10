"""
Customer Segmentation Pipeline using RFM Analysis
==================================================
This module performs customer segmentation using:
- RFM (Recency, Frequency, Monetary) feature engineering
- K-Means clustering with optimal k selection
- DBSCAN clustering
- Silhouette score evaluation
- PCA visualization
"""

import os
import sys
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pickle
import warnings
warnings.filterwarnings('ignore')

# Machine Learning imports
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans, DBSCAN
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, davies_bouldin_score

# Visualization imports
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import Ellipse
import matplotlib.patches as mpatches

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}


class CustomerSegmentation:
    """Customer Segmentation using RFM Analysis and Clustering"""
    
    def __init__(self, reference_date=None):
        """
        Initialize Customer Segmentation
        
        Args:
            reference_date: Reference date for recency calculation (default: today)
        """
        self.reference_date = reference_date or datetime.now()
        self.conn = None
        self.rfm_data = None
        self.rfm_scaled = None
        self.scaler = StandardScaler()
        self.kmeans_model = None
        self.dbscan_model = None
        self.optimal_k = None
        self.pca = PCA(n_components=2)
        self.pca_data = None
        
    def connect_db(self):
        """Establish database connection"""
        try:
            print("🔗 Connecting to database...")
            self.conn = psycopg2.connect(**DB_CONFIG)
            print("✅ Connected successfully!")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def close_db(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("🔌 Database connection closed")
    
    def extract_rfm_data(self):
        """
        Extract RFM data from data warehouse
        
        Returns:
            DataFrame with RFM features
        """
        query = """
        WITH customer_orders AS (
            SELECT 
                dc.customer_sk,
                dc.customer_id,
                dc.customer_unique_id,
                dc.customer_state,
                dc.customer_city,
                dc.customer_region,
                dos.order_id,
                dos.order_purchase_timestamp,
                foi.total_item_value,
                foi.item_price,
                foi.freight_value,
                fp.payment_value
            FROM dimensions.dim_customer dc
            INNER JOIN facts.fact_order_items foi ON dc.customer_sk = foi.customer_sk
            INNER JOIN dimensions.dim_order_status dos ON foi.order_status_sk = dos.order_status_sk
            LEFT JOIN facts.fact_payments fp ON foi.order_id = fp.order_id
            WHERE dos.order_status = 'delivered'
                AND dos.order_purchase_timestamp IS NOT NULL
        ),
        customer_reviews AS (
            SELECT 
                fr.customer_sk,
                AVG(fr.review_score) as avg_review_score,
                COUNT(fr.review_id) as total_reviews
            FROM facts.fact_reviews fr
            GROUP BY fr.customer_sk
        ),
        rfm_metrics AS (
            SELECT 
                co.customer_sk,
                co.customer_unique_id,
                co.customer_state,
                co.customer_city,
                co.customer_region,
                
                -- Recency: Days since last purchase
                EXTRACT(DAY FROM CURRENT_DATE - MAX(co.order_purchase_timestamp)) as recency_days,
                
                -- Frequency: Total number of orders
                COUNT(DISTINCT co.order_id) as frequency_orders,
                
                -- Monetary: Total spending
                COALESCE(SUM(co.payment_value), SUM(co.total_item_value)) as monetary_value,
                
                -- Additional metrics
                AVG(co.total_item_value) as avg_order_value,
                SUM(co.freight_value) as total_freight,
                AVG(co.freight_value) as avg_freight_value,
                COUNT(co.order_id) as total_items_purchased,
                
                -- Time-based features
                MIN(co.order_purchase_timestamp) as first_purchase_date,
                MAX(co.order_purchase_timestamp) as last_purchase_date,
                EXTRACT(DAY FROM MAX(co.order_purchase_timestamp) - MIN(co.order_purchase_timestamp)) as customer_lifetime_days,
                
                -- Customer satisfaction
                COALESCE(cr.avg_review_score, 3.0) as avg_review_score,
                COALESCE(cr.total_reviews, 0) as total_reviews
                
            FROM customer_orders co
            LEFT JOIN customer_reviews cr ON co.customer_sk = cr.customer_sk
            GROUP BY 
                co.customer_sk,
                co.customer_unique_id,
                co.customer_state,
                co.customer_city,
                co.customer_region,
                cr.avg_review_score,
                cr.total_reviews
        )
        SELECT 
            *,
            -- Calculate purchase frequency (orders per day active)
            CASE 
                WHEN customer_lifetime_days > 0 
                THEN CAST(frequency_orders AS FLOAT) / NULLIF(customer_lifetime_days, 0)
                ELSE 0 
            END as purchase_frequency_rate
        FROM rfm_metrics
        WHERE monetary_value > 0
        ORDER BY monetary_value DESC;
        """
        
        try:
            print("\n📊 Extracting RFM data from warehouse...")
            self.rfm_data = pd.read_sql_query(query, self.conn)
            
            # Handle missing values
            self.rfm_data['avg_review_score'].fillna(3.0, inplace=True)
            self.rfm_data['purchase_frequency_rate'].fillna(0, inplace=True)
            
            print(f"✅ Extracted {len(self.rfm_data)} customers")
            print(f"📋 Columns: {list(self.rfm_data.columns)}")
            
            return self.rfm_data
            
        except Exception as e:
            print(f"❌ Error extracting data: {e}")
            return None
    
    def engineer_features(self):
        """
        Engineer additional features for segmentation
        """
        print("\n🔧 Engineering features...")
        
        # Calculate RFM scores (1-5 scale)
        self.rfm_data['recency_score'] = pd.qcut(
            self.rfm_data['recency_days'], 
            q=5, 
            labels=[5, 4, 3, 2, 1],  # Lower recency = higher score
            duplicates='drop'
        )
        
        self.rfm_data['frequency_score'] = pd.qcut(
            self.rfm_data['frequency_orders'].rank(method='first'),
            q=5,
            labels=[1, 2, 3, 4, 5],
            duplicates='drop'
        )
        
        self.rfm_data['monetary_score'] = pd.qcut(
            self.rfm_data['monetary_value'].rank(method='first'),
            q=5,
            labels=[1, 2, 3, 4, 5],
            duplicates='drop'
        )
        
        # Convert to numeric
        self.rfm_data['recency_score'] = pd.to_numeric(self.rfm_data['recency_score'])
        self.rfm_data['frequency_score'] = pd.to_numeric(self.rfm_data['frequency_score'])
        self.rfm_data['monetary_score'] = pd.to_numeric(self.rfm_data['monetary_score'])
        
        # Calculate RFM score
        self.rfm_data['rfm_score'] = (
            self.rfm_data['recency_score'] + 
            self.rfm_data['frequency_score'] + 
            self.rfm_data['monetary_score']
        )
        
        # Customer value categories
        self.rfm_data['customer_value'] = pd.cut(
            self.rfm_data['rfm_score'],
            bins=[0, 6, 9, 12, 15],
            labels=['Low Value', 'Medium Value', 'High Value', 'VIP']
        )
        
        print("✅ Features engineered successfully")
        
        return self.rfm_data
    
    def prepare_features_for_clustering(self):
        """
        Prepare and scale features for clustering
        
        Returns:
            Scaled feature matrix
        """
        print("\n🎯 Preparing features for clustering...")
        
        # Select features for clustering
        clustering_features = [
            'recency_days',
            'frequency_orders',
            'monetary_value',
            'avg_order_value',
            'avg_review_score',
            'purchase_frequency_rate',
            'customer_lifetime_days'
        ]
        
        X = self.rfm_data[clustering_features].copy()
        
        # Handle any remaining NaN values
        X = X.fillna(X.median())
        
        # Scale features
        self.rfm_scaled = self.scaler.fit_transform(X)
        
        print(f"✅ Prepared {self.rfm_scaled.shape[0]} samples with {self.rfm_scaled.shape[1]} features")
        
        return self.rfm_scaled
    
    def find_optimal_k(self, max_k=10):
        """
        Find optimal number of clusters using Elbow Method and Silhouette Score
        
        Args:
            max_k: Maximum number of clusters to test
            
        Returns:
            Optimal k value
        """
        print(f"\n🔍 Finding optimal k (testing k=2 to k={max_k})...")
        
        inertias = []
        silhouette_scores = []
        k_range = range(2, max_k + 1)
        
        for k in k_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(self.rfm_scaled)
            
            inertias.append(kmeans.inertia_)
            silhouette_scores.append(silhouette_score(self.rfm_scaled, labels))
            
            print(f"  k={k}: Inertia={kmeans.inertia_:.2f}, Silhouette={silhouette_scores[-1]:.3f}")
        
        # Find optimal k based on highest silhouette score
        self.optimal_k = k_range[np.argmax(silhouette_scores)]
        
        print(f"\n✅ Optimal k = {self.optimal_k} (Silhouette Score: {max(silhouette_scores):.3f})")
        
        # Plot elbow curve and silhouette scores
        self._plot_optimal_k(k_range, inertias, silhouette_scores)
        
        return self.optimal_k
    
    def _plot_optimal_k(self, k_range, inertias, silhouette_scores):
        """Plot elbow curve and silhouette scores"""
        fig, axes = plt.subplots(1, 2, figsize=(15, 5))
        
        # Elbow curve
        axes[0].plot(k_range, inertias, 'bo-', linewidth=2, markersize=8)
        axes[0].set_xlabel('Number of Clusters (k)', fontsize=12)
        axes[0].set_ylabel('Inertia', fontsize=12)
        axes[0].set_title('Elbow Method - Inertia vs k', fontsize=14, fontweight='bold')
        axes[0].grid(True, alpha=0.3)
        axes[0].axvline(x=self.optimal_k, color='r', linestyle='--', label=f'Optimal k={self.optimal_k}')
        axes[0].legend()
        
        # Silhouette scores
        axes[1].plot(k_range, silhouette_scores, 'go-', linewidth=2, markersize=8)
        axes[1].set_xlabel('Number of Clusters (k)', fontsize=12)
        axes[1].set_ylabel('Silhouette Score', fontsize=12)
        axes[1].set_title('Silhouette Score vs k', fontsize=14, fontweight='bold')
        axes[1].grid(True, alpha=0.3)
        axes[1].axvline(x=self.optimal_k, color='r', linestyle='--', label=f'Optimal k={self.optimal_k}')
        axes[1].legend()
        
        plt.tight_layout()
        plt.savefig('outputs/optimal_k_selection.png', dpi=300, bbox_inches='tight')
        print("📊 Saved: outputs/optimal_k_selection.png")
        plt.close()
    
    def fit_kmeans(self, n_clusters=None):
        """
        Fit K-Means clustering model
        
        Args:
            n_clusters: Number of clusters (uses optimal_k if None)
            
        Returns:
            Cluster labels
        """
        if n_clusters is None:
            n_clusters = self.optimal_k if self.optimal_k else 4
        
        print(f"\n🎯 Fitting K-Means with k={n_clusters}...")
        
        self.kmeans_model = KMeans(
            n_clusters=n_clusters,
            random_state=42,
            n_init=10,
            max_iter=300
        )
        
        kmeans_labels = self.kmeans_model.fit_predict(self.rfm_scaled)
        self.rfm_data['kmeans_cluster'] = kmeans_labels
        
        # Calculate metrics
        silhouette_avg = silhouette_score(self.rfm_scaled, kmeans_labels)
        davies_bouldin = davies_bouldin_score(self.rfm_scaled, kmeans_labels)
        
        print(f"✅ K-Means clustering completed")
        print(f"   Silhouette Score: {silhouette_avg:.3f}")
        print(f"   Davies-Bouldin Index: {davies_bouldin:.3f}")
        
        # Cluster distribution
        print(f"\n📊 Cluster Distribution:")
        cluster_counts = self.rfm_data['kmeans_cluster'].value_counts().sort_index()
        for cluster, count in cluster_counts.items():
            percentage = (count / len(self.rfm_data)) * 100
            print(f"   Cluster {cluster}: {count} customers ({percentage:.1f}%)")
        
        return kmeans_labels
    
    def fit_dbscan(self, eps=0.5, min_samples=5):
        """
        Fit DBSCAN clustering model
        
        Args:
            eps: Maximum distance between samples
            min_samples: Minimum samples in neighborhood
            
        Returns:
            Cluster labels
        """
        print(f"\n🎯 Fitting DBSCAN (eps={eps}, min_samples={min_samples})...")
        
        self.dbscan_model = DBSCAN(eps=eps, min_samples=min_samples)
        dbscan_labels = self.dbscan_model.fit_predict(self.rfm_scaled)
        self.rfm_data['dbscan_cluster'] = dbscan_labels
        
        # Calculate metrics (excluding noise points)
        mask = dbscan_labels != -1
        n_clusters = len(set(dbscan_labels)) - (1 if -1 in dbscan_labels else 0)
        n_noise = list(dbscan_labels).count(-1)
        
        print(f"✅ DBSCAN clustering completed")
        print(f"   Number of clusters: {n_clusters}")
        print(f"   Noise points: {n_noise} ({(n_noise/len(dbscan_labels))*100:.1f}%)")
        
        if n_clusters > 1 and mask.sum() > 1:
            silhouette_avg = silhouette_score(self.rfm_scaled[mask], dbscan_labels[mask])
            print(f"   Silhouette Score: {silhouette_avg:.3f}")
        
        # Cluster distribution
        print(f"\n📊 Cluster Distribution:")
        cluster_counts = pd.Series(dbscan_labels).value_counts().sort_index()
        for cluster, count in cluster_counts.items():
            percentage = (count / len(self.rfm_data)) * 100
            label = "Noise" if cluster == -1 else f"Cluster {cluster}"
            print(f"   {label}: {count} customers ({percentage:.1f}%)")
        
        return dbscan_labels
    
    def apply_pca(self):
        """
        Apply PCA for dimensionality reduction to 2D
        
        Returns:
            PCA-transformed data
        """
        print("\n📉 Applying PCA for visualization...")
        
        self.pca_data = self.pca.fit_transform(self.rfm_scaled)
        
        # Add PCA components to dataframe
        self.rfm_data['pca_1'] = self.pca_data[:, 0]
        self.rfm_data['pca_2'] = self.pca_data[:, 1]
        
        # Explained variance
        explained_var = self.pca.explained_variance_ratio_
        print(f"✅ PCA completed")
        print(f"   PC1 explains {explained_var[0]*100:.2f}% of variance")
        print(f"   PC2 explains {explained_var[1]*100:.2f}% of variance")
        print(f"   Total: {sum(explained_var)*100:.2f}%")
        
        return self.pca_data
    
    def visualize_clusters(self):
        """
        Visualize clusters using PCA components
        """
        print("\n📊 Creating cluster visualizations...")
        
        fig, axes = plt.subplots(1, 2, figsize=(18, 7))
        
        # K-Means visualization
        scatter1 = axes[0].scatter(
            self.rfm_data['pca_1'],
            self.rfm_data['pca_2'],
            c=self.rfm_data['kmeans_cluster'],
            cmap='viridis',
            s=50,
            alpha=0.6,
            edgecolors='w',
            linewidth=0.5
        )
        axes[0].set_xlabel('First Principal Component', fontsize=12)
        axes[0].set_ylabel('Second Principal Component', fontsize=12)
        axes[0].set_title('K-Means Clustering', fontsize=14, fontweight='bold')
        axes[0].grid(True, alpha=0.3)
        plt.colorbar(scatter1, ax=axes[0], label='Cluster')
        
        # Add cluster centers for K-Means
        if self.kmeans_model:
            centers_pca = self.pca.transform(self.kmeans_model.cluster_centers_)
            axes[0].scatter(
                centers_pca[:, 0],
                centers_pca[:, 1],
                c='red',
                s=300,
                alpha=0.8,
                marker='*',
                edgecolors='black',
                linewidth=2,
                label='Centroids'
            )
            axes[0].legend()
        
        # DBSCAN visualization
        scatter2 = axes[1].scatter(
            self.rfm_data['pca_1'],
            self.rfm_data['pca_2'],
            c=self.rfm_data['dbscan_cluster'],
            cmap='plasma',
            s=50,
            alpha=0.6,
            edgecolors='w',
            linewidth=0.5
        )
        axes[1].set_xlabel('First Principal Component', fontsize=12)
        axes[1].set_ylabel('Second Principal Component', fontsize=12)
        axes[1].set_title('DBSCAN Clustering', fontsize=14, fontweight='bold')
        axes[1].grid(True, alpha=0.3)
        plt.colorbar(scatter2, ax=axes[1], label='Cluster')
        
        plt.tight_layout()
        plt.savefig('outputs/customer_segmentation_pca.png', dpi=300, bbox_inches='tight')
        print("✅ Saved: outputs/customer_segmentation_pca.png")
        plt.close()
    
    def analyze_segments(self):
        """
        Analyze characteristics of each segment
        """
        print("\n📊 Analyzing Customer Segments (K-Means)...\n")
        
        segment_analysis = self.rfm_data.groupby('kmeans_cluster').agg({
            'customer_sk': 'count',
            'recency_days': 'mean',
            'frequency_orders': 'mean',
            'monetary_value': ['mean', 'sum'],
            'avg_order_value': 'mean',
            'avg_review_score': 'mean',
            'customer_lifetime_days': 'mean',
            'purchase_frequency_rate': 'mean'
        }).round(2)
        
        segment_analysis.columns = [
            'Count', 'Avg_Recency', 'Avg_Frequency', 
            'Avg_Monetary', 'Total_Revenue', 'Avg_Order_Value',
            'Avg_Review', 'Avg_Lifetime', 'Purchase_Rate'
        ]
        
        print(segment_analysis.to_string())
        
        # Label segments based on characteristics
        self._label_segments()
        
        return segment_analysis
    
    def _label_segments(self):
        """
        Assign business labels to clusters
        """
        print("\n🏷️  Segment Labels:\n")
        
        for cluster in sorted(self.rfm_data['kmeans_cluster'].unique()):
            cluster_data = self.rfm_data[self.rfm_data['kmeans_cluster'] == cluster]
            
            avg_recency = cluster_data['recency_days'].mean()
            avg_frequency = cluster_data['frequency_orders'].mean()
            avg_monetary = cluster_data['monetary_value'].mean()
            
            # Simple labeling logic
            if avg_recency < 60 and avg_frequency > 3 and avg_monetary > 500:
                label = "🌟 Champions (VIP)"
            elif avg_frequency > 2 and avg_monetary > 300:
                label = "💎 Loyal Customers"
            elif avg_recency < 90 and avg_frequency <= 2:
                label = "🌱 Potential Loyalists"
            elif avg_recency < 60 and avg_frequency == 1:
                label = "🆕 New Customers"
            elif avg_recency > 180:
                label = "😴 Lost Customers"
            elif avg_recency > 90:
                label = "⚠️  At Risk"
            else:
                label = "📊 Standard Customers"
            
            self.rfm_data.loc[self.rfm_data['kmeans_cluster'] == cluster, 'segment_label'] = label
            
            count = len(cluster_data)
            percentage = (count / len(self.rfm_data)) * 100
            print(f"Cluster {cluster}: {label}")
            print(f"  Size: {count} ({percentage:.1f}%)")
            print(f"  Avg Recency: {avg_recency:.0f} days")
            print(f"  Avg Frequency: {avg_frequency:.1f} orders")
            print(f"  Avg Monetary: ${avg_monetary:.2f}\n")
    
    def save_model(self, model_path='outputs/kmeans_model.pkl'):
        """
        Save K-Means model, scaler, and PCA to pickle file
        
        Args:
            model_path: Path to save the model
        """
        print(f"\n💾 Saving K-Means model to {model_path}...")
        
        if self.kmeans_model is None:
            print("❌ No K-Means model to save!")
            return False
        
        # Create model package
        model_package = {
            'kmeans_model': self.kmeans_model,
            'scaler': self.scaler,
            'pca': self.pca,
            'optimal_k': self.optimal_k,
            'feature_names': [
                'recency_days',
                'frequency_orders',
                'monetary_value',
                'avg_order_value',
                'avg_review_score',
                'purchase_frequency_rate',
                'customer_lifetime_days'
            ],
            'metadata': {
                'created_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'n_clusters': self.kmeans_model.n_clusters,
                'n_samples': len(self.rfm_data),
                'silhouette_score': silhouette_score(self.rfm_scaled, self.kmeans_model.labels_),
                'davies_bouldin_score': davies_bouldin_score(self.rfm_scaled, self.kmeans_model.labels_)
            }
        }
        
        # Save to pickle file
        try:
            with open(model_path, 'wb') as f:
                pickle.dump(model_package, f)
            
            print(f"✅ Model saved successfully!")
            print(f"   Model: K-Means (k={model_package['metadata']['n_clusters']})")
            print(f"   Silhouette Score: {model_package['metadata']['silhouette_score']:.3f}")
            print(f"   Davies-Bouldin Index: {model_package['metadata']['davies_bouldin_score']:.3f}")
            print(f"   File size: {os.path.getsize(model_path) / 1024:.2f} KB")
            return True
            
        except Exception as e:
            print(f"❌ Error saving model: {e}")
            return False
    
    def load_model(self, model_path='outputs/kmeans_model.pkl'):
        """
        Load K-Means model, scaler, and PCA from pickle file
        
        Args:
            model_path: Path to load the model from
            
        Returns:
            True if successful, False otherwise
        """
        print(f"\n📂 Loading K-Means model from {model_path}...")
        
        try:
            with open(model_path, 'rb') as f:
                model_package = pickle.load(f)
            
            self.kmeans_model = model_package['kmeans_model']
            self.scaler = model_package['scaler']
            self.pca = model_package['pca']
            self.optimal_k = model_package['optimal_k']
            
            print(f"✅ Model loaded successfully!")
            print(f"   Created: {model_package['metadata']['created_date']}")
            print(f"   Model: K-Means (k={model_package['metadata']['n_clusters']})")
            print(f"   Silhouette Score: {model_package['metadata']['silhouette_score']:.3f}")
            print(f"   Trained on {model_package['metadata']['n_samples']} samples")
            return True
            
        except FileNotFoundError:
            print(f"❌ Model file not found: {model_path}")
            return False
        except Exception as e:
            print(f"❌ Error loading model: {e}")
            return False
    
    def predict_segment(self, customer_features):
        """
        Predict segment for new customer data
        
        Args:
            customer_features: Dictionary or DataFrame with customer features
            
        Returns:
            Cluster label and segment name
        """
        if self.kmeans_model is None:
            print("❌ No model loaded! Please load or train a model first.")
            return None
        
        # Convert to DataFrame if dictionary
        if isinstance(customer_features, dict):
            customer_features = pd.DataFrame([customer_features])
        
        # Scale features
        features_scaled = self.scaler.transform(customer_features)
        
        # Predict cluster
        cluster = self.kmeans_model.predict(features_scaled)[0]
        
        return cluster
    
    def save_results(self, output_path='outputs/customer_segments.csv'):
        """
        Save segmentation results to CSV
        
        Args:
            output_path: Path to save results
        """
        print(f"\n💾 Saving results to {output_path}...")
        
        # Select columns to save
        output_cols = [
            'customer_sk', 'customer_unique_id', 'customer_state', 'customer_region',
            'recency_days', 'frequency_orders', 'monetary_value',
            'avg_order_value', 'avg_review_score', 'customer_lifetime_days',
            'rfm_score', 'customer_value',
            'kmeans_cluster', 'dbscan_cluster', 'segment_label',
            'pca_1', 'pca_2'
        ]
        
        self.rfm_data[output_cols].to_csv(output_path, index=False)
        print(f"✅ Results saved successfully!")
        print(f"   Total customers: {len(self.rfm_data)}")
        print(f"   Columns: {len(output_cols)}")
    
    def run_pipeline(self):
        """
        Run the complete customer segmentation pipeline
        """
        print("=" * 70)
        print("🚀 CUSTOMER SEGMENTATION PIPELINE")
        print("=" * 70)
        
        # Create output directory
        os.makedirs('outputs', exist_ok=True)
        
        # Step 1: Connect to database
        if not self.connect_db():
            return False
        
        # Step 2: Extract RFM data
        if self.extract_rfm_data() is None:
            self.close_db()
            return False
        
        # Step 3: Engineer features
        self.engineer_features()
        
        # Step 4: Prepare features for clustering
        self.prepare_features_for_clustering()
        
        # Step 5: Find optimal k
        self.find_optimal_k(max_k=8)
        
        # Step 6: Fit K-Means
        self.fit_kmeans()
        
        # Step 7: Fit DBSCAN
        self.fit_dbscan(eps=0.8, min_samples=10)
        
        # Step 8: Apply PCA
        self.apply_pca()
        
        # Step 9: Visualize clusters
        self.visualize_clusters()
        
        # Step 10: Analyze segments
        self.analyze_segments()
        
        # Step 11: Save results
        self.save_results()
        
        # Step 12: Save K-Means model
        self.save_model()
        
        # Close database connection
        self.close_db()
        
        print("\n" + "=" * 70)
        print("✅ CUSTOMER SEGMENTATION PIPELINE COMPLETED!")
        print("=" * 70)
        
        return True


def main():
    """Main execution function"""
    segmentation = CustomerSegmentation()
    success = segmentation.run_pipeline()
    
    if success:
        print("\n📊 Pipeline Summary:")
        print("   ✓ RFM features extracted from warehouse")
        print("   ✓ Optimal k found using silhouette score")
        print("   ✓ K-Means clustering applied")
        print("   ✓ DBSCAN clustering applied")
        print("   ✓ PCA visualization created")
        print("   ✓ Results saved to outputs/")
        print("   ✓ K-Means model saved as .pkl file")
        print("\n🎯 Next steps:")
        print("   - Review segment characteristics")
        print("   - Create targeted marketing campaigns")
        print("   - Monitor segment transitions over time")
        print("   - Use saved model for real-time predictions")
    else:
        print("\n❌ Pipeline failed. Check logs for details.")


if __name__ == '__main__':
    main()
