# Customer Segmentation Pipeline 🎯

Complete pipeline for customer segmentation using RFM analysis, K-Means, DBSCAN, and PCA visualization.

## 📋 Overview

This pipeline performs advanced customer segmentation analysis by:
- Extracting RFM (Recency, Frequency, Monetary) features from the data warehouse
- Finding optimal number of clusters using Elbow Method and Silhouette Score
- Applying K-Means and DBSCAN clustering algorithms
- Evaluating models with Silhouette Score and Davies-Bouldin Index
- Visualizing results using PCA (Principal Component Analysis)
- Generating actionable business insights

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install pandas numpy scikit-learn matplotlib seaborn psycopg2-binary python-dotenv
```

### 2. Run the Pipeline

```bash
cd src/pipeline
python customer_segmentation.py
```

### 3. View Results

```bash
# Check outputs directory
ls outputs/

# Files generated:
# - customer_segments.csv          (Full segmentation results)
# - optimal_k_selection.png        (Elbow curve & silhouette scores)
# - customer_segmentation_pca.png  (PCA visualization)
```

## 📊 Features Used for Segmentation

### RFM Core Features
- **Recency**: Days since last purchase
- **Frequency**: Total number of orders
- **Monetary**: Total spending amount

### Additional Features
- `avg_order_value`: Average order value
- `avg_review_score`: Customer satisfaction score
- `purchase_frequency_rate`: Orders per day active
- `customer_lifetime_days`: Days since first purchase
- `total_freight`: Total shipping costs
- `customer_region`: Geographic region

## 🎯 Clustering Algorithms

### 1. K-Means Clustering
- **Method**: Centroid-based clustering
- **Optimal K**: Automatically selected using silhouette score
- **Range Tested**: k=2 to k=8
- **Evaluation**: Silhouette Score, Inertia

**Advantages:**
- Fast and scalable
- Well-defined cluster centers
- Good for spherical clusters

### 2. DBSCAN Clustering
- **Method**: Density-based clustering
- **Parameters**: `eps=0.8`, `min_samples=10`
- **Evaluation**: Silhouette Score, noise detection

**Advantages:**
- Identifies outliers as noise
- No need to specify number of clusters
- Finds arbitrary-shaped clusters

## 📈 Output Files

### 1. customer_segments.csv
Complete customer segmentation data with:
```
- customer_sk, customer_unique_id
- Geographic info (state, region, city)
- RFM metrics (recency, frequency, monetary)
- Cluster assignments (kmeans_cluster, dbscan_cluster)
- Segment labels (business interpretations)
- PCA components (pca_1, pca_2)
```

### 2. optimal_k_selection.png
Two plots showing:
- **Elbow Curve**: Inertia vs number of clusters
- **Silhouette Scores**: Quality metric vs number of clusters

### 3. customer_segmentation_pca.png
Side-by-side comparison of:
- **K-Means clustering** with centroids marked
- **DBSCAN clustering** with noise points

## 🏷️ Customer Segments

The pipeline automatically labels customers into segments:

| Segment | Characteristics | Marketing Strategy |
|---------|----------------|-------------------|
| 🌟 **Champions (VIP)** | Low recency, High frequency & monetary | Premium services, early access |
| 💎 **Loyal Customers** | High frequency & monetary | Loyalty rewards, referral programs |
| 🌱 **Potential Loyalists** | Recent buyers, medium engagement | Engagement campaigns, upselling |
| 🆕 **New Customers** | Very recent, single purchase | Welcome series, onboarding |
| ⚠️ **At Risk** | Moderate recency, previously active | Win-back campaigns, special offers |
| 😴 **Lost Customers** | High recency, inactive | Reactivation emails, surveys |

## 📊 Evaluation Metrics

### Silhouette Score
- **Range**: -1 to +1
- **Interpretation**: 
  - > 0.5: Strong clusters
  - 0.25-0.5: Moderate clusters
  - < 0.25: Weak clusters

### Davies-Bouldin Index
- **Range**: 0 to ∞
- **Interpretation**: Lower is better
- **Meaning**: Average similarity between clusters

### Inertia (Within-Cluster Sum of Squares)
- **Range**: 0 to ∞
- **Interpretation**: Lower is better
- **Use**: Elbow method for optimal k

## 🔧 Configuration

Edit parameters in `customer_segmentation.py`:

```python
# K-Means parameters
optimal_k = None  # Auto-detect or set manually
max_k = 8         # Maximum k to test

# DBSCAN parameters
eps = 0.8         # Neighborhood radius
min_samples = 10  # Minimum points for core point

# PCA
n_components = 2  # Dimensions for visualization
```

## 📚 Code Structure

```
customer_segmentation.py
├── CustomerSegmentation (Main Class)
│   ├── connect_db()              # Database connection
│   ├── extract_rfm_data()        # Extract from warehouse
│   ├── engineer_features()       # Feature engineering
│   ├── prepare_features_for_clustering()  # Scaling
│   ├── find_optimal_k()          # Optimal k selection
│   ├── fit_kmeans()              # K-Means clustering
│   ├── fit_dbscan()              # DBSCAN clustering
│   ├── apply_pca()               # PCA transformation
│   ├── visualize_clusters()      # Create plots
│   ├── analyze_segments()        # Generate insights
│   └── run_pipeline()            # Execute all steps
```

## 🎓 Usage Examples

### Example 1: Run with Default Settings
```python
from src.pipeline.customer_segmentation import CustomerSegmentation

segmentation = CustomerSegmentation()
segmentation.run_pipeline()
```

### Example 2: Custom Parameters
```python
segmentation = CustomerSegmentation()
segmentation.connect_db()
segmentation.extract_rfm_data()
segmentation.engineer_features()
segmentation.prepare_features_for_clustering()

# Custom K-Means with k=5
segmentation.fit_kmeans(n_clusters=5)

# Custom DBSCAN parameters
segmentation.fit_dbscan(eps=1.0, min_samples=15)

segmentation.apply_pca()
segmentation.visualize_clusters()
```

### Example 3: Load and Analyze Existing Results
```python
import pandas as pd

df = pd.read_csv('outputs/customer_segments.csv')

# Analyze specific segment
champions = df[df['segment_label'].str.contains('Champions')]
print(f"Champions: {len(champions)} customers")
print(f"Avg Monetary: ${champions['monetary_value'].mean():.2f}")
```

## 📊 Jupyter Notebook

Interactive analysis available in:
```
notebooks/Customer_Segmentation_Analysis.ipynb
```

Features:
- Step-by-step execution
- Detailed visualizations
- Business insights
- Export for dashboards

## 🔍 Data Warehouse Schema

Pipeline queries from:
```sql
dimensions.dim_customer       -- Customer information
dimensions.dim_order_status   -- Order details
facts.fact_order_items        -- Transaction data
facts.fact_payments           -- Payment information
facts.fact_reviews            -- Customer reviews
```

## 🐛 Troubleshooting

### Issue: Database Connection Failed
```bash
# Check .env file
DB_HOST=localhost
DB_PORT=5432
DB_NAME=olist_datawarehouse
DB_USER=your_user
DB_PASSWORD=your_password
```

### Issue: No Data Extracted
- Ensure ETL pipeline has been run
- Check if orders exist with status='delivered'
- Verify database has data in fact tables

### Issue: Poor Clustering Results
- Increase `max_k` to test more clusters
- Adjust DBSCAN `eps` parameter
- Check for data quality issues
- Consider feature engineering adjustments

## 📈 Performance Tips

1. **For Large Datasets**:
   - Use `batch_size` parameter for memory efficiency
   - Consider sampling for initial exploration
   - Use mini-batch K-Means for very large data

2. **Optimization**:
   - Cache database queries
   - Parallelize K-Means with `n_jobs=-1`
   - Use incremental PCA for large feature sets

## 🎯 Next Steps

1. **Monitoring**: Track segment transitions over time
2. **Automation**: Schedule weekly/monthly segmentation runs
3. **Integration**: Add segments to dashboards and reports
4. **Action**: Create targeted campaigns per segment
5. **Validation**: A/B test segment-specific strategies

## 📝 Citation

```
Pipeline developed for Olist E-commerce Data Warehouse
Course: CO4031 - Data Warehousing & Decision Support Systems
Features: RFM Analysis, K-Means, DBSCAN, PCA Visualization
```

## 📞 Support

For issues or questions:
1. Check error messages in console output
2. Review data warehouse connection settings
3. Verify ETL pipeline completed successfully
4. Check requirements.txt for dependencies

---

**Last Updated**: December 2025
**Version**: 1.0.0
