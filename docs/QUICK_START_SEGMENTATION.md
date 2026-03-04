# Customer Segmentation - Quick Start Guide 🚀

## Prerequisites ✅

1. **Data Warehouse Setup**
   - PostgreSQL database running
   - ETL pipeline completed
   - Data loaded in warehouse

2. **Python Environment**
   - Python 3.8 or higher
   - All dependencies installed

## Step-by-Step Execution 📝

### Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 2: Configure Database Connection

Edit `src/pipeline/.env`:
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=olist_datawarehouse
DB_USER=your_username
DB_PASSWORD=your_password
```

### Step 3: Run Segmentation Pipeline

```bash
cd src/pipeline
python customer_segmentation.py
```

**Expected Output:**
```
🚀 CUSTOMER SEGMENTATION PIPELINE
==================================================
🔗 Connecting to database...
✅ Connected successfully!
📊 Extracting RFM data from warehouse...
✅ Extracted 99,441 customers
🔧 Engineering features...
✅ Features engineered successfully
🎯 Preparing features for clustering...
✅ Prepared 99,441 samples with 7 features
🔍 Finding optimal k (testing k=2 to k=8)...
  k=2: Inertia=634825.12, Silhouette=0.456
  k=3: Inertia=521034.45, Silhouette=0.523
  k=4: Inertia=445123.78, Silhouette=0.587  ← Optimal
  ...
✅ Optimal k = 4 (Silhouette Score: 0.587)
🎯 Fitting K-Means with k=4...
✅ K-Means clustering completed
   Silhouette Score: 0.587
   Davies-Bouldin Index: 0.723
📊 Cluster Distribution:
   Cluster 0: 24,860 customers (25.0%)
   Cluster 1: 29,835 customers (30.0%)
   Cluster 2: 22,373 customers (22.5%)
   Cluster 3: 22,373 customers (22.5%)
...
✅ CUSTOMER SEGMENTATION PIPELINE COMPLETED!
```

### Step 4: View Results

**Generated Files:**
```
outputs/
├── customer_segments.csv              # Full segmentation results
├── optimal_k_selection.png            # Elbow curve analysis
├── customer_segmentation_pca.png      # PCA visualization
└── segment_summary.csv                # Summary statistics
```

### Step 5: Explore with Jupyter Notebook

```bash
jupyter notebook notebooks/Customer_Segmentation_Analysis.ipynb
```

### Step 6: Launch Dashboard

```bash
cd src
streamlit run dashboard_main.py
```

Then navigate to **"🎯 Customer Segmentation"** page.

## Quick Results Check ✨

### Load and Inspect Results

```python
import pandas as pd

# Load segmentation results
df = pd.read_csv('outputs/customer_segments.csv')

# Overview
print(f"Total Customers: {len(df):,}")
print(f"Number of Segments: {df['kmeans_cluster'].nunique()}")

# Segment distribution
print("\nSegment Distribution:")
print(df['kmeans_cluster'].value_counts().sort_index())

# Top segment by revenue
revenue_by_segment = df.groupby('kmeans_cluster')['monetary_value'].sum().sort_values(ascending=False)
print(f"\nTop Revenue Segment: {revenue_by_segment.index[0]} (${revenue_by_segment.values[0]:,.2f})")
```

## Understanding Your Segments 🎯

### Typical Segment Profile

| Segment | Characteristics | Action |
|---------|----------------|--------|
| **0** | Low recency, High value | VIP treatment |
| **1** | High recency, Low value | Win-back campaign |
| **2** | Medium all metrics | Standard marketing |
| **3** | New customers | Onboarding flow |

### RFM Interpretation

- **Recency < 60 days**: Active customers ✅
- **Frequency > 3 orders**: Loyal customers 💎
- **Monetary > $500**: High value customers 💰

## Troubleshooting 🔧

### Issue: "Database connection failed"
**Solution:**
```bash
# Check database is running
psql -h localhost -U postgres -d olist_datawarehouse

# Verify .env file
cat src/pipeline/.env
```

### Issue: "No data extracted"
**Solution:**
```bash
# Verify ETL completed
psql -d olist_datawarehouse -c "SELECT COUNT(*) FROM facts.fact_order_items;"

# Check order status
psql -d olist_datawarehouse -c "SELECT order_status, COUNT(*) FROM dimensions.dim_order_status GROUP BY order_status;"
```

### Issue: "Module not found"
**Solution:**
```bash
# Reinstall dependencies
pip install -r requirements.txt --upgrade

# Verify installation
python -c "import sklearn; print(sklearn.__version__)"
```

### Issue: "Poor clustering results (low silhouette score)"
**Solution:**
1. Check data quality: `df.describe()`
2. Increase max_k: Change `max_k=10` in code
3. Adjust DBSCAN parameters: `eps=1.0, min_samples=20`

## Performance Tips ⚡

### For Large Datasets (>100k customers)
```python
# Use sampling for exploration
segmentation = CustomerSegmentation()
segmentation.connect_db()
df = segmentation.extract_rfm_data()

# Sample 50k customers
df_sample = df.sample(n=50000, random_state=42)
segmentation.rfm_data = df_sample
segmentation.run_pipeline()
```

### Speed Up K-Means
```python
# Use mini-batch K-Means
from sklearn.cluster import MiniBatchKMeans

kmeans_model = MiniBatchKMeans(
    n_clusters=4,
    batch_size=1000,
    random_state=42
)
```

## Next Steps 🎯

1. **Validate Segments**
   - Review segment characteristics
   - Compare with business knowledge
   - Adjust parameters if needed

2. **Create Action Plans**
   - Define marketing strategies per segment
   - Set up automated campaigns
   - Measure results

3. **Monitor Over Time**
   - Schedule monthly segmentation runs
   - Track segment transitions
   - Identify trends

4. **Integrate with Systems**
   - Export segments to CRM
   - Set up dashboards
   - Create alerts

## Support 📞

**Documentation:**
- Full README: `CUSTOMER_SEGMENTATION_README.md`
- Code documentation: Inline comments in `customer_segmentation.py`

**Common Commands:**
```bash
# Run pipeline
python src/pipeline/customer_segmentation.py

# Launch dashboard
streamlit run src/dashboard_main.py

# Open notebook
jupyter notebook notebooks/Customer_Segmentation_Analysis.ipynb
```

---

**Happy Segmenting! 🎉**
