# Olist Data Warehouse Dashboard

Professional dashboards built with Streamlit and Plotly for analyzing Olist e-commerce data from the data warehouse marts.

## 📊 Dashboard Overview

### 1. Monthly Sales Dashboard (`📈 Monthly Sales Analysis`)
Real-time analysis of sales metrics over time with interactive visualizations:

- **Key Metrics**: Total orders, revenue, average order value, customers, products
- **Revenue Trend**: Monthly revenue analysis with trend visualization
- **Orders & Items**: Combined view of order count and item quantity
- **Customer & Geographic**: Customer acquisition and geographic reach
- **Average Metrics**: Average order value and items per order trends
- **Freight Analysis**: Shipping costs and their percentage of revenue
- **Detailed Data**: Complete monthly breakdown table

**Data Source**: `marts.agg_monthly_sales`

### 2. Product Performance Dashboard (`🛍️ Product Performance Analysis`)
Comprehensive product analytics with rankings and customer insights:

- **Key Metrics**: Top products, total revenue, orders, ratings, reviews
- **Interactive Filters**: Select top N products and minimum rating
- **Top Products by Revenue**: Revenue leaders visualization
- **Top Products by Orders**: Order volume leaders
- **Price vs Rating**: Scatter plot showing price-rating relationship
- **Review Distribution**: Products grouped by rating ranges
- **Revenue vs Volume**: Bubble chart for detailed product analysis
- **Detailed Table**: Complete product performance metrics

**Data Source**: `marts.agg_product_performance`

### 3. Data Overview (`📊 Data Warehouse Overview`)
System monitoring and database statistics:

- **Connection Status**: Database connectivity check
- **Table Row Counts**: Records per table by layer
- **Table Sizes**: Storage consumption analysis
- **Architecture Info**: Data warehouse structure documentation
- **Data Flow Diagram**: Visual representation of ETL pipeline

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- PostgreSQL with Olist data warehouse
- ETL pipeline completed (data loaded in marts)

### Installation

1. **Install Dependencies**:
```bash
pip install -r requirements_dashboard.txt
```

2. **Configure Database Connection**:
Ensure `src/.env` file exists with correct credentials:
```
DB_HOST=your_host
DB_PORT=5432
DB_NAME=olist_datawarehouse
DB_USER=your_user
DB_PASSWORD=your_password
```

### Running the Dashboard

#### Option 1: Streamlit Dashboard
```bash
cd src
streamlit run dashboard_main.py
```

Access at: `http://localhost:8501`

#### Option 2: FastAPI Backend (Optional)
```bash
cd src
python api_backend.py
```

Access at: `http://localhost:8000`
- API Documentation: `http://localhost:8000/docs`
- Alternative Docs: `http://localhost:8000/redoc`

## 📁 Project Structure

```
src/
├── dashboard_main.py              # Main Streamlit app
├── api_backend.py                 # FastAPI backend server
├── pages/
│   ├── __init__.py
│   ├── monthly_sales_dashboard.py # Monthly sales visualizations
│   ├── product_performance_dashboard.py # Product analytics
│   └── data_overview.py           # Database statistics
└── .env                           # Database configuration
```

## 🎨 Features

### Streamlit Dashboard
- ✅ Multi-page navigation with sidebar menu
- ✅ Interactive Plotly charts
- ✅ Real-time data updates
- ✅ Responsive design
- ✅ Data filtering and exploration
- ✅ Detailed data tables
- ✅ Custom CSS styling

### FastAPI Backend
- ✅ RESTful API endpoints
- ✅ CORS enabled for cross-origin requests
- ✅ Health check endpoint
- ✅ Query parameters for filtering
- ✅ Automatic API documentation (Swagger/ReDoc)
- ✅ Pydantic data validation

## 📊 API Endpoints

### Health Check
```
GET /health
```

### Monthly Sales
```
GET /api/v1/monthly-sales?limit=12&min_year_month=202301
GET /api/v1/monthly-sales/summary
```

### Products
```
GET /api/v1/products?limit=50&min_revenue_rank=20&min_rating=3.5
GET /api/v1/products/top-by-revenue?limit=10
GET /api/v1/products/top-by-orders?limit=10
GET /api/v1/products/summary
```

## 🔧 Configuration

### Dashboard Settings
Edit `dashboard_main.py` to customize:
- Page layout (wide/centered)
- Color scheme
- Default selections
- Refresh intervals

### API Settings
Edit `api_backend.py` to customize:
- Host/port
- CORS origins
- Query limits
- Response formats

## 📈 Data Visualization Types

### Monthly Sales Dashboard
- Line charts with markers (revenue trends)
- Dual-axis bar/line charts (orders vs items)
- Area charts (revenue fill)
- Data tables with sorting/filtering

### Product Performance Dashboard
- Horizontal bar charts (top products)
- Scatter plots (price vs rating)
- Bubble charts (3D analysis)
- Distribution histograms
- Interactive data grid

## 🐛 Troubleshooting

### Dashboard Won't Load
1. Verify PostgreSQL connection: `python -c "import psycopg2; print('OK')"`
2. Check `.env` file credentials
3. Ensure ETL pipeline completed: `SELECT COUNT(*) FROM marts.agg_monthly_sales;`

### API Connection Issues
1. Check FastAPI is running: `http://localhost:8000/health`
2. Verify CORS settings if calling from different origin
3. Check firewall/security group rules

### Slow Dashboard Performance
1. Clear Streamlit cache: `rm -rf ~/.streamlit/`
2. Increase database query cache (TTL = 3600s by default)
3. Optimize queries in database

## 📝 Usage Examples

### Streamlit Usage
1. Navigate using sidebar menu
2. Interactive charts support:
   - Hover for details
   - Zoom/pan
   - Export as PNG
   - Click legend items to toggle series

### API Usage
```python
import requests

# Get monthly sales
response = requests.get('http://localhost:8000/api/v1/monthly-sales?limit=6')
data = response.json()

# Get product summary
response = requests.get('http://localhost:8000/api/v1/products/summary')
summary = response.json()
```

## 🚀 Deployment

### Streamlit Cloud
```bash
streamlit run dashboard_main.py --logger.level=info
```

### Docker (Optional)
Create `Dockerfile`:
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements_dashboard.txt .
RUN pip install -r requirements_dashboard.txt
COPY src/ .
CMD ["streamlit", "run", "dashboard_main.py", "--server.port=8501"]
```

## 📞 Support

For issues or questions:
1. Check data warehouse connection
2. Verify ETL pipeline completion
3. Review logs in `logs/` directory
4. Check database query results directly

## 📄 License

This project is part of the Olist Data Warehouse educational project.
