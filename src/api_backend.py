"""
FastAPI Backend for Olist Data Warehouse
RESTful API endpoints for dashboard data
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
import psycopg2
from pathlib import Path
from datetime import datetime
import os

# Initialize FastAPI
app = FastAPI(
    title="Olist Data Warehouse API",
    description="RESTful API for Olist e-commerce data warehouse",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
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

def get_db_connection():
    """Create database connection"""
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
        raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")

# Pydantic models
class MonthlySalesMetrics(BaseModel):
    year_month: int
    year: int
    month: int
    month_name: str
    total_orders: int
    total_items: int
    total_revenue: float
    total_freight: float
    avg_order_value: float
    avg_items_per_order: float
    unique_customers: int
    unique_products: int
    unique_states: int

class ProductPerformance(BaseModel):
    product_sk: int
    product_id: str
    product_category_name: str
    product_category_english: str
    last_12_months_revenue: float
    last_12_months_orders: int
    last_12_months_items: int
    avg_price: float
    avg_review_score: float
    total_reviews: int
    revenue_rank: int
    volume_rank: int

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    database: str

# Health check
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": "connected"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {e}")

# Monthly Sales endpoints
@app.get("/api/v1/monthly-sales", response_model=List[MonthlySalesMetrics])
async def get_monthly_sales(
    limit: Optional[int] = Query(12, ge=1, le=120, description="Number of months to return"),
    min_year_month: Optional[int] = Query(None, description="Filter by minimum year_month (YYYYMM format)")
):
    """Get monthly sales metrics"""
    try:
        conn = get_db_connection()
        
        query = """
        SELECT 
            year_month, year, month, month_name,
            total_orders, total_items, total_revenue, total_freight,
            avg_order_value, avg_items_per_order,
            unique_customers, unique_products, unique_states
        FROM marts.agg_monthly_sales
        """
        
        params = []
        if min_year_month:
            query += " WHERE year_month >= %s"
            params.append(min_year_month)
        
        query += " ORDER BY year_month DESC LIMIT %s"
        params.append(limit)
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/monthly-sales/summary")
async def get_monthly_sales_summary():
    """Get monthly sales summary statistics"""
    try:
        conn = get_db_connection()
        
        query = """
        SELECT 
            COUNT(*) as total_months,
            SUM(total_orders) as total_orders,
            SUM(total_revenue) as total_revenue,
            AVG(avg_order_value) as avg_order_value,
            MAX(unique_customers) as max_customers_per_month,
            MAX(unique_products) as max_products_per_month
        FROM marts.agg_monthly_sales
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df.to_dict('records')[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Product Performance endpoints
@app.get("/api/v1/products", response_model=List[ProductPerformance])
async def get_products(
    limit: Optional[int] = Query(50, ge=1, le=500, description="Number of products to return"),
    min_revenue_rank: Optional[int] = Query(None, description="Filter by maximum revenue rank"),
    min_rating: Optional[float] = Query(None, ge=0, le=5, description="Minimum average review score")
):
    """Get product performance data"""
    try:
        conn = get_db_connection()
        
        query = """
        SELECT 
            app.product_sk, dp.product_id, dp.product_category_name,
            dp.product_category_english,
            app.last_12_months_revenue, app.last_12_months_orders,
            app.last_12_months_items, app.avg_price, app.avg_review_score,
            app.total_reviews, app.revenue_rank, app.volume_rank
        FROM marts.agg_product_performance app
        JOIN dimensions.dim_product dp ON app.product_sk = dp.product_sk
        WHERE 1=1
        """
        
        params = []
        if min_revenue_rank:
            query += " AND app.revenue_rank <= %s"
            params.append(min_revenue_rank)
        
        if min_rating is not None:
            query += " AND app.avg_review_score >= %s"
            params.append(min_rating)
        
        query += " ORDER BY app.revenue_rank ASC LIMIT %s"
        params.append(limit)
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/products/top-by-revenue")
async def get_top_products_by_revenue(limit: Optional[int] = Query(10, ge=1, le=100)):
    """Get top products by revenue"""
    try:
        conn = get_db_connection()
        
        query = """
        SELECT 
            app.product_sk, dp.product_id, dp.product_category_english,
            app.last_12_months_revenue, app.last_12_months_orders,
            app.avg_price, app.avg_review_score, app.total_reviews
        FROM marts.agg_product_performance app
        JOIN dimensions.dim_product dp ON app.product_sk = dp.product_sk
        ORDER BY app.revenue_rank ASC
        LIMIT %s
        """
        
        df = pd.read_sql_query(query, conn, params=[limit])
        conn.close()
        
        return df.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/products/top-by-orders")
async def get_top_products_by_orders(limit: Optional[int] = Query(10, ge=1, le=100)):
    """Get top products by order count"""
    try:
        conn = get_db_connection()
        
        query = """
        SELECT 
            app.product_sk, dp.product_id, dp.product_category_english,
            app.last_12_months_orders, app.last_12_months_revenue,
            app.avg_price, app.avg_review_score, app.total_reviews
        FROM marts.agg_product_performance app
        JOIN dimensions.dim_product dp ON app.product_sk = dp.product_sk
        ORDER BY app.volume_rank ASC
        LIMIT %s
        """
        
        df = pd.read_sql_query(query, conn, params=[limit])
        conn.close()
        
        return df.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/products/summary")
async def get_products_summary():
    """Get product performance summary"""
    try:
        conn = get_db_connection()
        
        query = """
        SELECT 
            COUNT(*) as total_products,
            SUM(last_12_months_revenue) as total_revenue,
            SUM(last_12_months_orders) as total_orders,
            AVG(avg_review_score) as avg_rating,
            MAX(total_reviews) as max_reviews,
            MIN(avg_price) as min_price,
            MAX(avg_price) as max_price
        FROM marts.agg_product_performance
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df.to_dict('records')[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "name": "Olist Data Warehouse API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc",
        "endpoints": {
            "health": "/health",
            "monthly_sales": {
                "list": "/api/v1/monthly-sales",
                "summary": "/api/v1/monthly-sales/summary"
            },
            "products": {
                "list": "/api/v1/products",
                "top_by_revenue": "/api/v1/products/top-by-revenue",
                "top_by_orders": "/api/v1/products/top-by-orders",
                "summary": "/api/v1/products/summary"
            }
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
