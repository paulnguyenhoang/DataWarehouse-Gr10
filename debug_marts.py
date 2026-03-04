import psycopg2
from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv(Path('src/.env'))
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
cur = conn.cursor()

# Get column names
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'marts' AND table_name = 'agg_monthly_sales' ORDER BY ordinal_position")
columns = [r[0] for r in cur.fetchall()]
print('Columns:', ', '.join(columns))

# Get first row
cur.execute('SELECT * FROM marts.agg_monthly_sales ORDER BY year_month DESC LIMIT 1')
row = cur.fetchone()
print('\nFirst row data:')
for i, col in enumerate(columns):
    print(f'  {col}: {row[i]}')

# Check fact_order_items
print('\n--- Checking fact_order_items ---')
cur.execute('SELECT COUNT(*), SUM(item_price), SUM(total_item_value) FROM facts.fact_order_items')
foi = cur.fetchone()
print(f'Order items: {foi[0]:,} records')
print(f'Total item_price: {foi[1]}')
print(f'Total total_item_value: {foi[2]}')

# Check a sample
cur.execute('SELECT order_id, item_price, freight_value, total_item_value FROM facts.fact_order_items LIMIT 5')
print('\nSample order items:')
for row in cur.fetchall():
    print(f'  Order: {row[0]}, Price: {row[1]}, Freight: {row[2]}, Total: {row[3]}')

cur.close()
conn.close()
