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

print('\n📊 DATABASE STRUCTURE CHECK\n' + '='*60)

# Check schemas
cur.execute("""
    SELECT schema_name 
    FROM information_schema.schemata 
    WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
    ORDER BY schema_name
""")
schemas = cur.fetchall()
print(f'\n✅ Schemas: {len(schemas)}')
for s in schemas:
    print(f'   - {s[0]}')

# Check dimension tables
cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'dimensions'
    ORDER BY table_name
""")
dims = cur.fetchall()
print(f'\n✅ Dimension Tables: {len(dims)}')
for d in dims:
    cur.execute(f'SELECT COUNT(*) FROM dimensions.{d[0]}')
    count = cur.fetchone()[0]
    print(f'   - {d[0]}: {count:,} records')

# Check fact tables
cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'facts'
    ORDER BY table_name
""")
facts = cur.fetchall()
print(f'\n✅ Fact Tables: {len(facts)}')
for f in facts:
    cur.execute(f'SELECT COUNT(*) FROM facts.{f[0]}')
    count = cur.fetchone()[0]
    print(f'   - {f[0]}: {count:,} records')

# Check mart tables
cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'marts'
    ORDER BY table_name
""")
marts = cur.fetchall()
print(f'\n✅ Mart Tables: {len(marts)}')
for m in marts:
    cur.execute(f'SELECT COUNT(*) FROM marts.{m[0]}')
    count = cur.fetchone()[0]
    print(f'   - {m[0]}: {count:,} records')

# Check foreign keys
cur.execute("""
    SELECT 
        tc.table_schema,
        tc.table_name,
        COUNT(*) as fk_count
    FROM information_schema.table_constraints AS tc 
    WHERE tc.constraint_type = 'FOREIGN KEY'
    GROUP BY tc.table_schema, tc.table_name
    ORDER BY tc.table_schema, tc.table_name
""")
fks = cur.fetchall()
print(f'\n✅ Foreign Key Constraints: {sum(f[2] for f in fks)} total')
for fk in fks:
    print(f'   - {fk[0]}.{fk[1]}: {fk[2]} FKs')

print('\n' + '='*60)
print('✅ Database is ready for analysis!\n')

cur.close()
conn.close()
