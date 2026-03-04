import psycopg2
from psycopg2 import sql, ProgrammingError
import os
from pathlib import Path
import re
from dotenv import load_dotenv
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT'))
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

ADMIN_CONFIG = {
    'host': DB_HOST,
    'port': DB_PORT,
    'user': 'postgres',  # Connect as postgres admin
    'password': DB_PASSWORD,
    'database': 'postgres'
}

APP_CONFIG = {
    'host': DB_HOST,
    'port': DB_PORT,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'database': DB_NAME
}

# Path to SQL file
SQL_FILE_PATH = Path(__file__).parent.parent.parent / 'sql' / 'create_datawarehouse_postgresql.sql'

def drop_and_create_database():
    """Drop existing database and create new one (requires postgres user)"""
    conn = None
    try:
        print("🔗 Connecting as postgres admin...")
        conn = psycopg2.connect(**ADMIN_CONFIG)
        conn.autocommit = True
        
        cursor = conn.cursor()
        
        # Terminate existing connections
        print("🔌 Terminating existing connections...")
        cursor.execute("""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = 'olist_datawarehouse'
            AND pid <> pg_backend_pid();
        """)
        
        # Drop database
        print("🗑️  Dropping existing database...")
        cursor.execute("DROP DATABASE IF EXISTS olist_datawarehouse;")
        
        # Only create user if not postgres (superuser)
        if APP_CONFIG['user'] != 'postgres':
            # Drop user if exists
            cursor.execute(f"DROP USER IF EXISTS {APP_CONFIG['user']};")
            
            # Create new user with full privileges
            print("👤 Creating user with full privileges...")
            cursor.execute(f"CREATE USER {APP_CONFIG['user']} WITH PASSWORD '{APP_CONFIG['password']}' CREATEDB CREATEROLE;")
        else:
            print("👤 Using existing postgres superuser...")
        
        # Create new database
        print("📦 Creating new database...")
        cursor.execute(f"CREATE DATABASE olist_datawarehouse OWNER {APP_CONFIG['user']};")
        
        # Grant all privileges on database
        cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE olist_datawarehouse TO {APP_CONFIG['user']};")
        
        # Connect to new database to grant schema privileges
        conn.close()
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user='postgres',
            password=DB_PASSWORD,
            database=DB_NAME
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Grant schema privileges
        cursor.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {APP_CONFIG['user']};")
        cursor.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {APP_CONFIG['user']};")
        cursor.execute(f"GRANT ALL ON SCHEMA public TO {APP_CONFIG['user']};")
        
        cursor.close()
        print("✅ Database and user setup completed!")
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def clean_sql_content(sql_content):
    """Remove psql-specific commands and clean SQL"""
    lines = []
    skip_until_semicolon = False
    
    for line in sql_content.split('\n'):
        original_line = line
        line = line.strip()
        
        # Skip psql commands
        if line.startswith('\\'):
            continue
        
        # Skip empty lines and comments
        if not line or line.startswith('--'):
            continue
        
        lines.append(original_line)
    
    return '\n'.join(lines)

def split_sql_statements(sql_content):
    """
    Split SQL content into individual statements, 
    handling dollar-quoted strings properly
    """
    statements = []
    current = []
    in_dollar_quote = False
    dollar_quote_delimiter = None
    i = 0
    
    while i < len(sql_content):
        if not in_dollar_quote:
            # Check for dollar quote start
            if sql_content[i:i+1] == '$':
                # Find the closing $
                j = i + 1
                while j < len(sql_content) and sql_content[j] != '$':
                    j += 1
                dollar_quote_delimiter = sql_content[i:j+1]
                in_dollar_quote = True
                current.append(sql_content[i:j+1])
                i = j + 1
            elif sql_content[i] == ';':
                current.append(';')
                statement = ''.join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
                i += 1
            else:
                current.append(sql_content[i])
                i += 1
        else:
            # Inside dollar quote - look for closing delimiter
            if sql_content[i:i+len(dollar_quote_delimiter)] == dollar_quote_delimiter:
                current.append(dollar_quote_delimiter)
                in_dollar_quote = False
                i += len(dollar_quote_delimiter)
            else:
                current.append(sql_content[i])
                i += 1
    
    # Add remaining
    if current:
        statement = ''.join(current).strip()
        if statement:
            statements.append(statement)
    
    return statements

def read_sql_file(file_path):
    """Read SQL file and return its content"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            content = clean_sql_content(content)
            return content
    except FileNotFoundError:
        print(f"❌ Error: SQL file not found at {file_path}")
        raise

def execute_sql_script(sql_content):
    """Execute SQL script with proper connection"""
    conn = None
    try:
        print("🔗 Connecting to olist_datawarehouse...")
        conn = psycopg2.connect(**APP_CONFIG)
        conn.autocommit = False
        
        cursor = conn.cursor()
        print("✅ Connected successfully!")
        
        # Split statements properly
        statements = split_sql_statements(sql_content)
        print(f"📊 Found {len(statements)} SQL statements")
        
        executed = 0
        failed = 0
        
        for i, statement in enumerate(statements):
            if not statement.strip():
                continue
            
            try:
                cursor.execute(statement)
                executed += 1
                
                if executed % 10 == 0:
                    print(f"  ✓ Executed {executed} statements...")
                    
            except psycopg2.Error as e:
                error_msg = str(e).split('\n')[0][:80]
                print(f"  ⚠️  Statement {i+1} error: {error_msg}")
                failed += 1
                conn.rollback()  # Rollback this statement
                continue
        
        conn.commit()
        cursor.close()
        print(f"✅ SQL script executed! Executed: {executed}, Failed: {failed}")
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        print(f"❌ Error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            print("🔌 Database connection closed.")

def main():
    """Main function to orchestrate the data warehouse creation"""
    try:
        print("=" * 60)
        print("PostgreSQL Data Warehouse Setup")
        print("=" * 60)
        
        # Step 1: Drop and create database
        print("\n📋 Step 1: Database Setup")
        drop_and_create_database()
        
        # Step 2: Read SQL file
        print("\n📋 Step 2: Reading SQL Schema")
        print(f"📄 Reading SQL file: {SQL_FILE_PATH}")
        sql_content = read_sql_file(SQL_FILE_PATH)
        
        # Step 3: Execute SQL script
        print("\n📋 Step 3: Creating Schemas, Tables, and Constraints")
        print("⏳ This may take a few moments...\n")
        execute_sql_script(sql_content)
        
        print("\n" + "=" * 60)
        print("✅ Data Warehouse created successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        exit(1)

if __name__ == '__main__':
    main()