import os
import snowflake.connector
from dotenv import load_dotenv

# ----------------------------------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# ----------------------------------------------------------------------
# Load variables from the .env file into the system environment
load_dotenv()

def run_snowflake_queries():
    """
    Connects to Snowflake using secure credentials from .env 
    and executes the SQL pipeline scripts in order.
    """
    print("[INFO] Initializing Snowflake connection...")

    try:
        # Fetch credentials securely from environment variables.
        # NOTE: 'schema' is intentionally omitted here so that the SQL scripts 
        # can dynamically switch between 'Staging' and 'gold' schemas using 'USE SCHEMA'.
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE')
        )
        print("[SUCCESS] Connected to Snowflake successfully!")
    except Exception as e:
        print(f"[ERROR] Failed to connect to Snowflake: {e}")
        raise

    # ----------------------------------------------------------------------
    # EXECUTE SQL SCRIPTS
    # ----------------------------------------------------------------------
    # Define the exact order of SQL scripts to be executed
    sql_files = [
        '/opt/airflow/sql/1_create_tables.sql',
        '/opt/airflow/sql/2_load_data.sql',
        '/opt/airflow/sql/3_gold_layer_tables.sql'
    ]

    for file_path in sql_files:
        print(f"[INFO] Executing script: {file_path}")
        try:
            # Open and read the SQL file
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_text = f.read()
                
                # execute_string gracefully handles multiple SQL statements separated by ';'
                for cur in conn.execute_string(sql_text):
                    pass
                    
            print(f"[SUCCESS] Script executed successfully: {file_path}")
        except Exception as e:
            print(f"[ERROR] Failed while executing {file_path}: {e}")
            conn.close()
            raise

    print("[SUCCESS] All data successfully loaded and transformed in Snowflake! 🚀")
    
    # Safely close the connection
    conn.close()

if __name__ == "__main__":
    run_snowflake_queries()
