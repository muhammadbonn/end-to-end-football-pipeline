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
    Connects to Snowflake using secure credentials from .env,
    creates the S3 stage dynamically using AWS credentials,
    and executes the SQL pipeline scripts in order.
    """
    print("[INFO] Initializing Snowflake connection...")

    try:
        # 1. Fetch Snowflake credentials securely from environment variables.
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

        # ----------------------------------------------------------------------
        # SECURELY CREATE S3 STAGE
        # ----------------------------------------------------------------------
        print("[INFO] Creating S3 Stage securely using environment variables...")
        
        # Fetch AWS credentials from .env
        aws_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
        s3_bucket = os.getenv('S3_BUCKET_NAME')
        
        # Dynamic SQL query to create the stage without hardcoding secrets in .sql files
        create_stage_query = f"""
        CREATE OR REPLACE STAGE ete_football_db.Staging.football_s3_stage
        URL='s3://{s3_bucket}/'
        CREDENTIALS=(AWS_KEY_ID='{aws_key}' AWS_SECRET_KEY='{aws_secret}')
        FILE_FORMAT = (TYPE = PARQUET);
        """
        
        # Execute the stage creation query
        for cur in conn.execute_string(create_stage_query):
            pass
            
        print("[SUCCESS] S3 Stage created securely via Python!")

    except Exception as e:
        print(f"[ERROR] Failed to connect to Snowflake or create stage: {e}")
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