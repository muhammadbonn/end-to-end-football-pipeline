import os
import shutil
import kagglehub
from dotenv import load_dotenv
from utils.spark_utils import init_spark_with_s3
from utils.aws_utils import create_s3_client
from processors.process_teamstats import process_teamstats
from processors.process_shots import process_shots
from processors.process_appearances import process_appearances
from processors.process_games import process_games
from processors.process_common import process_na_du
from processors.process_common import process_teams
from processors.process_common import process_players
from processors.process_common import process_leagues

# ----------------------------------------------------------------------
# ENV LOADING
def load_env():
    """
    Load required environment variables.
    """
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    dotenv_path = os.path.join(project_root, '.env')
    load_dotenv(dotenv_path)

    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION")
    
    bucket_name = os.getenv("S3_BUCKET_NAME")
    kaggle_path = os.getenv("KAGGLE_PATH")

    if not aws_access_key or not aws_secret_key:
        raise ValueError("AWS credentials not found in environment variables")

    return aws_access_key, aws_secret_key, region, bucket_name, kaggle_path

# ----------------------------------------------------------------------
# KAGGLE DOWNLOAD
def download_kaggle_dataset(kaggle_path):
    """
    Download dataset from Kaggle and return local path.
    Clears the kagglehub cache first to prevent corrupted zip file errors.
    """
    if not kaggle_path:
        raise ValueError("kaggle_path is missing")

    # 1. Define the cache directory path for kagglehub
    cache_path = os.path.expanduser('~/.cache/kagglehub')
    
    # 2. Check if the cache directory exists, and if so, delete it entirely
    # This ensures an idempotent run and prevents zipfile.BadZipFile errors
    if os.path.exists(cache_path):
        print(f"Clearing old cache at: {cache_path} to avoid Corrupted/BadZipFile errors...")
        shutil.rmtree(cache_path)

    # 3. Proceed with a fresh download
    print("Downloading fresh dataset from Kaggle...")
    dataset_path = kagglehub.dataset_download(kaggle_path)
    print(f"Dataset stored successfully at: {dataset_path}")

    return dataset_path

# ----------------------------------------------------------------------
# FILE HANDLING
def get_csv_metadata(dataset_path):
    """
    Recursively collect all CSV files with metadata:
    - full path
    - file name
    - file size (bytes + MB)
    """
    csv_files = []

    for root, _, files in os.walk(dataset_path):
        for file in files:
            if file.endswith(".csv"):
                full_path = os.path.join(root, file)

                # get file size in bytes
                size_bytes = os.path.getsize(full_path)

                # convert to MB
                size_mb = round(size_bytes / (1024 * 1024), 2)

                csv_files.append({
                    "file_name": file,
                    "file_path": full_path,
                    "size_bytes": size_bytes,
                    "size_mb": size_mb
                })

    return csv_files

# ----------------------------------------------------------------------
# ENV SETUP
def setup_environment():
    aws_access_key, aws_secret_key, region, bucket_name, _ = load_env()

    spark = init_spark_with_s3(aws_access_key, aws_secret_key, region)

    s3_client = create_s3_client(
        aws_access_key,
        aws_secret_key,
        region
    )

    return spark, s3_client, bucket_name

# ----------------------------------------------------------------------
# PROCESSING TABLES AND SAVING THEM IN A NEW FOLDER
PROCESSORS_FUNCTIONS = {
"teamstats": process_teamstats,
"shots": process_shots,
"teams": process_teams,
"players": process_players,
"leagues": process_leagues,
"games": process_games,
"appearances": process_appearances
}
    
def process_table(dfs, bucket_name, table, staging_folder="staging"):
    print(f"Processing table: {table}")

    if table not in dfs:
        raise ValueError(f"{table} table not found")

    if table not in PROCESSORS_FUNCTIONS:
        raise ValueError(f"No processor found for {table}")

    # EXCUTE
    processor = PROCESSORS_FUNCTIONS[table]

    df = dfs[table]
    df = process_na_du(df)
    df_new = processor(df)
    
    print("Processing Completed")

    staging_path = f"s3a://{bucket_name}/{staging_folder}/{table}"

    df_new.write.mode("overwrite").parquet(staging_path)

    print(f"{table} saved successfully to {staging_path}")
