import os
from pyspark.sql import SparkSession

# CREATE SPARK SESSION S3
def init_spark_with_s3(aws_access_key, aws_secret_key, region):
    print("Initializing Spark Session...")

    spark = SparkSession.builder \
        .appName("S3_Direct_Read") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("Spark is connected to AWS")
    print("-------------------------")
    return spark

# READING CSVs from S3
def read_csv(spark, bucket_name, folder, s3_files):
    """
    Read each CSV file separately into its own DataFrame.
    Store DataFrames using clean names (without .csv extension).
    """

    dataframes = {}

    for file in s3_files:
        file_name = file["file_name"]

        # Remove .csv extension safely
        clean_name = file_name.rsplit(".", 1)[0]

        # Build S3 path
        s3_path = f"s3a://{bucket_name}/{folder}/{file_name}"

        print(f"[READING] {file_name}")

        try:
            # Read CSV into Spark DataFrame
            df = (
                spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(s3_path)
            )

            # Count rows (triggers execution)
            row_count = df.count()
            print(f"[DONE] {file_name} → {row_count} rows")

            # Store DataFrame
            dataframes[clean_name] = df

        except Exception as e:
            print(f"[ERROR] Failed to read {file_name}: {e}")

    # Summary
    print("\n========== Loaded DataFrames ==========")
    for name in dataframes:
        print(f"- {name}")

    print(f"\nTotal DataFrames loaded: {len(dataframes)}")

    return dataframes
