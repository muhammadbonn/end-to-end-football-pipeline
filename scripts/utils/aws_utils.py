import os
import boto3
import botocore

# ----------------------------------------------------------------------
# AWS CONNECTION
def create_s3_client(aws_access_key, aws_secret_key, region):
    """
    Create and return an S3 client.
    """
    print("Connecting to AWS S3...")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )

    print("Connected")
    print("-------------------------")
    return s3_client

# ----------------------------------------------------------------------
# S3 BUCKET MANAGEMENT
def ensure_bucket_exists(s3_client, bucket_name, region):
    """
    Check if S3 bucket exists, if not create it.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
        return True

    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])

        if error_code == 404:
            print(f"Bucket '{bucket_name}' not found. Creating...")

            try:
                if region == "us-east-1":
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={
                            "LocationConstraint": region
                        }
                    )

                print(f"Bucket '{bucket_name}' created.")
                return True

            except Exception as create_error:
                print(f"Error creating bucket: {create_error}")
                return False

        elif error_code == 403:
            raise PermissionError(
                f"Bucket '{bucket_name}' exists but access is denied."
            )
        else:
            raise

# ----------------------------------------------------------------------
# UPLOAD LOGIC
def upload_files_to_s3(s3_client, bucket_name, csv_files, prefix="raw"):
    """
    Upload CSV files to S3 with smart overwrite logic:
    - If file doesn't exist -> upload
    - If exists and same size -> skip
    - If exists and different size -> replace
    """

    if not csv_files:
        print("No CSV files found.")
        return

    print(f"Uploading {len(csv_files)} files to s3://{bucket_name}/{prefix}/")

    for file in csv_files:
        file_path = file["file_path"]
        file_name = file["file_name"]
        local_size = file["size_bytes"]

        s3_key = f"{prefix}/{file_name}"

        try:
            # Check if object exists
            response = s3_client.head_object(
                Bucket=bucket_name,
                Key=s3_key
            )

            s3_size = response["ContentLength"]

            # Compare sizes
            if s3_size == local_size:
                print(f"[SKIP] {file_name} (same size)")
                continue
            else:
                print(f"[REPLACE] {file_name} (size mismatch)")

                # Delete old file
                s3_client.delete_object(
                    Bucket=bucket_name,
                    Key=s3_key
                )

                # Upload new file
                s3_client.upload_file(
                    file_path,
                    bucket_name,
                    s3_key
                )

                print(f"[UPLOADED] {file_name}")

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # File does not exist → upload
                try:
                    s3_client.upload_file(
                        file_path,
                        bucket_name,
                        s3_key
                    )
                    print(f"[UPLOAD] {file_name}")

                except Exception as upload_error:
                    print(f"[ERROR] {file_name}: {upload_error}")
            else:
                raise

    print("Upload completed")
    print("-------------------------")

# ----------------------------------------------------------------------
# LIST FILES FROM S3
def list_s3_files(s3_client, bucket_name, prefix="raw"):
    """
    List all files in an S3 bucket under a given prefix
    """
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix
    )

    files = []

    if "Contents" in response:
        for obj in response["Contents"]:
            key = obj["Key"]

            if key.endswith(".csv"):
                file_name = key.split("/")[-1]
                files.append({"file_name": file_name})

    return files