from utils.aws_utils import (
    ensure_bucket_exists,
    upload_files_to_s3,
    create_s3_client
)

from utils.common_utils import (
    load_env,
    download_kaggle_dataset,
    get_csv_metadata
)


def run_pipeline():
    # load env
    aws_access_key, aws_secret_key, region, bucket_name, kaggle_path = load_env()

    # create s3 client
    s3_client = create_s3_client(
        aws_access_key,
        aws_secret_key,
        region
    )

    # download dataset
    dataset_path = download_kaggle_dataset(kaggle_path)

    # ensure bucket
    ensure_bucket_exists(
        s3_client,
        bucket_name,
        region
    )

    # get CSV files
    csv_files = get_csv_metadata(dataset_path)
    print(f"Found {len(csv_files)} CSV files")

    # upload
    upload_files_to_s3(
        s3_client,
        bucket_name,
        csv_files,
        prefix="raw"
    )

    print("Pipeline completed successfully!")

if __name__ == "__main__":
    run_pipeline()