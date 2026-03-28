from utils.common_utils import setup_environment, process_table
from utils.aws_utils import list_s3_files
from utils.spark_utils import read_csv

def load_raw_data(folder="raw"):
    # setup
    spark, s3_client, bucket_name = setup_environment()
    print("Setup Completed")
    
    # get files
    s3_files = list_s3_files(s3_client, bucket_name, folder)

    # read
    print("Reading Tables ...")
    dfs = read_csv(spark, bucket_name, folder, s3_files)
    print("Reading Completed")

    return dfs, bucket_name


if __name__ == "__main__":
    dfs, bucket_name = load_raw_data("raw")
    print('=====================================')
    
    process_table(dfs, bucket_name, "teamstats")
    print('=====================================')
    
    process_table(dfs, bucket_name, "teams")
    print('=====================================')
    
    process_table(dfs, bucket_name, "players")
    print('=====================================')

    process_table(dfs, bucket_name, "shots")
    print('=====================================')
    
    process_table(dfs, bucket_name, "leagues")
    print('=====================================')

    process_table(dfs, bucket_name, "games")
    print('=====================================')

    process_table(dfs, bucket_name, "appearances")


