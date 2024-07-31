import boto3
import pandas as pd
import os
import shutil
from dotenv import load_dotenv
load_dotenv()

s3 = boto3.resource('s3', 
                        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                        aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
                    )

# Function to list all objects in an S3 bucket
def list_s3_files(bucket_name, prefix=''):
    
    # print(os.getenv('AWS_ACCESS_KEY'))
    # print(os.getenv('AWS_SECRET_KEY'))

    files = []
    s3_bucket = s3.Bucket(bucket_name)
    for s3_file in s3_bucket.objects.all():
        files.append(s3_file.key)
    print(files)
    return files

# Function to download a file from S3
def download_file(bucket_name, s3_key, local_path):
    s3.Bucket(bucket_name).download_file(s3_key, local_path)

# Function to download all Parquet files and combine them into a DataFrame
def combine_parquet_files(prefix='', local_folder='downloads'):
    bucket_name = os.getenv('DEFAULT_BUCKET')

    if not os.path.exists(local_folder):
        os.makedirs(local_folder)
    
    files = list_s3_files(bucket_name, prefix)
    parquet_files = [file for file in files if file.endswith('.parquet')]
    
    dataframes = []
    for file in parquet_files:
        local_path = os.path.join(local_folder, os.path.basename(file))
        download_file(bucket_name, file, local_path)
        df = pd.read_parquet(local_path)
        dataframes.append(df)
    
    combined_df = pd.concat(dataframes, ignore_index=True)
    shutil.rmtree(local_folder)
    return combined_df

def get_dataframe():
    # Define your bucket name and prefix (if any)
    prefix = 'data/fraud_data'

    # Combine Parquet files and show the DataFrame
    combined_df = combine_parquet_files(prefix)
    return combined_df

print(get_dataframe().to_csv("dataframe.csv", index=False))
