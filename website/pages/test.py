import boto3
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()


# Function to list all objects in an S3 bucket
def list_s3_files(bucket_name, prefix=''):
    s3 = boto3.client('s3', 
                        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                        aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
                        region_name='ap-south-1'
                    )
    
    print(os.getenv('AWS_ACCESS_KEY'))
    print(os.getenv('AWS_SECRET_KEY'))

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = []
    for obj in response.get('Contents', []):
        files.append(obj['Key'])
    return files

# Function to download a file from S3
def download_file(bucket_name, s3_key, local_path):
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, s3_key, local_path)

# Function to download all Parquet files and combine them into a DataFrame
def combine_parquet_files(bucket_name, prefix='', local_folder='downloads'):
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
    return combined_df

# Define your bucket name and prefix (if any)
bucket_name = 'streaming-fraud-data'
prefix = 'data/fraud_data'  # If your files are in a subfolder, set the prefix here

# Combine Parquet files and show the DataFrame
combined_df = combine_parquet_files(bucket_name, prefix)
print(combined_df)
