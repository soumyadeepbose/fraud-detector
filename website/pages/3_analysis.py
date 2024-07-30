import streamlit as st
import pandas as pd
import plost, boto3, os, shutil
from dotenv import load_dotenv
load_dotenv()

st.set_page_config(layout='wide', initial_sidebar_state='expanded')

with open('website/style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', 
                unsafe_allow_html=True
            )
    
# Getting the dataframe
def get_dataframe():
    s3 = boto3.resource('s3', 
                        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                        aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
                    )
    
    # Function to list all objects in an S3 bucket
    def list_s3_files(bucket_name, prefix=''):
        files = []
        s3_bucket = s3.Bucket(bucket_name)
        for s3_file in s3_bucket.objects.all():
            files.append(s3_file.key)
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
    
    prefix = 'data/fraud_data'
    combined_df = combine_parquet_files(prefix)
    return combined_df

df = get_dataframe()

# Row A
st.markdown('### Metrics')
col1, col2, col3 = st.columns(3)
col1.metric("Fraud Percentage", "7%", "0.87%")
col2.metric("Transaction Rate", "12 /min", "-1 /min")
col3.metric("Total Transactions", "6", "- low")

# Row B
# c1, c2 = st.columns((7,3))
# with c1:
#     st.markdown('### Heatmap')
#     plost.time_hist(
#     data=seattle_weather,
#     date='date',
#     x_unit='week',
#     y_unit='day',
#     color=time_hist_color,
#     aggregate='median',
#     legend=None,
#     height=345,
#     use_container_width=True)
# with c2:
#     st.markdown('### Donut chart')
#     plost.donut_chart(
#         data=stocks,
#         theta=donut_theta,
#         color='company',
#         legend='bottom', 
#         use_container_width=True)

# Row C
st.markdown('### Fraud Analysis Chart')
# st.line_chart(df, 
