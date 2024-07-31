import streamlit as st
import pandas as pd
import plost, boto3, os, shutil
from dotenv import load_dotenv
import seaborn as sns
import matplotlib.pyplot as plt
from tqdm import tqdm

load_dotenv()

st.set_page_config(initial_sidebar_state='expanded')

# with open('website/style.css') as f:
#     st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Global variables to keep track of processed files and the dataframe
processed_files = set()
combined_df = pd.DataFrame()

def main():
    st.markdown("<h2 style='text-align: center; font-weight: bold;'>Welcome to the dashboard, </br>"+st.session_state.user_data['name']+"!</h2>", unsafe_allow_html=True)

    # Function to get the dataframe
    def get_dataframe():
        s3 = boto3.resource('s3',
                            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'))

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

        # Function to download new Parquet files and combine them into a DataFrame
        def combine_new_parquet_files(prefix='', local_folder='downloads'):
            bucket_name = os.getenv('DEFAULT_BUCKET')

            if not os.path.exists(local_folder):
                os.makedirs(local_folder)

            files = list_s3_files(bucket_name, prefix)
            parquet_files = [file for file in files if file.endswith('.parquet') and file not in processed_files]

            dataframes = []
            progress_bar = st.progress(0)
            for i, file in enumerate(tqdm(parquet_files, desc="Downloading and combining Parquet files")):
                local_path = os.path.join(local_folder, os.path.basename(file))
                download_file(bucket_name, file, local_path)
                df = pd.read_parquet(local_path)
                dataframes.append(df)
                processed_files.add(file)
                progress_bar.progress((i + 1) / len(parquet_files))

            if dataframes:
                new_combined_df = pd.concat(dataframes, ignore_index=True)
            else:
                new_combined_df = pd.DataFrame()

            shutil.rmtree(local_folder)
            return new_combined_df

        prefix = 'data/fraud_data'
        new_df = combine_new_parquet_files(prefix)
        
        global combined_df
        if not combined_df.empty:
            combined_df = pd.concat([combined_df, new_df], ignore_index=True)
        else:
            combined_df = new_df

        return combined_df

    # Store previous metrics in session state
    if 'previous_total_transactions' not in st.session_state:
        st.session_state.previous_total_transactions = 0
    if 'previous_total_frauds' not in st.session_state:
        st.session_state.previous_total_frauds = 0
    if 'previous_fraud_percentage' not in st.session_state:
        st.session_state.previous_fraud_percentage = 0.0
    if 'previous_highest_fraud_amount' not in st.session_state:
        st.session_state.previous_highest_fraud_amount = 0.0

    # Button to refresh the dashboard
    if st.button('Refresh Dashboard'):
        df = get_dataframe()

        # Calculate metrics
        total_transactions = len(df)
        total_frauds = df['fraud'].sum()
        fraud_percentage = (total_frauds / total_transactions) * 100
        highest_fraud_amount = df[df['fraud'] == 1]['amount'].max()

        # Calculate percentage change for metrics
        change_total_transactions = ((total_transactions - st.session_state.previous_total_transactions) / st.session_state.previous_total_transactions) * 100 if st.session_state.previous_total_transactions else 0
        change_total_frauds = ((total_frauds - st.session_state.previous_total_frauds) / st.session_state.previous_total_frauds) * 100 if st.session_state.previous_total_frauds else 0
        change_fraud_percentage = fraud_percentage - st.session_state.previous_fraud_percentage
        change_highest_fraud_amount = ((highest_fraud_amount - st.session_state.previous_highest_fraud_amount) / st.session_state.previous_highest_fraud_amount) * 100 if st.session_state.previous_highest_fraud_amount else 0

        # Update session state with current metrics
        st.session_state.previous_total_transactions = total_transactions
        st.session_state.previous_total_frauds = total_frauds
        st.session_state.previous_fraud_percentage = fraud_percentage
        st.session_state.previous_highest_fraud_amount = highest_fraud_amount

    # If no button is clicked, use the existing dataframe and previous metrics
    else:
        if combined_df.empty:
            df = get_dataframe()
        else:
            df = combined_df

        # Calculate metrics
        total_transactions = len(df)
        total_frauds = df['fraud'].sum()
        fraud_percentage = (total_frauds / total_transactions) * 100
        highest_fraud_amount = df[df['fraud'] == 1]['amount'].max()

        # Set the change to 0 since this is the initial load
        change_total_transactions = 0
        change_total_frauds = 0
        change_fraud_percentage = 0
        change_highest_fraud_amount = 0

    # Display metrics with percentage change
    st.markdown('### Metrics')
    col1, col2, col3, col4 = st.columns(4)
    col1.metric('Total Transactions', total_transactions, f'{change_total_transactions:.2f}%')
    col2.metric('Total Frauds', total_frauds, f'{change_total_frauds:.2f}%')
    col3.metric('Fraud Percentage', f'{fraud_percentage:.2f}%', f'{change_fraud_percentage:.2f}%')
    col4.metric('Highest Fraud Amount', highest_fraud_amount, f'{change_highest_fraud_amount:.2f}%')

    # Row B
    c1, c2 = st.columns((5, 5))
    with c1:
        # Donut Chart: Fraud vs. Non-Fraud Transactions
        st.subheader('Fraud vs. Non-Fraud Transactions')
        fraud_counts = df['fraud'].value_counts()
        plt.figure(figsize=(8, 6), dpi=120)
        plt.pie(fraud_counts, labels=['Non-Fraud', 'Fraud'], autopct='%1.1f%%', startangle=90, pctdistance=0.85, colors=['#66b3ff','#ff6666'], wedgeprops={'edgecolor': 'white'}, textprops={'color':'white'})
        centre_circle = plt.Circle((0, 0), 0.70, fc='white')
        fig = plt.gcf()
        fig.gca().add_artist(centre_circle)
        fig.patch.set_alpha(0.0)
        st.pyplot(plt)

    with c2:
        # Donut Chart: Transaction Types of Frauds
        st.subheader('Transaction Types of Frauds')
        fraud_type_counts = df[df['fraud'] == 1]['type'].value_counts()
        plt.figure(figsize=(8, 6), dpi=120)
        plt.pie(fraud_type_counts, labels=fraud_type_counts.index, autopct='%1.1f%%', startangle=90, pctdistance=0.85, colors=sns.color_palette('pastel'), wedgeprops={'edgecolor': 'white'}, textprops={'color':'white'})
        centre_circle = plt.Circle((0, 0), 0.70, fc='white')
        fig = plt.gcf()
        fig.gca().add_artist(centre_circle)
        fig.patch.set_alpha(0.0)
        st.pyplot(plt)

    # Row C
    c3, c4 = st.columns((5, 5))
    with c3:
        # Plot: Transaction Amount Distribution
        st.subheader('Transaction Amount Distribution')
        plt.figure(figsize=(8, 6))
        sns.histplot(df[df['fraud'] == 0]['amount'], bins=50, kde=True, color='blue', label='Non-Fraud')
        sns.histplot(df[df['fraud'] == 1]['amount'], bins=50, kde=True, color='red', label='Fraud')
        plt.title('Transaction Amount Distribution')
        plt.xlabel('Amount')
        plt.ylabel('Frequency')
        plt.legend()
        st.pyplot(plt)

    with c4:
        # Plot: Fraud Percentage by Transaction Type
        st.subheader('Fraud Percentage by Transaction Type')
        fraud_percentage_by_type = df.groupby('type')['fraud'].mean() * 100
        plt.figure(figsize=(8, 6))
        sns.barplot(x=fraud_percentage_by_type.index, y=fraud_percentage_by_type.values)
        plt.title('Fraud Percentage by Transaction Type')
        plt.xlabel('Transaction Type')
        plt.ylabel('Fraud Percentage')
        st.pyplot(plt)

    # Row D
    c5, c6 = st.columns((5, 5))
    with c5:
        # Plot: Correlation Heatmap
        st.subheader('Correlation Heatmap')
        plt.figure(figsize=(12, 8))
        correlation_matrix = df.corr(numeric_only=True)
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', linewidths=0.5)
        plt.title('Correlation Heatmap')
        st.pyplot(plt)

    with c6:
        # Plot: Top 10 Highest Fraud Transactions
        st.subheader('Top 10 Highest Fraud Transactions')
        top_fraud_transactions = df[df['fraud'] == 1].nlargest(10, 'amount')
        plt.figure(figsize=(8, 6))
        sns.barplot(x=top_fraud_transactions['amount'], y=top_fraud_transactions.index, orient='h')
        plt.title('Top 10 Highest Fraud Transactions')
        plt.xlabel('Amount')
        plt.ylabel('Transaction Index')
        st.pyplot(plt)

    # Display fraud rows with "Report False" button
    st.markdown('### Fraud Transactions')
    fraud_rows = df[df['fraud'] == 1]

    for index, row in fraud_rows.iterrows():
        st.write(row.to_dict())
        if st.button('Report False', key=f'report_{index}'):
            # Placeholder function to handle reporting false positives
            def report_false_positive(unique_id):
                st.sidebar.success(f"Transaction with unique ID {unique_id} \
                                    has been reported as a false positive.")
            
            report_false_positive(row['id'])

if __name__ == '__main__':
    # Checking if the user is logged in
    try:
        if st.session_state.user is None:
            st.error("Please login to access this page.")
        else:
            main()
    except:
            st.error("Please login to access this page.")