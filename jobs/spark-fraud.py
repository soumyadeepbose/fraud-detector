from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import from_json, col
from config import config
# import pandas as pd
import joblib

model = joblib.load('/opt/bitnami/spark/jobs/best_model.pkl')

def main():
    spark = SparkSession.builder \
        .appName('FraudDetection') \
        .config('spark.jars.packages', 
                'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,'
                'org.apache.hadoop:hadoop-aws:3.3.1,'
                'com.amazonaws:aws-java-sdk:1.11.469') \
        .config('spark.hadoop.fs.s3a.impl', 
                'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.access.key', config.get('AWS_ACCESS_KEY')) \
        .config('spark.hadoop.fs.s3a.secret.key', config.get('AWS_SECRET_KEY')) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()
    
    # Minimizing console output
    # spark.sparkContext.setLogLevel('WARN')

    # Fraud Schema
    # fraud_schema = StructType([
    #     StructField('unique_id', StringType(), True),
    #     StructField('company_id', StringType(), True),
    #     StructField('transaction_id', StringType(), True),
    #     StructField('transaction_time', TimestampType(), True),
    #     StructField('transaction_amount', IntegerType(), True)
    # ])
    
    fraud_schema = StructType([
        StructField('unique_id', StringType(), True),
        StructField('transaction_time', TimestampType(), True),
        StructField('transaction_date', StringType(), True),
        StructField('step', IntegerType(), True),
        StructField('amount', DoubleType(), True),
        StructField('oldbalanceOrig', DoubleType(), True),
        StructField('newbalanceOrig', DoubleType(), True),
        StructField('oldbalanceDest', DoubleType(), True),
        StructField('newbalanceDest', DoubleType(), True),
        StructField('type_Encoded', IntegerType(), True),
        StructField('type', StringType(), True),
    ])

    fraud_schema_with_predictions = StructType([
        StructField('unique_id', StringType(), True),
        StructField('transaction_time', TimestampType(), True),
        StructField('transaction_date', StringType(), True),
        StructField('step', IntegerType(), True),
        StructField('amount', DoubleType(), True),
        StructField('oldbalanceOrig', DoubleType(), True),
        StructField('newbalanceOrig', DoubleType(), True),
        StructField('oldbalanceDest', DoubleType(), True),
        StructField('newbalanceDest', DoubleType(), True),
        StructField('type_Encoded', IntegerType(), True),
        StructField('type', StringType(), True),
        StructField('fraud', IntegerType(), True)
    ])

    def read_kafka_topic(topic, schema):
        return spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', topic) \
            .option('startingOffsets', 'earliest') \
            .load() \
            .selectExpr('CAST(value AS STRING)') \
            .select(from_json(col('value'), schema).alias('data')) \
            .select('data.*') \
            .withWatermark('transaction_time', '2 minutes')

    def stream_writer(input: DataFrame, checkpoint_location, output):
        return input.writeStream \
            .format('parquet') \
            .option('checkpointLocation', checkpoint_location) \
            .option('path', output) \
            .outputMode('append') \
            .start()

    fraud_df = read_kafka_topic('fraud_test_topic_1', fraud_schema)\
                    .alias('fraud')
    
    # # Converting to Pandas Dataframe for predictions
    # fraud_df_pandas = fraud_df.toPandas()
    # fraud_df_pandas['fraud'] = model.predict(fraud_df_pandas[['step', 
    #                                                           'amount', 
    #                                                           'oldbalanceOrig',
    #                                                           'newbalanceOrig',
    #                                                           'oldbalanceDest',
    #                                                           'newbalanceDest',
    #                                                           'type_Encoded'
    #                                                         ]])
    
    # #Converting back to Spark DataFrame
    # fraud_df = spark.createDataFrame(fraud_df_pandas, 
    #                                  schema=fraud_schema_with_predictions)
    
    # Define a Pandas UDF for making predictions
    @pandas_udf(fraud_schema_with_predictions, PandasUDFType.GROUPED_MAP)
    def predict_fraud(fraud_pdf):
        fraud_pdf['fraud'] = model.predict(fraud_pdf[['step', 
                                                    'amount', 
                                                    'oldbalanceOrig',
                                                    'newbalanceOrig',
                                                    'oldbalanceDest',
                                                    'newbalanceDest',
                                                    'type_Encoded'
                                                ]])
        return fraud_pdf

    # Apply the Pandas UDF
    fraud_df = fraud_df.groupBy().apply(predict_fraud)

    stream_writer(fraud_df, 's3a://streaming-fraud-data/checkpoints/fraud_data', 
                            's3a://streaming-fraud-data/data/fraud_data').awaitTermination()

if __name__ == '__main__':
    main()