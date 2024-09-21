from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import sys
import os
from pyspark.sql import SparkSession

base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(base_dir)

# Import your custom functions
from pipeline.read_excel import read_spark_in_batches
from pipeline.transform_data import transform_data 
from pipeline.load_to_postgres import create_table, load_data_to_postgres  

# Create or reuse a Spark session
def start_spark_session():
    return SparkSession.builder\
        .appName("Data-Processing")\
        .config("spark.master", "local[*]")\
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")\
        .getOrCreate()

# Default arguments for the DAG
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'csv_to_postgres_in_batches_pipeline',
    default_args=default_args,
    description='A pipeline to read, transform, and store data from CSV to Postgres in batches',
    schedule=timedelta(days=1),
)

# Task 1: Reset the start_row to 0
def reset_start_row(**kwargs):
    Variable.set('start_row', 0)
    print("Reset start_row to 0")

reset_row_task = PythonOperator(
    task_id='reset_start_row',
    python_callable=reset_start_row,
    dag=dag,
)

# Task 2: Read data in batches and save to a temporary file
def read_data_in_batches(**kwargs):
    spark = start_spark_session()
    start_row = Variable.get('start_row', default_var=0)
    start_row = int(start_row)
    file_path = '/home/kapiushon05/airflow/dags/Sales_Data_Pipeline/Data.csv'

    df = read_spark_in_batches(spark,file_path, start_row, batch_size=4000)
    
    temp_file_path = '/tmp/spark_temp.parquet'
    df.write.parquet(temp_file_path, mode='overwrite')
    
    print(f"Data saved to temporary file: {temp_file_path}")

read_task = PythonOperator(
    task_id='read_data_in_batches',
    python_callable=read_data_in_batches,
    dag=dag,
)

# Task 3: Transform data from the temporary file
def transform_data_task(**kwargs):
    spark = start_spark_session()
    
    temp_file_path = '/tmp/spark_temp.parquet'
    df = spark.read.parquet(temp_file_path)
    
    transformed_df = transform_data(df,spark)
    
    transformed_file_path = '/tmp/spark_transformed.parquet'
    transformed_df.write.parquet(transformed_file_path, mode='overwrite')
    
    print(f"Transformed data saved to: {transformed_file_path}")

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data_task,
    dag=dag,
)

# Task 4: Load the transformed data to PostgreSQL
def load_to_postgres_task(**kwargs):
    spark = start_spark_session()

    transformed_file_path = '/tmp/spark_transformed.parquet'
    df = spark.read.parquet(transformed_file_path)
    df.printSchema()
    df.select('Order Date', 'Ship Date').show(5)
    create_table()
    load_data_to_postgres(df)
    
    print("Data loaded to PostgreSQL")

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres_task,
    dag=dag,
)

# Task 5: Update the start_row after successful load
def update_start_row(**kwargs):
    start_row = Variable.get('start_row', default_var=0)
    start_row = int(start_row) + 4000
    Variable.set('start_row', start_row)
    print("Updated start_row to:", start_row)

update_row_task = PythonOperator(
    task_id='update_start_row',
    python_callable=update_start_row,
    dag=dag,
)

# Define the task dependencies
reset_row_task >> read_task >> transform_task >> load_task >> update_row_task
