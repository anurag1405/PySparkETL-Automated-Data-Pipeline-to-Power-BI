from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def read_spark_in_batches(spark,file_path, start_row, batch_size=3000):
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        batch_df = df.limit(batch_size).offset(start_row)   
        return batch_df
    except Exception as e:
        logging.error(f"Error in reading or processing batch: {e}")
        return "Failed"

