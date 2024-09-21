from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType, DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import col,to_date

def transform_data(spark_df,spark):
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    string_cols = [field.name for field in spark_df.schema.fields if isinstance(field.dataType, StringType)]
    integer_cols = [field.name for field in spark_df.schema.fields if isinstance(field.dataType, DoubleType)]
    mode_values = {}
    for column in string_cols:
        mode_value = spark_df.groupBy(column).count().orderBy(F.desc("count")).first()[0]
        mode_values[column] = mode_value
    
    mean = {}
    for column in integer_cols:
        mean_value = spark_df.select(F.mean(F.col(column))).first()[0]
        mean[column] = mean_value
    
    window_spec = Window.partitionBy("City")
    df_filled = spark_df.withColumn(
        "Postal Code",
        F.when(spark_df["Postal Code"].isNull(), F.first("Postal Code", ignorenulls=True).over(window_spec))
        .otherwise(spark_df["Postal Code"])
    )
    transformed_df = df_filled.fillna(mode_values).fillna(mean)
    
    final_df = transformed_df \
        .withColumn('Order Date', to_date(col('Order Date'), 'dd/MM/yyyy')) \
        .withColumn('Ship Date', to_date(col('Ship Date'), 'dd/MM/yyyy'))\
        .withColumn('Sales', col('Sales').cast(DoubleType())) \
        .withColumn('Quantity', col('Quantity').cast(IntegerType())) \
        .withColumn('Discount', col('Discount').cast(DoubleType()))
    return final_df
