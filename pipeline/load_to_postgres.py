import psycopg2
from pyspark.sql import DataFrame
from config import host,user,database,password

def create_table():
    try:
        conn = psycopg2.connect(database=database, user=user, password=password, host=host, port="5432")
        cursor = conn.cursor()

        create_table_query = '''
           CREATE TABLE IF NOT EXISTS orders (
                "Order ID" VARCHAR(255) NOT NULL,
                "Order Date" DATE,
                "Ship Date" DATE,
                "Ship Mode" VARCHAR(255) NOT NULL,
                "Customer ID" VARCHAR(255) NOT NULL,
                "Customer Name" VARCHAR(255) NOT NULL,
                "Segment" VARCHAR(255) NOT NULL,
                "Country/Region" VARCHAR(255) NOT NULL,
                "City" VARCHAR(255) NOT NULL,
                "State" VARCHAR(255) NOT NULL,
                "Postal Code" INTEGER,
                "Region" VARCHAR(255) NOT NULL,
                "Product ID" VARCHAR(255) NOT NULL,
                "Category" VARCHAR(255) NOT NULL,
                "Sub-Category" VARCHAR(255) NOT NULL,
                "Product Name" VARCHAR(255) NOT NULL,
                "Sales" DOUBLE PRECISION,
                "Quantity" INTEGER,
                "Discount" DOUBLE PRECISION,
                "Profit" DOUBLE PRECISION NOT NULL,
                PRIMARY KEY ("Order ID")
            );
        '''
        cursor.execute(create_table_query)
        conn.commit()
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        cursor.close()
        conn.close()

def load_data_to_postgres(df: DataFrame):
    df.write \
        .format("jdbc") \
        .option('batchsize', 1000) \
        .option("url", "jdbc:postgresql://192.168.1.37:5432/sales") \
        .option("dbtable", "orders") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .mode('append') \
        .save()
