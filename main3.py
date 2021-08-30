from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Window
from datetime import datetime
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.window import Window
from pyspark.sql.functions import sum
from pyspark.sql.functions import lag

import pyodbc
import pyspark.sql.functions as F

schema = StructType([
    StructField("BIRTHDAY_PROMO_TO", DateType(), True),
    StructField("DFL_TELCO", IntegerType(), True),
    StructField("DFL_INSURANCE_HOME", IntegerType(), True),
    StructField("CUSTOMER_ID", StringType(), True),
    StructField("DFL_LUZ", IntegerType(), True),
    StructField("BIRTHDAY_DISCOUNT_REASON", StringType(), True),
    StructField("BIRTHDAY_DISCOUNT_DESCRIPTION", StringType(), True),
    StructField("PERMISSION_COMUNICATION", IntegerType(), True),
    StructField("DFL_GAS", IntegerType(), True),
    StructField("MAIL", StringType(), True),
    StructField("PROMO_BIRTHDAY", IntegerType(), True),
    StructField("BIRTHDAY_PROMO_SINCE", DateType(), True),
    StructField("CUSTOMER_ID_PH", IntegerType(), True)])

schemaSales = StructType([
    StructField("CUSTOMER_ID", IntegerType(), True),
    StructField("DISCOUNT_REASON", StringType(), True),
    StructField("DISCOUNT_DESC", StringType(), True),
    StructField("CLOSED_DATE", TimestampType(), True),
    StructField("FAMILIA_LTV", StringType(), True),
    StructField("SHOP_ID", IntegerType(), True),
    StructField("SHOP_CITY", StringType(), True),
    StructField("TRANSACTION", StringType(), True),
    StructField("PRODUCT_QUANTITY", StringType(), True),
    StructField("TOTAL_DISCOUNT_AMOUNT", FloatType(), True),
    StructField("TOTAL_RETAIL_AMOUNT", FloatType(), True)])

schemaTimestamp = StructType([StructField("timestamp", TimestampType(), True)])

# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("Spark") \
    .getOrCreate()

# Kafka Consumer
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales-details-topic") \
    .load() \
    .select(from_json(col("value").cast("string"), schemaSales).alias("data")) \
    .select("data.*")

# Spark reads from Kafka Topic
'''df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "customer-details-topic") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")'''

'''
# Print data in console
df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()
'''

# ODBC connection string to Azure SQL Server
conn = 'Driver={ODBC Driver 13 for SQL Server};Server=tcp:tfgalga.database.windows.net,1433;' \
       'Database=ClientesYPromociones;Uid=dataLake-alga;Pwd={Tfgserver!};Encrypt=yes;' \
       'TrustServerCertificate=no;Connection Timeout=30;'

retailsAmount = df \
    .withWatermark("CLOSED_DATE", "1 hour") \
    .groupBy(window(df.CLOSED_DATE, "1 hour", "1 hour")) \
    .agg(sum("TOTAL_RETAIL_AMOUNT").alias("sum"))


# Writing in SQL Server
def foreach_batch_function(df, epoch_id):
    data = df.select('*').collect()
    print(data)

    connStr = pyodbc.connect(conn, autocommit=False)
    cursor = connStr.cursor()

    for row in data:
        windowTime = row['window']

        cursor.execute("DELETE "
                       "FROM dbo.RETAILAMOUNT "
                       "WHERE TIMESTART='" + str(windowTime['start']) +
                       "' AND TIMEEND='" + str(windowTime['end']) + "'")

        cursor.execute("INSERT INTO dbo.RETAILAMOUNT("
                       "[TIMESTART],"
                       "[TIMEEND],"
                       "[AMOUNT])"
                       "values(?, ?, ?)",
                       windowTime['start'], windowTime['end'], row["sum"])

        connStr.commit()

    cursor.close()
    connStr.close()


retailsAmount.writeStream.foreachBatch(foreach_batch_function).outputMode("update").start().awaitTermination()

'''
retailsAmount \
    .writeStream \
    .outputMode('complete') \
    .format('console') \
    .option('truncate', 'false') \
    .start() \
    .awaitTermination()
'''
