from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("APP") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "quickstart-events") \
    .option("startingOffsets", "earliest") \
    .load()

#df.show(10)

#df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#df.printSchema()

df2 = df.selectExpr("CAST(VALUE AS STRING) AS VALUE") 
schema_json= StructType([
    StructField("Date/Time", StringType(), True),
    StructField("LV_ActivePower", DoubleType(), True),
    StructField("Wind_Speed", DoubleType(), True),
    StructField("Theoretical_Power_Curve", DoubleType(), True),
    StructField("Wind_Direction", DoubleType(), True)
])

json_expanded_df = df2.withColumn("value", from_json(df2["value"], schema_json)).select("value.*")
query = json_expanded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()\
    .awaitTermination()  
