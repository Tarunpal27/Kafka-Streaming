from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark =SparkSession.builder.appName("Read CSV").getOrCreate()

schema = StructType([
    StructField("Date/Time", StringType(), True),
    StructField("LV_ActivePower", DoubleType(), True),
    StructField("Wind_Speed", DoubleType(), True),
    StructField("Theoretical_Power_Curve", DoubleType(), True),
    StructField("Wind_Direction", DoubleType(), True)   
])

df = spark.read \
    .schema(schema).csv("/home/xs442-tarsin/kafka/data/data.csv", header=True)

df.show(5)

df.selectExpr("to_json(struct(*)) AS value").write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "quickstart-events") \
    .save()

query=df.select()




