from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Read from Kafka using pyspark") \
    .master("local[*]") \
    .getOrCreate()
    

