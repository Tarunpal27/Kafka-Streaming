from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import *
 
spark = SparkSession \
    .builder \
    .appName("DeltaTutorial") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

#Read Delta table

df = spark.read.format("delta").load("/home/xs442-tarsin/kafka/deltaTable")
df.show()

# Count distinct signal_ts per day

df.createOrReplaceTempView("df2")
spark.sql("select signal_date,count(distinct(signal_ts)) from df2 group by signal_date").show()

#Add column

df = df.withColumn("generation_indicator",when(col("signals.LV_ActivePower") < 200, "Low")\
    .when((col("signals.LV_ActivePower") >= 200) & (col("signals.LV_ActivePower") < 600), "Medium")\
    .when((col("signals.LV_ActivePower") >= 600) & (col("signals.LV_ActivePower") < 1000), "High")\
    .otherwise("Exceptional"))
 
df.show(10)



json_df = [
    {"sig_name": "LV_ActivePower", "sig_mapping_name": "LV_ActivePower_average"},
    {"sig_name": "Wind_Speed", "sig_mapping_name": "Wind_Speed_average"},
    {"sig_name": "Theoretical_Power_Curve", "sig_mapping_name": "Theoretical_Power_Curve_average"},
    {"sig_name": "Wind_Direction", "sig_mapping_name": "Wind Direction_average"}
]
 
new_df = spark.createDataFrame([Row(**x) for x in json_df])
new_df.show()


 
# perform brodcast join between 4th and 5th
broadcast_df=df.join(broadcast(new_df),df["generation_indicator"] == new_df["sig_mapping_name"],"left_outer")
 
# broadcast_df=broadcast_df.withColumn("generation_indicator",broadcast_df.sig_mapping_name)
# broadcast_df = broadcast_df.drop("sig_name", "sig_mapping_name")
broadcast_df.show()
