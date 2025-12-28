from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

custom_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("Date",IntegerType(),True),
    StructField("measure_type",StringType(),True),
    StructField("temperature",FloatType(),True)  ])

spark = SparkSession.builder.appName("temperature_station").getOrCreate()

input = spark.read.schema(custom_schema).csv("C:/Users/user/Desktop/projets/spark-learning/spark_files/1800.csv")

input_selected = input.filter(input.measure_type == "TMIN").select("station_id","temperature")

input_agg = input_selected.groupby("station_id").agg(
    func.min("temperature").alias("min_temperature"),
    func.max("temperature").alias("max_temperature")

)

outcome = input_agg.withColumn("min_temperature",
                    func.round((func.col("min_temperature") *0.1 *9/5 +32),2))\
                    .withColumn("max_temperature",
                    func.round((func.col("max_temperature") * 0.1 *9/5 +32),2))\
                    .select("station_id","min_temperature","max_temperature")



# result in sql with temperature in farheneit
# input_agg.createOrReplaceTempView("weather")
# outcome = spark.sql("select station_id, round((min_temperature * 0.1 *9/5 +32),2) as min, round((max_temperature * 0.1 *9/5 +32),2) as max from weather")




outcome.show(5)