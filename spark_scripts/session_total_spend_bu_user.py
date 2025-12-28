from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType,FloatType

custom_schema = StructType([
    StructField("user_id",IntegerType(),True),
    StructField("product_id",IntegerType(),True),
    StructField("amount_spent",FloatType(),True),
])

spark = SparkSession.builder.appName("amountSpend").getOrCreate()

input = spark.read.schema(custom_schema).csv("C:/Users/user/Desktop/projets/spark-learning/spark_files/customer-orders.csv")

input_selected = input.select("user_id","amount_spent")

input_agg = input_selected.groupby("user_id").agg(
    func.round(func.sum("amount_spent"),2).alias("total_spent")
)

input_agg_sorted = input_agg.orderBy("total_spent",ascending=False)

input_agg_sorted.show(5)

spark.stop()