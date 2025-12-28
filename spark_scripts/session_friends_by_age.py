from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

input = spark.read.option("header","true").option("inferSchema","true").csv("C:/Users/user/Desktop/projets/spark-learning/spark_files/fakefriends-header.csv")
##      Spark SQL       ##
# input.createOrReplaceTempView("people")

# count_users = spark.sql("select age, round(avg(friends),2) as avg_friends from people group by age order by age asc")

# print(count_users.show())

##      Spark Python    ##

input_selected = input.select("age","friends")

outcome = input_selected.groupby("age").agg(
    func.round(func.avg("friends")).alias("avg_friends")
).sort("age")

outcome.show(5)

spark.stop()
