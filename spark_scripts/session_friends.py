from pyspark.sql import SparkSession

# this line attempt to create a sparkSession Object
spark = SparkSession.builder.appName("test").getOrCreate()

#Here we attempt to import our data file and specify to spark that the first line has to be considered as the header and tells to spark to automatically detect the data types of each column
input = spark.read.option("header", "true").option("inferSchema","true").csv("C:/Users/user/Desktop/projets/spark-learning/spark_files/fakefriends-header.csv")

# we print the schema of our data. spark will print in the console the data types of each column
print("Here is our inferred Schema")
input.printSchema()

input.createOrReplaceTempView("people")
print("request with sql command")
print(spark.sql("select * from people where age < 21").show())

#here, he just print users with age under 2
print("request with built in functions of spark sql")
# input.filter(input.age<21).show()


# input.groupby(input.age).sum("friends").show(5)

spark.stop()
