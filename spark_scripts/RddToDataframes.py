from pyspark.sql import SparkSession, Row

# create a sparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

#A Row represent each line on your dataframe and it should be named
def get_structured(row):
    row_splitted = row.split(",")
    return Row(ID = int(row_splitted[0]), name = str(row_splitted[1].encode('utf-8')),age = int(row_splitted[2]), numFriends = int(row_splitted[3]))

#import csv_file using sparkContext
input = spark.sparkContext.textFile("C:/Users/user/Desktop/projets/spark-learning/spark_files/fakefriends.csv")
print(input)
#map every row of the file with the function
input_structured = input.map(get_structured)

#create a dataframe and store it in memory
schema_people = spark.createDataFrame(input_structured).cache()
print(schema_people)
#create a view to manipulate your dataset easily
schema_people.createOrReplaceTempView("people")

#apply an sql function 
teenagers = spark.sql("select * from people where age between 13 and 19")


# print(teenagers.take(5))


# schema_people.groupBy("age").count().orderBy("age").show(5)


#stop the session, useful to get free the space allocated in memory to process your data
spark.stop()
