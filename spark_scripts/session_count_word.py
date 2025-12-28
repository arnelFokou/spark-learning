from pyspark.sql import SparkSession, functions as func


spark = SparkSession.builder.appName("word count").getOrCreate()

# attempt to import the data
input = spark.read.text("C:/Users/user/Desktop/projets/spark-learning/spark_files/book.txt")


#transform the text by splitting using regex and func.explode that works like flatMap. the outcome will be the column with name -> word
words_df = input.select( func.explode(func.split(func.lower(input.value),"\\W+")).alias("word"))

# discard the emoty value 
words_filtered = words_df.filter(words_df.word != "" )

outcome = words_filtered.groupby("word").count().sort("count")

outcome.show(outcome.count())



spark.stop()