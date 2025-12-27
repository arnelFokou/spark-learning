from pyspark import SparkConf, SparkContext
import re

def clean_sentences(row):
    row_cleaned = re.compile(r"\W+",re.UNICODE).split(row.lower())
    return row_cleaned

conf = SparkConf().setMaster('local').setAppName("sort_count")
sc = SparkContext(conf=conf)

input = sc.textFile("C:/Users/user/Desktop/projets/spark-learning/spark_files/book.txt")

data_cleaned = input.flatMap(clean_sentences)

data_agg = data_cleaned.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)

data_sorted = data_agg.map(lambda x:(x[1],x[0])).sortByKey(ascending=False)

for data in data_sorted.collect():
    print(data)
