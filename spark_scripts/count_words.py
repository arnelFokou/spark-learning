from pyspark import SparkConf,SparkContext
import re

"""
clean_sentences is a function that takes a row of RDD, transform each word to lower, then
deletes all the special characters as \n,\t,... and punctuatons
"""
def clean_sentences(row):
    row_cleaned = re.compile(r"\W+",re.UNICODE).split(row.lower())
    return row_cleaned

conf = SparkConf().setMaster("local").setAppName("WordCounts")
sc = SparkContext(conf = conf)

input = sc.textFile("C:/Users/user/Desktop/projets/spark-learning/spark_files/book.txt")
flatten_input = input.flatMap(clean_sentences)

result = flatten_input.countByValue()

for word,count in result.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):    
        print(cleanWord.decode() + " " + str(count))
