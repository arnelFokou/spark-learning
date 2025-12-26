def get_rdd_kv(row):
    row_splitted = row.split(",")
    age = int(row_splitted[2])
    friends = int(row_splitted[3])
    return (age,friends)


from pyspark import SparkContext,SparkConf

conf = SparkConf().setMaster('local').setAppName("AvgFriendsByUser")
sc = SparkContext(conf=conf)

rdd = sc.textFile("C:/Users/user/Desktop/projets/spark-learning/spark_files/fakefriends.csv")

rdd1 = rdd.map(get_rdd_kv)

rdd2 = rdd1.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1]))

rdd3 = rdd2.mapValues(lambda x: x[0]/x[1])

results = sorted(rdd3.collect())
for k,v in results:
   print(f"La moyenne d'amis pour l'age {k} est {round(v,2)}")