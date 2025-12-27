from pyspark import SparkContext, SparkConf

def get_rdd_kv(row):
    row_splitted = row.split(",")
    user_id = int(row_splitted[0])
    amount = float(row_splitted[2])
    return (user_id,amount)



conf = SparkConf().setMaster("local").setAppName("amount_spent_by_customer")
sc = SparkContext(conf=conf)

input = sc.textFile("C:/Users/user/Desktop/projets/spark-learning/spark_files/customer-orders.csv")

input_kv = input.map(get_rdd_kv).reduceByKey(lambda x,y: x+y)

sorted_kv = input_kv.map(lambda x : (x[1],x[0]) ).sortByKey(ascending=False)

results = sorted_kv.collect()

for spent_amount, user in results:
    print(f"{user} spent {spent_amount:.2f}$")