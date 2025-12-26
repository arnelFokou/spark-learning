from pyspark import SparkConf, SparkContext

def extract_useful_data(row):
    row_splitted = row.split(",")
    station_name = row_splitted[0]
    status_tmp = row_splitted[2]
    temperature = (float(row_splitted[3])*0.1)*9/5 + 32
    
    return (station_name,status_tmp,temperature)

conf = SparkConf().setMaster("local").setAppName("min_temperature")
sc = SparkContext(conf=conf)

file = sc.textFile("C:/Users/user/Desktop/projets/spark-learning/spark_files/1800.csv")

data_selected = file.map(extract_useful_data)

# data_filtered =  data_selected.filter(lambda x : "TMAX" == x[1])
data_filtered =  data_selected.filter(lambda x : "TMIN" == x[1])
data_kv = data_filtered.map(lambda x: (x[0],x[2]))
result = data_kv.reduceByKey(lambda x,y : min(x,y)) 

# result = data_kv.reduceByKey(lambda x,y : max(x,y)) 

final_data = result.collect()

for data in final_data:
    print(f"station {data[0]}, temperature {data[1]:.2f}F")