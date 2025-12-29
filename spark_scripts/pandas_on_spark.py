from pyspark.sql import SparkSession
# Must set this env variable to avoid warnings
import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
import pyspark.pandas as ps  # Import pandas-on-Spark

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Pandas API on Spark") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
    .getOrCreate()

df = ps.DataFrame(
    {
        "user_id": [1,2,3,4,5,6],
        "name": ["fokou", "fotso", "arnel", "delene", "jeff", "charvet"],
        "age":[12,23,32,15,28,17],
    }
)

print("### describe of data using pandas_spark   ###")
# print(df.describe())

print("### transformation of data using pandas_spark  ###")
def add_age(x) -> int:
    return x+100

user_majeur = df[df['age']>21]
user_majeur['age']=user_majeur['age'].apply(add_age)
print(user_majeur)


print("###  switch to dataframe spark    ###")
data = user_majeur.to_spark()
data_filtered = data.filter(data.name=='fotso')
data_filtered.show()

print("### switch back from spark dataframe to pandas on spark dataframe")
dsp = ps.DataFrame(data_filtered)
print(dsp)

spark.stop()