# https://towardsdev.com/reading-bigquery-table-in-pyspark-9853d60095d9

#1 - Loading a Google BigQuery table into a DataFrame
# Initializing SparkSession
from pyspark.sql import SparkSession
print("----------------------------------")
spark = SparkSession.builder.master('local[*]')\
    .appName('spark-read-from-bigquery')\
            .getOrCreate()
# Creating DataFrames
df = spark.read.format('bigquery')\
    .option('project','sujit-use-case')\
    .option('table', 'test_dataset.test') \
    .option('temporaryGcsBucket', 'sujit_bucket_11')\
        .load()
# 2 - Print the Google BigQuery table
df.show()
df.printSchema()
# 3 - Creating or replacing a local temporary view with this DataFrame
df.createOrReplaceTempView("test")
# Perform select order_id
peopleCountDf = spark.sql("SELECT * from test")
print("----------------------------------")
# Display the content of df
peopleCountDf.show()