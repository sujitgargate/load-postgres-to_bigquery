
import time
import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, FloatType

start = time.perf_counter()
# findspark.find()
findspark.init('C:\SPARK_HOME\spark-3.3.1-bin-hadoop3')

#use config for env
#read() write()
#use tuple for vars
os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'

#As of now, pyawworw does not support MapType, ArrayType of TimestampType, Nested StructTyp
#Follow readme.md #1 link for more info

spark = SparkSession.builder.master("local[*]")\
        .appName("OracleToBigQuery")\
        .config("spark.jars", "resources\postgresql-42.5.1.jar")\
        .getOrCreate()

        #   .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
        #   .config("spark.jars", "resources\spark-bigquery-with-dependencies_2.13-0.27.0.jar")\

#use persist

# employeesDF = spark.read.format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/ecommerce_source_db") \
#     .option("dbtable", "employees") \
#     .option("user", "postgres") \
#     .option("password", "postgres") \
#     .option("driver", "org.postgresql.Driver") \
#     .load()

def readFromPostgresDB(table):
    return spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/retail_db") \
    .option("dbtable", "{}".format(table)) \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

order_itemsDF= readFromPostgresDB("order_items")
print("order_itemsDF", order_itemsDF.count())
order_itemsDF.show(2)

ordersDF= readFromPostgresDB("orders")
print("ordersDF", ordersDF.count())
ordersDF.show(2)

#add below
#group by, cust id, cust name, daily order count() =  day count() under col,


newDF = ordersDF.join(order_itemsDF, ordersDF.order_id ==  order_itemsDF.order_item_id)

# if null,Empty = use default
filteredDF = newDF.withColumn("order_status_updated", when(newDF.order_status == "PROCESSING", "Waiting").otherwise(newDF.order_status))

# dropped column[order_status] as we have new updated column as order_status_updated
filteredDF = filteredDF.drop("order_status")

print(filteredDF.count())
# filteredDF.show(10, truncate=False)


print(filteredDF.count())
# using regex to filter data

# newDF1 = filteredDF.filter(filteredDF["order_status_updated"].rlike("%Waiting%"))

newDF1 = filteredDF.filter(col("order_status_updated") == "Waiting")

print(newDF1.count())

newDF2 = filteredDF.filter(filteredDF.order_status_updated.rlike("Waiting"))
print(newDF2.count())


#filter null values before applying udf to column, null values causes to raise exceptions while in execution
# make udf to convert time zone
#put nulls in some cols
#check for nulls

def convertINRToUSD(INRCurrencyAMT):
    usd = float(INRCurrencyAMT)/80.00
    return float(usd)

convertCurrencyUDF =  udf(lambda x : convertINRToUSD(x), FloatType())
# spark.udf.register("convertCurrencyUDF", convertINRToUSD,FloatType())

filteredDF = filteredDF.filter(filteredDF.order_item_subtotal.isNotNull())

filteredDF =filteredDF.withColumn("order_item_subtotal_USD", convertCurrencyUDF(col("order_item_subtotal")))
# print(filteredDF.take(4))

# filteredDF = filteredDF.select(col("order_item_subtotal") , convertCurrencyUDF(col("order_item_subtotal")).alias("order_item_subtotal_USD") )

filteredDF.show(4)
print(filteredDF.columns)

filteredPandasDF = filteredDF.toPandas()
print(filteredPandasDF)

# filteredDF.select("order_id")
# .withColumnRenamed("order_id","index")
# .write.format('com.google.cloud.spark.bigquery') \
#   .option('table', 'test.test_table') \
#   .save()

from google.cloud import bigquery
import pytz
from google.oauth2 import service_account

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="resources\sujit-5893-24cf6c163f3f.json"


project_id="sujit-5893"
dataset_id = "ecommerce"
table="orders_table"
table_id="{}.{}.{}".format(project_id, dataset_id, table)

print("********* NAME OF TABLE IS", table_id)

def load_table_dataframe(project_id,table_id):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition= "WRITE_APPEND")
        # write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(
        filteredPandasDF, table_id, job_config=job_config
    )  
    job.result()  

    data = client.get_table(table_id)  
    return data

data = load_table_dataframe(project_id, table_id)

def load_table_dataframe_config(project_id,table_id, data):
    # Construct a BigQuery client object.
    client = bigquery.Client( project=project_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(
        data, table_id, job_config=job_config
    )  
    job.result()  

    data = client.get_table(table_id)  
    return data

end = time.perf_counter()
print(f" {end - start:0.4f} seconds")