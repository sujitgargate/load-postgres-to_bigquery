import time
from pyspark.sql import SparkSession
import psycopg2
tic = time.perf_counter()

spark = SparkSession.builder.master("local[*]")\
          .appName("SparkByExamples.com")\
          .config("spark.jars", "resources\postgresql-42.5.1.jar")\
          .getOrCreate()

# Amazon_Sale_Report_df =  spark.read.format("csv").option("header","true").load("resources\Amazon_Sale_Report.csv")

International_sale_Report_df = spark.read.csv("resources\International_sale_Report.csv",header=True)

# for col in Expense_IIGF_df.dtypes:
#     print(col[0], "......", col[1])
# print((Expense_IIGF_df.dtypes))

International_sale_Report_df\
    .select("index","DATE","Months","CUSTOMER","Style","SKU","Size","PCS","RATE","GROSS AMT")\
    .write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/ecommerce_source_db") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "International_sale_Report_table") \
    .option("user", "postgres").option("password", "postgres")\
        .mode('append')\
        .save()


toc = time.perf_counter()
print(f"processed in {toc - tic:0.4f} seconds")