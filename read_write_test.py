from pyspark.sql import SparkSession
import findspark

findspark.init('C:\SPARK_HOME\spark-3.3.1-bin-hadoop3')
spark = SparkSession.builder.master("local[*]")\
.appName("read")\
.config("spark.driver.extraClassPath","resources\postgresql-42.5.1.jar")\
.getOrCreate()

order_items_df = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/retail_db")\
        .option("dbtable", "order_items") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()

# order_items_df.show(11)

orders_df = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/retail_db")\
        .option("dbtable", "orders") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()
# orders_df.show(11)


df2 = orders_df.join(order_items_df, orders_df.order_id == order_items_df.order_item_order_id, how="full")

# df2.show(11)

df2.createOrReplaceTempView("orders")

spark.sql("select * from orders limit 22").show(truncate=False)


df2\
    .coalesce(1)\
    .write\
    .option("header",True)\
    .mode('overwrite')\
    .csv("C://Users//sujit//IdeaProjects//spark_scala_certification_pratice//src//main//resources")
