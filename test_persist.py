from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,StructField
import findspark
from pyspark import StorageLevel


findspark.init('C:\SPARK_HOME\spark-3.3.1-bin-hadoop3')
spark = SparkSession.builder.appName("Test").getOrCreate()

data = [("Sujit",),("karan",),("swapnil",)]

schema = StructType([\
        StructField("name", StringType(),True)\
    ])

df = spark.createDataFrame(data=data,schema=schema)
df.persist(StorageLevel.DISK_ONLY)

print(df.count())