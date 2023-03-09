from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import findspark

findspark.init('C:\SPARK_HOME\spark-3.3.1-bin-hadoop3')

spark = SparkSession.builder.appName("test_write").getOrCreate()

data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", StringType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
print(type(df))

df.write.format('bigquery') \
  .option('table', 'test_dataset.test11') \
    .option('temporaryGcsBucket', 'sujit_bucket_11')\
  .mode("append")\
  .save()
  #append