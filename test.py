from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import findspark
import os
from glob import glob

class test_csv_write_gcs:
    def test():
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
        
        # df = spark.createDataFrame(data=data2,schema=schema)
        # print(df.count(), "..........")

        # df.coalesce(1).write.options(header = "True").mode("overwrite").csv("resources\\csv")

        # PATH = "resources\\csv\\"
        # EXT = "*.csv"
        # csv_files = [file
        #                 for path, subdir, files in os.walk(PATH)
        #                 for file in glob(os.path.join(path, EXT))]

        # print(csv_files[0].split("\\")[-1])

        # os.system("gsutil cp {} gs://sujit_bucket_11/".format(csv_files[0]))


# test_csv_write_gcs = test_csv_write_gcs()
test_csv_write_gcs.test()