from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StructField,StructType
# import findspark

import logging


class test:
    def printHello(self):
        logging.info('This is an info message')

        # findspark.init('C:\SPARK_HOME\spark-3.3.1-bin-hadoop3')
        spark = SparkSession.builder.appName("Test").getOrCreate()
        data = [(1,),(2,)]
        schema = StructType([
            StructField("id",IntegerType(),True)
        ])

        df = spark.createDataFrame(data)
        print(df.count())
        print("Hello.................................")

if __name__ == "__main__":
    test().printHello()