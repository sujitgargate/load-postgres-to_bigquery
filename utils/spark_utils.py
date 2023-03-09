from pyspark.sql import SparkSession
import findspark

class spark_utils:
    def spark_obj_onCreate(self):
        # findspark.find()
        findspark.init('C:\SPARK_HOME\spark-3.3.1-bin-hadoop3')
        spark = SparkSession.builder.master("local[*]")\
        .appName("OracleToBigQuery")\
        .config("spark.driver.extraClassPath","resources\postgresql-42.5.1.jar")\
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
        .config('spark.jars', 'resources\spark-3.1-bigquery-0.28.0-preview.jar')\
        .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        # .config('spark.jars', 'resources\gcs-connector-hadoop3-latest.jar')\
        # .config('spark.jars', 'resources\spark-bigquery-with-dependencies_2.13-0.27.0.jar')\
        # .config("spark.driver.extraClassPath","resources\postgresql-42.5.1.jar")\
        
        return spark
    
    def spark_obj_onComplete():
        spark.stop()
