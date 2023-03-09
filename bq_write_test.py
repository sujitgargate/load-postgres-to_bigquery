from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]")\
.appName("OracleToBigQuery")\
.config("spark.driver.extraClassPath","resources\postgresql-42.5.1.jar")\
.config("spark.sql.execution.arrow.pyspark.enabled", "true")\
.config('spark.jars', 'resources\spark-3.1-bigquery-0.28.0-preview.jar')\
.getOrCreate()


df = spark.read \
    .format("bigquery") \
    .spark.conf.set("viewsEnabled","true")\
    .conf.set("materializationDataset","<dataset>")\
    .load("bigquery-public-data.samples.shakespeare")

sql = """
  SELECT tag, COUNT(*) c
  FROM (
    SELECT SPLIT(tags, '|') tags
    FROM `bigquery-public-data.stackoverflow.posts_questions` a
    WHERE EXTRACT(YEAR FROM creation_date)>=2014
  ), UNNEST(tags) tag
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 10
  """
df = spark.read.format("bigquery").load(sql)
df.show()