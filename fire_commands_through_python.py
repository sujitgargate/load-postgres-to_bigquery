import os 

os.system("gcloud dataproc jobs submit pyspark --cluster test-cluster --jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.28.0.jar test_write_bq.py")