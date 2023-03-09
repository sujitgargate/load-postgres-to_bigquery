from google.cloud import bigquery
import os

class write_bigquery:
    
    def load_pandasDF_BQ(self, filteredPandasDF, project_id, table_id):
        # Construct a BigQuery client object.
        timer = 0
        start = time.perf_counter()
        
        #add below feature
        #check for auto schema change feature
        job_config = bigquery.LoadJobConfig(
            write_disposition= "WRITE_APPEND")
            # write_disposition="WRITE_TRUNCATE")

        job = client.load_table_from_dataframe(
            filteredPandasDF, table_id, job_config=job_config
        )  
        job.result()  

        data = client.get_table(table_id)  
        return data

    #bucket name -- test_bucket_11_bucket
    def pyspark_df_BQ_write(self, filteredDF):
        # globalFilteredDF = filteredDF
        os.system("  pyspark --cluster test-cluster --jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.28.0.jar utils\write_BQ.py")

    # def bq_save(self):
    #     globalFilteredDF.write.format('bigquery') \
    #         .option('table', 'test_dataset.test11') \
    #         .option('temporaryGcsBucket', 'sujit_bucket_11')\
    #         .mode("append")\
    #         .save()

    # def __init__(self):
    #     global globalFilteredDF
    #     self.globalFilteredDF = None

write_bq = write_bigquery()

# write_bq.bq_save()