from utils.spark_utils import spark_utils
import time, calendar
import os
from pyspark.sql.functions import when, col, udf
from pyspark.sql.types import FloatType, IntegerType, StringType
from utils.read_postgres import read_from_postgres
from utils.udf.custom_udf import Custom_UDF_Class
from utils.write_BQ import write_bigquery
from utils.read_json import read_json_class
from utils.csvFinder import csvFinderClass
from google.cloud import bigquery
import logging

#addd below features
    # Logger
    # read from bq and write into pgsql
    # check for null values and handle nulls
    # write data based on order_date
    # partation data based on order_date
    # add complex data type
    # Write test cases

class Main:
    try:
        spark_util = spark_utils()
        spark = spark_util.spark_obj_onCreate()
        read_postgres = read_from_postgres()
        start = time.perf_counter()
        Custom_UDF_obj = Custom_UDF_Class()
        write_BQ_util_obj = write_bigquery()
        read_json_obj = read_json_class()
        configs = read_json_obj.read_json_function()

        csvFinder = csvFinderClass()

        project_id = configs["PROJECT_CONFIG"]["PROJECT_ID"]
        dataset_id = configs["PROJECT_CONFIG"]["DATASET_ID"]
        table= configs["PROJECT_CONFIG"]["TABLE"]
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = configs["GOOGLE_CLOUD"]["credentials"]
        # os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = configs["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"]

        # os.environ['PYSPARK_SUBMIT_ARGS'] = '''
        # spark --properties spark.jars.packages=com.google.cloud.spark:spark-bigquery_2.11:0.9.1-beta 
        # --packages spark.driver.extraClassPath:resources\postgresql-42.5.1.jar'

        table_id="{}.{}.{}".format(project_id, dataset_id, table)

        order_itemsDF= read_postgres.readFromPostgresDB(spark, "order_items")
        ordersDF= read_postgres.readFromPostgresDB(spark, "orders")
        
        ordersAndOrder_item_joined_DF = ordersDF.join(order_itemsDF, ordersDF.order_id ==  order_itemsDF.order_item_id)

        ordersAndOrder_item_joined_DF.show(5)
        filteredDF = ordersAndOrder_item_joined_DF\
        .withColumn("order_status_updated",
        when(ordersAndOrder_item_joined_DF.order_status == "PROCESSING", "Waiting")\
        .otherwise(ordersAndOrder_item_joined_DF.order_status))

        # filteredDF.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in filteredDF.columns))).show(4)

        # newDF2 = filteredDF.filter(filteredDF.order_status_updated.rlike("Waiting"))

        convertCurrencyUDF =  udf(lambda x : ((x)/80.00), FloatType())
        convertDateToEpochUDF = udf(lambda x : (calendar.timegm(time.strptime(str(x), '%Y-%m-%d %H:%M:%S'))) , StringType())

        filteredDF.show(4)
        print("-----------------------")

        filteredDF = filteredDF.withColumn("order_item_subtotal", convertCurrencyUDF(col("order_item_subtotal")).alias("order_item_subtotal_USD") )

        filteredDF = filteredDF.withColumn("order_date_epoch", convertDateToEpochUDF(col("order_date")))

        # filteredDF = filteredDF.withColumn("order_date_epoch_divided", col("order_date_epoch")/10)
        filteredDF.show(10)

        # os.system("gsutil rm  gs://sujit_bucket_11/part*")
        # bucket_name = 'sujit_bucket_11' 
        # blob_path = ''
        # local_path = 'resources\\csv'
        # upload_to_bucket(bucket_name, blob_path, local_path)

        dailyOrdersDF  = filteredDF.select("order_date").groupBy("order_date").count()
        dailyOrdersDF.show()

        dailyProductRevenueDF = filteredDF.select("order_item_subtotal", "order_date").groupBy("order_date").count()
        print("daily ", dailyOrdersDF)

        dailyProductRevenueDF.show()

        filteredDF.coalesce(1).write.options(header = "True").mode("overwrite").csv("resources\\csv")

        csvFile = csvFinder.findCV()

        # os.system("gsutil cp {} gs://sujit_bucket_11/".format(csvFile))

        # def upload_to_bucket(bucket_name, blob_path, local_path):
        #     bucket = storage.Client().bucket(bucket_name)
        #     blob = bucket.blob(blob_path)
        #     blob.upload_from_filename(local_path)
        #     return blob.url

        table_id = "sujit-use-case.test_dataset.orders"

        def load_gcs_csv_data_bq_table():
            # Construct a BigQuery client object.
            client = bigquery.Client()

            table_id = "sujit-use-case.test_dataset.orders"

            destination_table_prev = client.get_table(table_id)
            print("Previous rows {}.".format(destination_table_prev.num_rows))

            job_config = bigquery.LoadJobConfig(
                    
            #         schema = [\
            #         bigquery.SchemaField("order_id", "STRING"),
            # ],

                autodetect=True,
                skip_leading_rows=1,
                # The source format defaults to CSV, so the line below is optional.
                source_format=bigquery.SourceFormat.CSV,
            )
            uri = "gs://sujit_bucket_11/part*.csv"

            load_job = client.load_table_from_uri(
                uri, table_id, job_config=job_config
            ) 

            load_job.result()  # Waits for the job to complete.

            destination_table_current = client.get_table(table_id) 
            print("Newly Loaded {} rows.".format(destination_table_current.num_rows - destination_table_prev.num_rows))

            print("Total {} rows.".format(destination_table_current.num_rows))

        # load_gcs_csv_data_bq_table()

        from_df_to_table = filteredDF.createOrReplaceTempView("orders_table")

        orders_table_df1 = spark.sql("select * from orders_table")
        #use spark built in fun


        orders_table_daily_orders_revenue_df = spark.sql(
            """select order_date, count(1) as daily_orders_count, 
            round(sum(order_item_product_price),2) as daily_orders_revenue  
            from orders_table group by order_date order by order_date"""
            )
        orders_table_daily_orders_revenue_df.show(13)
        end = time.perf_counter()
        
        # spark.stop()
        print(f"Consumed time for execution is  {end - start:0.4f} seconds")
    except IOError:
        print("IO error occured")