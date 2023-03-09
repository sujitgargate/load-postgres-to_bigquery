from google.cloud import bigquery
from utils.read_json import read_json_class
import os

read_json_obj = read_json_class()
configs = read_json_obj.read_json_function()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = configs["GOOGLE_CLOUD"]["credentials"]
table_id = "sujit-use-case.test_dataset.orders"

def load_gcs_csv_data_bq_table():
    # Construct a BigQuery client object.
    client = bigquery.Client()

    destination_table_prev = client.get_table(table_id)
    print("Previous rows {}.".format(destination_table_prev.num_rows))

    job_config = bigquery.LoadJobConfig(

            schema = [\
            bigquery.SchemaField("order_id", "STRING"),
            bigquery.SchemaField("order_date", "STRING"),
            bigquery.SchemaField("order_customer_id", "STRING"),
            bigquery.SchemaField("order_status", "STRING"),
            bigquery.SchemaField("order_item_id", "STRING"),
            bigquery.SchemaField("order_item_order_id", "STRING"),
            bigquery.SchemaField("order_item_product_id", "STRING"),
            bigquery.SchemaField("order_item_quantity", "STRING"),
            bigquery.SchemaField("order_item_subtotal", "STRING"),
            bigquery.SchemaField("order_item_product_price", "STRING"),
            bigquery.SchemaField("order_status_updated", "STRING"),
            bigquery.SchemaField("order_date_epoch", "STRING"),

    ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = "gs://sujit_bucket_11/part*.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table_current = client.get_table(table_id)  # Make an API request.
    print("Newly Loaded {} rows.".format(destination_table_current.num_rows - destination_table_prev.num_rows))

    print("Total {} rows.".format(destination_table_current.num_rows))
