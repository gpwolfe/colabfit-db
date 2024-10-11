import os
from time import time

from colabfit.tools.schema import dataset_schema
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from vastdb.session import Session

load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
sess = Session(access=access, secret=secret, endpoint=endpoint)

spark = SparkSession.builder.appName("copy_to_dev").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

begin = time()

ds_table = "ndb.colabfit.dev.ds_wip"
to_ds_table = "ndb.colabfit.dev.ds_wip2"


print("loading dss")
dss = spark.table(ds_table)
print("writing dss to table")
dss.printSchema()
dss.write.mode("errorifexists").saveAsTable(to_ds_table, schema=dataset_schema)


# Make projections on DSs
sorted_columns = ["id"]
unsorted_columns = [
    col for col in dataset_schema.fieldNames() if col not in sorted_columns
]
_, bucket, schema, table_name = to_ds_table.split(".")
with sess.transaction() as tx:
    table = tx.bucket(bucket).schema(schema).table(table_name)
    table.create_projection(
        projection_name="ds-id-all",
        sorted_columns=sorted_columns,
        unsorted_columns=unsorted_columns,
    )
    print(table.projections())

print(f"Time elapsed: {time() - begin:.2f} seconds")
spark.stop()
