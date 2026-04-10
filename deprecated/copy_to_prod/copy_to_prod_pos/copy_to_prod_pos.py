import os
from time import time

from colabfit.tools.vast.schema import property_object_schema
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

po_table = "ndb.colabfit.dev.po_wip"
# to_po_table = "ndb.`colabfit-prod`.prod.po_tmp"
to_po_table = "ndb.colabfit.dev.po_arr"
to_po_table_split = to_po_table.split(".")[1:]

print("loading pos")
pos = spark.table(po_table)
print("writing pos to table")
pos.printSchema()
pos.write.mode("errorifexists").saveAsTable(to_po_table, schema=property_object_schema)

# Set up projections

sorted_columns = ["dataset_id"]
unsorted_columns = ["id"]
with sess.transaction() as tx:
    table = (
        tx.bucket(to_po_table_split[0])
        .schema(to_po_table_split[1])
        .table(to_po_table_split[2])
    )
    table.create_projection(
        projection_name="po-dataset_id",
        sorted_columns=sorted_columns,
        unsorted_columns=unsorted_columns,
    )

    sorted_columns = ["configuration_id"]
    unsorted_columns = ["id"]
    table.create_projection(
        projection_name="po-configuration_id",
        sorted_columns=sorted_columns,
        unsorted_columns=unsorted_columns,
    )
    sorted_columns = ["id"]
    unsorted_columns = [
        col for col in property_object_schema.fieldNames() if col not in sorted_columns
    ]
    table.create_projection(
        projection_name="po-id-all",
        sorted_columns=sorted_columns,
        unsorted_columns=unsorted_columns,
    )
    print(table.projections())


print(f"Time elapsed: {time() - begin:.2f} seconds")
spark.stop()
