import os
from time import time

from colabfit.tools.schema import config_schema
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

co_table = "ndb.colabfit.dev.co_wip"

to_co_table = "ndb.`colabfit-prod`.prod.co_tmp"


print("loading cos")
cos = spark.table(co_table)
print("writing cos to table")
cos.printSchema()
cos.write.mode("errorifexists").saveAsTable(to_co_table, schema=config_schema)

# sorted_columns = ["dataset_ids"]
# unsorted_columns = ["id"]
# with sess.transaction() as tx:
#     table = tx.bucket("colabfit-prod").schema("prod").table("co_tmp")
#     table.create_projection(
#         projection_name="co-dataset_ids",
#         sorted_columns=sorted_columns,
#         unsorted_columns=unsorted_columns,
#     )

sorted_columns = ["id"]
unsorted_columns = [
    col for col in config_schema.fieldNames() if col not in sorted_columns
]
with sess.transaction() as tx:
    table = tx.bucket("colabfit-prod").schema("prod").table("co_tmp")
    table.create_projection(
        projection_name="co-id-all",
        sorted_columns=sorted_columns,
        unsorted_columns=unsorted_columns,
    )
    print(table.projections())


print(f"Time elapsed: {time() - begin:.2f} seconds")
spark.stop()
