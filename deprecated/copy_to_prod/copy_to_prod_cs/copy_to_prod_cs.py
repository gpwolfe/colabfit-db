import os
from time import time

from colabfit.tools.schema import configuration_set_schema, co_cs_mapping_schema
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

cs_table = "ndb.colabfit.dev.cs_wip"

to_cs_table = "ndb.`colabfit-prod`.prod.cs_tmp"


print("loading css")
css = spark.table(cs_table)
print("writing css to table")
css.printSchema()
css.write.mode("errorifexists").saveAsTable(
    to_cs_table, schema=configuration_set_schema
)

sorted_columns = ["id"]
unsorted_columns = [
    col for col in configuration_set_schema.fieldNames() if col not in sorted_columns
]
with sess.transaction() as tx:
    table = tx.bucket("colabfit-prod").schema("prod").table("cs_tmp")
    table.create_projection(
        projection_name="cs-id-all",
        sorted_columns=sorted_columns,
        unsorted_columns=unsorted_columns,
    )

sorted_columns = ["dataset_id"]
unsorted_columns = ["id", "name", "description"]
with sess.transaction() as tx:
    table = tx.bucket("colabfit-prod").schema("prod").table("cs_tmp")
    table.create_projection(
        projection_name="cs-dataset_id",
        sorted_columns=sorted_columns,
        unsorted_columns=unsorted_columns,
    )


cs_map_table = "ndb.colabfit.dev.cs_co_wip"
to_cs_map_table = "ndb.`colabfit-prod`.prod.cs_co_map_tmp"

print("loading cs-co maps")
cs_co_maps = spark.table(cs_map_table)
print("writing cs-co maps to table")
cs_co_maps.printSchema()
cs_co_maps.write.mode("errorifexists").saveAsTable(
    to_cs_map_table, schema=co_cs_mapping_schema
)

# CS-CO map
sorted_columns = ["configuration_set_id", "configuration_id"]
unsorted_columns = []
with sess.transaction() as tx:
    table = tx.bucket("colabfit-prod").schema("prod").table("cs_co_map_tmp")
    table.create_projection(
        projection_name="cs-co",
        sorted_columns=sorted_columns,
        unsorted_columns=unsorted_columns,
    )
    print(table.projections())

print(f"Time elapsed: {time() - begin:.2f} seconds")
spark.stop()
