import os
from time import time

import pyspark.sql.functions as sf
from colabfit.tools.vast.schema import config_schema
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
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
cos = cos.withColumn("last_modified", sf.to_timestamp(sf.col("last_modified")))
print("writing cos to table")
cos.printSchema()
config_schema = StructType(
    [field for field in config_schema if field.name in cos.columns]
)
cos.write.mode("errorifexists").saveAsTable(to_co_table, schema=config_schema)

# Set up projections

sorted_columns = ["id"]
unsorted_columns = [col for col in cos.columns if col not in sorted_columns]
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
