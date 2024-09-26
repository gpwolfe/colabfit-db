from time import time
from colabfit.tools.schema import (
    dataset_schema,
    property_object_schema,
    config_schema,
)
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

spark = SparkSession.builder.appName("copy_to_dev").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

begin = time()

ds_table = "ndb.colabfit.dev.ds_wip"

to_ds_table = "ndb.`colabfit-prod`.prod.ds_tmp"


print("loading dss")
dss = spark.table(ds_table)
print("writing dss to table")
dss.printSchema()
dss.write.mode("errorifexists").saveAsTable(to_ds_table, schema=dataset_schema)

print(f"Time elapsed: {time() - begin:.2f} seconds")
spark.stop()
