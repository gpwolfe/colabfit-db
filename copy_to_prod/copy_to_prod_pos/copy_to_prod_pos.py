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

po_table = "ndb.colabfit.dev.po_wip"
to_po_table = "ndb.`colabfit-prod`.prod.po_tmp"

print("loading pos")
pos = spark.table(po_table)
print("writing pos to table")
pos.printSchema()
pos.write.mode("errorifexists").saveAsTable(to_po_table, schema=property_object_schema)

print(f"Time elapsed: {time() - begin:.2f} seconds")
spark.stop()
