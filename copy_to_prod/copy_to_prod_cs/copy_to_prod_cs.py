from time import time
from colabfit.tools.schema import (
    dataset_schema,
    property_object_schema,
    config_schema,
    configuration_set_schema,
)
from pyspark.sql import SparkSession

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

print(f"Time elapsed: {time() - begin:.2f} seconds")
spark.stop()
