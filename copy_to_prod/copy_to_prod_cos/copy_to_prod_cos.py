from time import time
from colabfit.tools.schema import (
    dataset_schema,
    property_object_schema,
    config_schema,
)
from pyspark.sql import SparkSession

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

print(f"Time elapsed: {time() - begin:.2f} seconds")
spark.stop()
