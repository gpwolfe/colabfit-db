from time import time

from pyspark.sql import SparkSession

from colabfit.tools.schema import dataset_schema

spark = SparkSession.builder.appName("copy_to_dev").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

begin = time()

ds_table = "ndb.`colabfit-prod`.prod.ds"
to_ds_table = "ndb.colabfit.dev.ds_wip"


print("loading dss")
dss = spark.table(ds_table)
print("writing dss to table")
dss.printSchema()
dss.write.mode("errorifexists").saveAsTable(to_ds_table, schema=dataset_schema)

print(f"Time elapsed: {time() - begin:.2f} seconds")
spark.stop()
