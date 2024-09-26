from time import time
from colabfit.tools.schema import dataset_schema, property_object_schema
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("copy_to_dev").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

begin = time()


config_table = "ndb.colabfit.dev.co_test"
config_set_table = "ndb.colabfit.dev.cs_test"
dataset_table = "ndb.colabfit.ingest.ds_new_schema"
prop_table = "ndb.colabfit.ingest.po_new_schema"

# to_co_table = "ndb.colabfit.dev.co_oc_test"
# to_cs_table = "ndb.colabfit.dev.cs_oc_test"
to_po_table = "ndb.colabfit.dev.po_oc_test"
# to_ds_table = "ndb.colabfit.dev.ds_oc_test"


# print("loading cos")
# cos = spark.table(config_table)
# # cos = cos.drop("old_id")
# cos.write.mode("append").saveAsTable(to_co_table)

# print("loading css")
# css = spark.table(config_set_table)
# css.write.mode("append").saveAsTable(to_cs_table)
# # to_cs_table = "ndb.colabfit.dev.cs_test"

print("loading pos")
pos = spark.table(prop_table)
pos = pos.filter(F.col("dataset_id") != "DS_rf10ovxd13ne_0")
print(pos.count())
pos.printSchema()
pos.write.mode("overwrite").saveAsTable(to_po_table, schema=property_object_schema)

# print("loading dss")
# dss = spark.table(dataset_table)
# dss.write.mode("overwrite").saveAsTable(to_ds_table, schema=dataset_schema)
# print(dss.printSchema())

print(f"Time elapsed: {time() - begin:.2f} seconds")
spark.stop()
