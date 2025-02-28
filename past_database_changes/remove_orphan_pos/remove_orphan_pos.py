from pyspark.sql import SparkSession
from time import time

t = time()
spark = SparkSession.builder.appName("remove_orphan_pos").getOrCreate()
pos = spark.table("ndb.colabfit.dev.po_wip")

new_po_table = "ndb.colabfit.dev.po_wip_remove_orphans"


id_to_remove = "DS_8k237jm1rd3s_0"
count1 = pos.count()
print(f"count1: {count1}")
# pos = pos.filter(~pos.dataset_id.isin(ids_to_remove))
pos = pos.filter(pos.dataset_id != id_to_remove)
count2 = pos.count()
print(f"count2: {count2}")
print(f"difference: {count1 - count2}")
pos.write.mode("overwrite").saveAsTable(new_po_table)
print(f"completed in {time() - t} seconds")
