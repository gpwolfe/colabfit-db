from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from ast import literal_eval
import pyspark.sql.functions as sf
from datetime import datetime

spark = SparkSession.builder.appName("find_orphans").getOrCreate()

cos = spark.table("ndb.colabfit.dev.co_wip")
ds_ids = [
    x["id"] for x in spark.table("ndb.colabfit.dev.ds_wip").select("id").collect()
]
today = datetime.now().strftime("%Y-%m-%d")
# co_ds_ids = cos.select("dataset_ids").distinct()
# unstring_udf = sf.udf(lambda x: literal_eval(x), ArrayType(StringType()))
# co_ds_ids = co_ds_ids.withColumn("dataset_ids", unstring_udf("dataset_ids"))
# co_ds_ids = co_ds_ids.select(sf.explode("dataset_ids").alias("dataset_ids"))
# co_ds_ids = co_ds_ids.filter(~co_ds_ids.dataset_ids.isin(ds_ids))

# with open(f"orphan_co_ds_ids_{today}.txt", "a") as f:
#     for id in co_ds_ids.collect():
#         f.write(f"{id['dataset_ids']}\n")
# print(f"count of distinct co dsids {co_ds_ids.count()}")

# ds_id_set = set()
# for id in ids:
#     co_ds_ids = (
#         co_ds_ids.filter(co_ds_ids.dataset_ids.contains(id))
#         .select("dataset_ids")
#         .distinct()
#     )
#     ds_id_set.update([x["dataset_ids"] for x in co_ds_ids.collect()])
# with open("orphan_co_ds_ids_full_values.txt", "a") as f:
#     for id in ds_id_set:
#         f.write(f"{id}\n")

# pos = spark.table("ndb.colabfit.dev.po_oc_reset2").select("dataset_id").distinct()
# pos = pos.filter(~pos.dataset_id.isin(ds_ids))

# with open(f"orphan_pos_ids_{today}.txt", "w") as f:
#     for id in pos.select("dataset_id").collect():
#         f.write(f"{id['dataset_id']}\n")

# print(f"count of pos ids: {pos.count()}")

pos = spark.table("ndb.colabfit.dev.po_wip")

for id in ds_ids:
    count = cos.filter(cos.dataset_ids.contains(id)).count()
    if count == 0:
        print(f"{id} has no cos")
        with open(f"dataset_with_no_configs_{today}.txt", "a") as f:
            f.write(f"{id}\n")
    po_count = pos.filter(pos.dataset_id == id).count()
    if po_count == 0:
        print(f"{id} has no pos")
        with open(f"dataset_with_no_pos_{today}.txt", "a") as f:
            f.write(f"{id}\n")


print("complete")
