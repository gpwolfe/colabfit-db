from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from ast import literal_eval
import pyspark.sql.functions as sf

spark = SparkSession.builder.appName("find_orphans").getOrCreate()

cos = spark.table("ndb.colabfit.dev.co_oc_reset2")
ds_ids = [
    x["id"] for x in spark.table("ndb.colabfit.dev.ds_oc_reset").select("id").collect()
]

# co_ds_ids = cos.select("dataset_ids").distinct()
# unstring_udf = sf.udf(lambda x: literal_eval(x), ArrayType(StringType()))
# co_ds_ids = co_ds_ids.withColumn("dataset_ids", unstring_udf("dataset_ids"))
# co_ds_ids = co_ds_ids.select(sf.explode("dataset_ids").alias("dataset_ids"))
# co_ds_ids = co_ds_ids.filter(~co_ds_ids.dataset_ids.isin(ds_ids))

# with open("orphan_co_ds_ids.txt", "a") as f:
#     for id in co_ds_ids.collect():
#         f.write(f"{id['dataset_ids']}\n")
# print(f"count of distinct co dsids {co_ds_ids.count()}")


# ids = [
#     "DS_gn2r8wkmj8na_0",
#     "DS_tnelggdzqaw2_0",
#     "DS_8yagh9ajb4k8_0",
#     "DS_zlxgrekdla8l_0",
#     "DS_nn303f7h8syk_0",
#     "DS_zlxgrekdla8l_0",
#     "DS_y1yt8cetzhk3_0",
#     "DS_8yagh9ajb4k8_0",
#     "DS_38gx79a86v6r_0",
#     "DS_foyahm0mqd0z_0",
#     "DS_kq5u4xa6sq5h_0",
#     "DS_bk5yirwyozjy_0",
#     "DS_arkwiyee6f3e_0",
#     "DS_zrdshoa4zpvy_0",
#     "DS_40cwg2s2cy2z_0",
#     "DS_f312cdv4p0eh_0",
#     "DS_gn2r8wkmj8na_0",
#     "DS_y1yt8cetzhk3_0",
#     "DS_38gx79a86v6r_0",
#     "DS_alqeb8acwpzh_0",
#     "DS_w5kzth1uof2y_0",
#     "DS_gn2r8wkmj8na_0",
#     "DS_8yagh9ajb4k8_0",
#     "DS_yj6mh5v6cj6r_0",
#     "DS_alqeb8acwpzh_0",
#     "DS_gn2r8wkmj8na_0",
#     "DS_8yagh9ajb4k8_0",
# ]
#     "DS_otx1qc9f3pm4_0", OC20_S2EF_train_20M

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

# with open("orphan_pos_ids.txt", "w") as f:
#     for id in pos.select("dataset_id").collect():
#         f.write(f"{id['dataset_id']}\n")

# print(f"count of pos ids: {pos.count()}")

pos = spark.table("ndb.colabfit.dev.po_oc_reset")

for id in ds_ids:
    count = cos.filter(cos.dataset_ids.contains(id)).count()
    if count == 0:
        print(f"{id} has no cos")
        with open("dataset_with_no_configs_4-11-24.txt", "a") as f:
            f.write(f"{id}\n")
    po_count = pos.filter(pos.dataset_id == id).count()
    if po_count == 0:
        print(f"{id} has no pos")
        with open("dataset_with_no_pos_4-11-24.txt", "a") as f:
            f.write(f"{id}\n")


print("complete")
