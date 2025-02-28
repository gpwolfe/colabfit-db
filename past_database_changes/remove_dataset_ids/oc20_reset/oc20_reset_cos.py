from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from ast import literal_eval
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("oc20_reset_cos").getOrCreate()
cos = spark.table("ndb.colabfit.dev.co_oc_reset2")
# pos = spark.table("ndb.colabfit.dev.po_wip")
# dss = spark.table("ndb.colabfit.dev.ds_wip")
# cos = spark.table("ndb.colabfit.dev.co_oc_reset")

new_co_table = "ndb.colabfit.dev.co_oc_reset3"
# new_po_table = "ndb.colabfit.dev.po_oc_reset"
# new_ds_table = "ndb.colabfit.dev.ds_oc_reset"


# ids_to_remove = [
#     "DS_zdy2xz6y88nl_0",
#     "DS_dgop3abwq9ya_0",
#     "DS_7qi6dh0ig7sd_0",
#     "DS_otx1qc9f3pm4_0",
#     "DS_wmgdq06mzdys_0",
#     "DS_cgdsc3gxoamu_0",
#     "DS_wv9zv6egp9vk_0",
#     "DS_889euoe7akyy_0",
#     "DS_rf10ovxd13ne_0",
# ]

# The following ids can be removed when we fix OC20 -- all instances are
# shared only between the existing OC20
# datasets, whether S2EF or IS2RES
# +-----------------+----------------------+
# |id               |name                  |
# +-----------------+----------------------+
# |DS_zdy2xz6y88nl_0|OC20_S2EF_train_200K  |
# |DS_dgop3abwq9ya_0|OC20_IS2RES_train     |
# |DS_7qi6dh0ig7sd_0|OC20_S2EF_train_2M    |
# |DS_otx1qc9f3pm4_0|OC20_S2EF_train_20M   |
# |DS_wmgdq06mzdys_0|OC20_S2EF_val_ood_cat |
# |DS_cgdsc3gxoamu_0|OC20_S2EF_val_ood_ads |
# |DS_wv9zv6egp9vk_0|OC20_S2EF_val_id      |
# |DS_889euoe7akyy_0|OC20_S2EF_val_ood_both|
# +-----------------+----------------------+
#  in addition, the id DS_rf10ovxd13ne_0 is one that should be removed
#  as a failed ingest of the 20 M split of S2EF from mongo.
# variations = [
# "['DS_889euoe7akyy_0']",
# "['DS_cgdsc3gxoamu_0']",
# "['DS_dgop3abwq9ya_0']",
# "['DS_otx1qc9f3pm4_0']",
# "['DS_rf10ovxd13ne_0', 'DS_7qi6dh0ig7sd_0', 'DS_dgop3abwq9ya_0']",
# "['DS_rf10ovxd13ne_0', 'DS_7qi6dh0ig7sd_0']",
# "['DS_rf10ovxd13ne_0', 'DS_dgop3abwq9ya_0']",
# "['DS_rf10ovxd13ne_0', 'DS_zdy2xz6y88nl_0', 'DS_7qi6dh0ig7sd_0', 'DS_dgop3abwq9ya_0']",
# "['DS_rf10ovxd13ne_0', 'DS_zdy2xz6y88nl_0', 'DS_7qi6dh0ig7sd_0']",
# "['DS_rf10ovxd13ne_0']",
# "['DS_wmgdq06mzdys_0']",
# "['DS_wv9zv6egp9vk_0']",

# ]
# variations = [
#     "['DS_jyuwhl30jklq_0', 'DS_otx1qc9f3pm4_0']",
#     "['DS_otx1qc9f3pm4_0']",
#     "['DS_jyuwhl30jklq_0']",
# ]
ids_to_remove = [
    "DS_gn2r8wkmj8na_0",
    "DS_tnelggdzqaw2_0",
    "DS_8yagh9ajb4k8_0",
    "DS_zlxgrekdla8l_0",
    "DS_nn303f7h8syk_0",
    "DS_zlxgrekdla8l_0",
    "DS_y1yt8cetzhk3_0",
    "DS_8yagh9ajb4k8_0",
    "DS_38gx79a86v6r_0",
    "DS_foyahm0mqd0z_0",
    "DS_kq5u4xa6sq5h_0",
    "DS_bk5yirwyozjy_0",
    "DS_arkwiyee6f3e_0",
    "DS_zrdshoa4zpvy_0",
    "DS_40cwg2s2cy2z_0",
    "DS_f312cdv4p0eh_0",
    "DS_gn2r8wkmj8na_0",
    "DS_y1yt8cetzhk3_0",
    "DS_38gx79a86v6r_0",
    "DS_alqeb8acwpzh_0",
    "DS_w5kzth1uof2y_0",
    "DS_gn2r8wkmj8na_0",
    "DS_8yagh9ajb4k8_0",
    "DS_yj6mh5v6cj6r_0",
    "DS_alqeb8acwpzh_0",
    "DS_gn2r8wkmj8na_0",
    "DS_8yagh9ajb4k8_0",
]


@sf.udf(StringType())
def remove_id_udf(id_val):
    id_val = literal_eval(id_val)
    id_val = [x for x in id_val if x not in ids_to_remove]
    if len(id_val) == 0:
        return None
    return str(id_val)


# DS_wmgdq06mzdys_0
count1 = cos.count()
print(f"count1: {count1}")
# cos = cos.filter(~cos.dataset_ids.isin(variations))
cos = cos.withColumn("dataset_ids", remove_id_udf("dataset_ids"))
cos = cos.filter(cos.dataset_ids.isNotNull())
count2 = cos.count()
print(f"count2: {count2}")
print(f"difference: {count1 - count2}")
cos.write.mode("errorifexists").saveAsTable(new_co_table)
print("complete")
