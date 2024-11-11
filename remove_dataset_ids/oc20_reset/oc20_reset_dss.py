from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("oc20_reset_dss").getOrCreate()
# cos = spark.table("ndb.colabfit.dev.co_wip")
# pos = spark.table("ndb.colabfit.dev.po_wip")
dss = spark.table("ndb.colabfit.dev.ds_wip")

# new_co_table = "ndb.colabfit.dev.co_oc_reset"
# new_po_table = "ndb.colabfit.dev.po_oc_reset"
new_ds_table = "ndb.colabfit.dev.ds_oc_reset"


ids_to_remove = [
    "DS_zdy2xz6y88nl_0",
    "DS_dgop3abwq9ya_0",
    "DS_7qi6dh0ig7sd_0",
    "DS_otx1qc9f3pm4_0",
    "DS_wmgdq06mzdys_0",
    "DS_cgdsc3gxoamu_0",
    "DS_wv9zv6egp9vk_0",
    "DS_889euoe7akyy_0",
    "DS_rf10ovxd13ne_0",
]

# The following ids can be removed when we fix OC20 -- all instances are shared only
# between the existing OC20
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
#  in addition, the id DS_rf10ovxd13ne_0 is one that should be
#  removed as a failed ingest of the 20 M split of S2EF from mongo.
# variations = [
#     "['DS_889euoe7akyy_0']",
#     "['DS_cgdsc3gxoamu_0']",
#     "['DS_dgop3abwq9ya_0']",
#     "['DS_otx1qc9f3pm4_0']",
#     "['DS_rf10ovxd13ne_0', 'DS_7qi6dh0ig7sd_0', 'DS_dgop3abwq9ya_0']",
#     "['DS_rf10ovxd13ne_0', 'DS_7qi6dh0ig7sd_0']",
#     "['DS_rf10ovxd13ne_0', 'DS_dgop3abwq9ya_0']",
#     "['DS_rf10ovxd13ne_0', 'DS_zdy2xz6y88nl_0', 'DS_7qi6dh0ig7sd_0', 'DS_dgop3abwq9ya_0']",
#     "['DS_rf10ovxd13ne_0', 'DS_zdy2xz6y88nl_0', 'DS_7qi6dh0ig7sd_0']",
#     "['DS_rf10ovxd13ne_0']",
#     "['DS_wmgdq06mzdys_0']",
#     "['DS_wv9zv6egp9vk_0']",
# ]
dss = dss.filter(~dss.id.isin(ids_to_remove))
dss.write.mode("errorifexists").saveAsTable(new_ds_table)
