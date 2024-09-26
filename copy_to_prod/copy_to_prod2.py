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


config_table = "ndb.`colabfit-prod`.prod.co"
# config_set_table = "ndb.colabfit.dev.cs_test"
dataset_table = "ndb.`colabfit-prod`.prod.ds"
prop_table = "ndb.`colabfit-prod`.prod.po"

to_co_table = "ndb.colabfit.dev.co_remove_dataset_ids"
# to_cs_table = "ndb.colabfit.dev.cs_oc_test"
to_po_table = "ndb.colabfit.dev.po_remove_dataset_ids"
to_ds_table = "ndb.colabfit.dev.ds_remove_dataset_ids"


print("loading cos")
cos = spark.table(config_table)

dataset_ids_to_remove_from_cos = [
    "['DS_e471qdt7c6db_0', 'DS_e471qdt7c6db_0']",  # Jarvis qetb
    "['DS_e471qdt7c6db_0', 'DS_e471qdt7c6db_0', 'DS_e471qdt7c6db_0']",
    "['DS_e471qdt7c6db_0']",
    "['DS_e471qdt7c6db_0', 'DS_e471qdt7c6db_0', 'DS_e471qdt7c6db_0', 'DS_e471qdt7c6db_0']",
    "['DS_5h3810yhu4wj_0']",  # Fe_nanoparticles_PRB_2023
    "['DS_jz1q9juw7ycj_0']",  # JARVIS_QM9_STD_JCTC
    "['DS_mvuwxu67yrdy_0']",  # JARVIS_HOPV
    "['DS_1nbddfnjxbjc_0']",  # JARVIS_SNUMAT
    "['DS_0j2smy6relq0_0']",  # NENCI-2021
    "['DS_14m394gnh3ae_0']",  # Silica_NPJCM_2022
    "['DS_tat5i46x3hkr_0']",  # JARVIS_QM9-DGL
    "['DS_dbgckv1il6v7_0']",  # JARVIS_Polymer-Genome
]
cos = cos.filter(~sf.col("dataset_ids").isin(dataset_ids_to_remove_from_cos))
print("writing cos to table")
cos.printSchema()
cos.write.mode("append").saveAsTable(to_co_table, schema=config_schema)

# print("loading css")
# css = spark.table(config_set_table)
# css.write.mode("append").saveAsTable(to_cs_table)
# # to_cs_table = "ndb.colabfit.dev.cs_test"

print("loading pos")
pos = spark.table(prop_table)
print("writing pos to table")
pos.printSchema()
pos.write.mode("overwrite").saveAsTable(to_po_table, schema=property_object_schema)

print("loading dss")
dss = spark.table(dataset_table)
dss.write.mode("overwrite").saveAsTable(to_ds_table, schema=dataset_schema)
print(dss.printSchema())

print(f"Time elapsed: {time() - begin:.2f} seconds")
spark.stop()
