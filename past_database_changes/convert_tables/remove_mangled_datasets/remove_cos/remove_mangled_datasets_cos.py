from time import time

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession

from colabfit.tools.schema import config_schema

spark = SparkSession.builder.appName("copy_to_dev").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

begin = time()

ids = ["['DS_ilswipl2g5jn_0', 'DS_kg0dv12aiq97_0']", "['DS_14h4rvviya0k_0']"]


co_table = "ndb.colabfit.dev.co_wip"
to_co_table = "ndb.colabfit.dev.co_wip2"
cos = spark.table(co_table)
print(f"first_count: {cos.count()}")
cos = cos.filter(~sf.col("dataset_ids").isin(ids))
print(f"final_count: {cos.count()}")

cos.write.mode("errorifexists").saveAsTable(to_co_table, schema=config_schema)

end = time()
print(end)
spark.stop()


# ids = [
#     "DS_la08goe2lz0g_0",
#     # discrepencies_and_error_metrics_NPJ_2023_vacancy_re_testing_set no pos
#     # Entangled with DS_q6e3bvq4y67a_0 above
#     "DS_dhe9aqs9q1wf_0",
#     # discrepencies_and_error_metrics_NPJ_2023_interstitial_re_testing_set no pos
#     # Entangled with DS_nublbp38wse0_0 above
#     "DS_14m394gnh3ae_0",
#     # Silica_NPJCM_2022 no pos
#     # entangled with DS_u9wd92plbetq_0 below
#     "DS_gpsibs9f47k4_0",
# ]
# JARVIS_Materials_Project_2020 no pos
# entangled with DS_5ar73nonq6l1_0 and DS_9yr94hhj1k33_0 below
# coss = spark.table("ndb.colabfit.dev.co_remove_dataset_ids_stage2")
#  coss.count()
# 198852226
# for id in ids:
#     coss.filter(sf.col("dataset_ids").contains(id)).distinct().show()


# dataset_ids_to_remove = [
#     "['DS_e471qdt7c6db_0', 'DS_e471qdt7c6db_0']",  # Jarvis qetb
#     "['DS_e471qdt7c6db_0', 'DS_e471qdt7c6db_0', 'DS_e471qdt7c6db_0']",
#     "['DS_e471qdt7c6db_0']",
#     "['DS_e471qdt7c6db_0', 'DS_e471qdt7c6db_0', 'DS_e471qdt7c6db_0',
#  'DS_e471qdt7c6db_0']",
#     "['DS_5h3810yhu4wj_0']",  # Fe_nanoparticles_PRB_2023
#     "['DS_jz1q9juw7ycj_0']",  # JARVIS_QM9_STD_JCTC
#     "['DS_mvuwxu67yrdy_0']",  # JARVIS_HOPV
#     "['DS_1nbddfnjxbjc_0']",  # JARVIS_SNUMAT
#     "['DS_0j2smy6relq0_0']",  # NENCI-2021
#     "['DS_14m394gnh3ae_0']",  # Silica_NPJCM_2022
#     "['DS_tat5i46x3hkr_0']",  # JARVIS_QM9-DGL
#     "['DS_dbgckv1il6v7_0']",  # JARVIS_Polymer-Genome
# ]
# dataset_id_convert_to = {
#     "['DS_qxd7wv9yabtp_0', 'DS_la08goe2lz0g_0']": "['DS_qxd7wv9yabtp_0']",
#     "['DS_q6e3bvq4y67a_0', 'DS_la08goe2lz0g_0']": "['DS_q6e3bvq4y67a_0']",
#     "['DS_nublbp38wse0_0', 'DS_dhe9aqs9q1wf_0']": "['DS_nublbp38wse0_0']",
#     "['DS_q6e3bvq4y67a_0', 'DS_dhe9aqs9q1wf_0']": "['DS_q6e3bvq4y67a_0']",
#     "['DS_u9wd92plbetq_0', 'DS_14m394gnh3ae_0']": "['DS_u9wd92plbetq_0']",
#     "['DS_5ar73nonq6l1_0', 'DS_gpsibs9f47k4_0']": "['DS_5ar73nonq6l1_0']",
#     "['DS_9yr94hhj1k33_0', 'DS_5ar73nonq6l1_0', 'DS_gpsibs9f47k4_0']":
# "['DS_9yr94hhj1k33_0', 'DS_5ar73nonq6l1_0']",
# }

# for k, v in dataset_id_convert_to.items():
#     cos = cos.withColumn(
#         "dataset_ids",
#         sf.when(sf.col("dataset_ids") == k, v).otherwise(sf.col("dataset_ids")),
#     )
