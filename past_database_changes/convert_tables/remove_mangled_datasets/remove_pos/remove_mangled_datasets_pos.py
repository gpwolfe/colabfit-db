from time import time

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession

from colabfit.tools.schema import property_object_schema

spark = SparkSession.builder.appName("copy_to_dev").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

begin = time()

dataset_ids_to_remove = ["DS_14h4rvviya0k_0", "DS_kg0dv12aiq97_0", "DS_ilswipl2g5jn_0"]
# "DS_gpsibs9f47k4_0"

# "['DS_e471qdt7c6db_0']",  # Jarvis qetb
# "['DS_5h3810yhu4wj_0']",  # Fe_nanoparticles_PRB_2023
# "['DS_jz1q9juw7ycj_0']",  # JARVIS_QM9_STD_JCTC
# "['DS_mvuwxu67yrdy_0']",  # JARVIS_HOPV
# "['DS_1nbddfnjxbjc_0']",  # JARVIS_SNUMAT
# "['DS_0j2smy6relq0_0']",  # NENCI-2021
# "['DS_14m394gnh3ae_0']",  # Silica_NPJCM_2022
# "['DS_tat5i46x3hkr_0']",  # JARVIS_QM9-DGL
# "['DS_dbgckv1il6v7_0']",  # JARVIS_Polymer-Genome
# ]

po_table = "ndb.colabfit.dev.po_wip"
to_po_table = "ndb.colabfit.dev.po_wip2"
po = spark.table(po_table)

po = po.filter(~sf.col("dataset_id").isin(dataset_ids_to_remove))
print("writing pos to table")
po.printSchema()
po.write.mode("errorifexists").saveAsTable(to_po_table, schema=property_object_schema)

end = time()
