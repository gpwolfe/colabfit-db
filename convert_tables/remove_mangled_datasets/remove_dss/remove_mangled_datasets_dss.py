from time import time

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession

from colabfit.tools.schema import dataset_schema

spark = SparkSession.builder.appName("copy_to_dev").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

begin = time()

dataset_ids_to_remove = ["DS_kg0dv12aiq97_0", "DS_14h4rvviya0k_0"]

# "DS_gpsibs9f47k4_0" JARVIS Materials Project 2020

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

ds_table = "ndb.colabfit.dev.ds_wip"
to_ds_table = "ndb.colabfit.dev.ds_wip2"
ds = spark.table(ds_table)


ds = ds.filter(~sf.col("id").isin(dataset_ids_to_remove))
print("writing dss to table")
ds.printSchema()
ds.write.mode("errorifexists").saveAsTable(to_ds_table, schema=dataset_schema)

end = time()
