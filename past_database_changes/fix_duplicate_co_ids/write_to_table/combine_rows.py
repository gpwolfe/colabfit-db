"""labels and configuration set ids are all null for the duplicated rows"""

import os
from ast import literal_eval
from pathlib import Path
from time import time

from colabfit.tools.schema import config_schema
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

load_dotenv()
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("VAST_DB_ENDPOINT")
# n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
# if not n_cpus:
#     n_cpus = 1
spark_ui_port = os.getenv("__SPARK_UI_PORT")
print(spark_ui_port)
# jars = os.getenv("VASTDB_CONNECTOR_JARS")
# print(jars)
# SLURM_ARRAY_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
# SLURM_JOB_ID = int(os.getenv("SLURM_JOB_ID"))
spark = (
    SparkSession.builder.appName(f"colabfit_combine_rows")
    # .master(f"local[{n_cpus}]")
    .config("spark.ui.port", f"{spark_ui_port}").getOrCreate()
)


def parse_stringified_array(col):
    return F.udf(lambda x: literal_eval(x) if x else [], ArrayType(StringType()))(
        F.col(col)
    )


edit_cols = ["dataset_ids", "names"]

read_schema = config_schema
new_table_name = "ndb.colabfit.dev.co_wip_combined_rows"

start = time()

table = "ndb.colabfit.dev.co_wip_duplicate_rows_yet_again"
table = spark.table(table)
dup_rows = table.select(
    *[parse_stringified_array(col).alias(col) for col in edit_cols],
    *[col for col in table.columns if col not in edit_cols],
)
combined_rows = dup_rows.groupBy("id").agg(
    F.flatten(F.collect_set("dataset_ids")).alias("dataset_ids"),
    F.flatten(F.collect_set("names")).alias("names"),
    *[
        F.first(col, ignorenulls=True).alias(col)
        for col in table.columns
        if col not in edit_cols and col != "id"
    ],
)
print("combined")

combined_rows = combined_rows.select(
    *[col for col in table.columns if col not in edit_cols],
    *[
        F.udf(lambda x: str(x) if x else None)(F.col(col)).alias(col)
        for col in edit_cols
    ],
)
print("selected")

combined_rows = combined_rows.select([field.name for field in config_schema])
combined_rows.write.mode("overwrite").saveAsTable(new_table_name)
print("done writing to table")
print(f"Time taken: {time() - start}")

print("Finished!")
