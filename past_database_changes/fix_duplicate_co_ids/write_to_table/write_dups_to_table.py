import os
from ast import literal_eval
from time import time

import numpy as np
import pyarrow as pa

# from colabfit.tools.schema import config_schema
from colabfit.tools.utilities import spark_schema_to_arrow_schema
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from vastdb.session import Session
import pandas as pd
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType

load_dotenv()
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("VAST_DB_ENDPOINT")
n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1
spark_ui_port = os.getenv("__SPARK_UI_PORT")
print(spark_ui_port)
jars = os.getenv("VASTDB_CONNECTOR_JARS")
# print(jars)
SLURM_ARRAY_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
SLURM_JOB_ID = int(os.getenv("SLURM_JOB_ID"))
spark = (
    SparkSession.builder.appName(f"colabfit_{SLURM_JOB_ID}_{SLURM_ARRAY_TASK_ID}")
    .master(f"local[{n_cpus}]")
    .config("spark.jars", jars)
    .config("spark.executor.memoryOverhead", "8g")
    .config("spark.port.maxRetries", 50)
    .config("spark.sql.shuffle.partitions", 2400)
    .config("spark.network.timeout", "500s")
    .config("spark.rpc.askTimeout", "500s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.yarn.maxAppAttempts", 5)
    .config("spark.task.maxFailures", 1)
    .config("spark.driver.maxResultSize", 0)
    .config("spark.ui.port", f"{spark_ui_port}")
    .getOrCreate()
)
session = Session(access=access, secret=secret, endpoint=endpoint)


# edit_cols = [
#     "dataset_ids",
#     "names",
#     "labels",
# ]


# def parse_stringified_array(col):
#     return F.udf(lambda x: literal_eval(x) if x else [], ArrayType(StringType()))(
#         F.col(col)
#     )
# def replace_integer_with_float(schema):
#     new_fields = []
#     for field in schema.fields:
#         if field.name == "metadata_size":
#             new_fields.append(
#                 StructField(field.name, FloatType(), nullable=field.nullable)
#             )
#         else:
#             new_fields.append(field)
#     return StructType(new_fields)


# read_schema = replace_integer_with_float(config_schema)
# read_schema = config_schema
read_schema = StructType([StructField("id", StringType(), True)])

# arrow_schema = spark_schema_to_arrow_schema(config_schema)
arrow_schema = spark_schema_to_arrow_schema(read_schema)


bucket_name, schema_name, table_name = "colabfit.dev.co_wip".split(".")
# new_table_name = "co_wip_duplicate_ids"

prefixes = list(range(100, 1000))
prefix = prefixes[SLURM_ARRAY_TASK_ID]
id_prefix = f"CO_{prefix:03d}"
print(f" --> id_prefix={id_prefix}")
start = time()


# def fillna_in_place(d):
#     if pd.isna(d["metadata_size"]):
#         d["metadata_size"] = 0
#     return d


with session.transaction() as tx:
    table = tx.bucket(bucket_name).schema(schema_name).table(table_name)
    reader = table.select(
        predicate=table["id"].startswith(id_prefix),
        columns=["id"],
        internal_row_id=False,
    )
    df_batch = reader.read_all().to_struct_array().to_pandas()

    df_batch = spark.createDataFrame(df_batch, schema=read_schema)
    # df_batch = df_batch.withColumn(
    #     "metadata_size",
    #     F.when(F.col("metadata_size") == 0, None).otherwise(F.col("metadata_size")),
    # ).cast(IntegerType())
    # df_batch = df_batch.withColumn(
    #     "metadata_size",
    #     F.when(F.isnan(F.col("metadata_size")), None).otherwise(
    #         F.col("metadata_size").cast("integer")
    #     ),
    # )
    # df_batch = df_batch.select(
    #     *[
    #         F.when(F.isnan(F.col(col)), None)
    #         .otherwise(F.col(col))
    #         .cast("integer")
    #         .alias(col)
    #         for col in int_columns
    #     ],
    #     *[col for col in df_batch.columns if col not in int_columns],
    # )

    duplicate_ids = df_batch.select("id").groupBy("id").count().filter("count > 1")
    print("grouped")
    count = duplicate_ids.count()
    if count == 0:
        print("No duplicates found")
        exit()
    print(f"Found {count} duplicates")
    with open(f"duplicate_co_ids_{id_prefix}.csv", "w") as f:
        f.write(duplicate_ids.toPandas().to_csv(index=False))
    # dup_rows = df_batch.join(F.broadcast(duplicate_ids), on="id", how="inner")
    # print("joined")
    # if not spark.catalog.tableExists(
    #     f"ndb.{bucket_name}.{schema_name}.{new_table_name}"
    # ):
    #     print(f"Creating table {new_table_name}")
    #     schema = tx.bucket(bucket_name).schema(schema_name)
    #     schema.create_table(new_table_name, arrow_schema)
    # update_table = pa.table(
    #     [
    #         pa.array(col)
    #         for col in zip(
    #             *dup_rows.select([field.name for field in arrow_schema]).collect()
    #         )
    #     ],
    #     schema=arrow_schema,
    # )
    # print("trying table insert")
    # # with session.transaction() as tx:
    # table = tx.bucket(bucket_name).schema(schema_name).table(new_table_name)
    # table.insert(update_table)
    # print("done writing batch to table")
    print(f"Time taken: {time() - start}")

print("Finished!")
