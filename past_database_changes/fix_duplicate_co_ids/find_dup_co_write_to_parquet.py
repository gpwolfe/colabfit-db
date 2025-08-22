import os
from ast import literal_eval
from time import time

import numpy as np
from colabfit.tools.vast.schema import config_schema, config_row_id_schema
from colabfit.tools.vast.utilities import spark_schema_to_arrow_schema
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from vastdb.session import Session

print("imports done", flush=True)
load_dotenv()
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("VAST_DB_ENDPOINT")
n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1
jars = os.getenv("VASTDB_CONNECTOR_JARS")
SLURM_ARRAY_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
SLURM_JOB_ID = int(os.getenv("SLURM_JOB_ID"))
ACTUAL_INDEX = int(os.getenv("ACTUAL_INDEX"))
print("creating spark session", flush=True)
spark = (
    SparkSession.builder.appName(f"colabfit_{SLURM_JOB_ID}_{SLURM_ARRAY_TASK_ID}")
    .master(f"local[{n_cpus}]")
    .config("spark.jars", jars)
    .config("spark.port.maxRetries", 50)
    .config("spark.sql.shuffle.partitions", 2400)
    .config("spark.network.timeout", "500s")
    .config("spark.rpc.askTimeout", "500s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.yarn.maxAppAttempts", 5)
    .config("spark.task.maxFailures", 1)
    .config("spark.driver.maxResultSize", 0)
    .getOrCreate()
)
session = Session(access=access, secret=secret, endpoint=endpoint)
print("sessions created", flush=True)

# edit_cols = [
#     "dataset_ids",
#     "names",
#     "labels",
# ]


def parse_stringified_array(col):
    return F.udf(lambda x: literal_eval(x) if x else [], ArrayType(StringType()))(
        F.col(col)
    )


arrow_schema = spark_schema_to_arrow_schema(config_schema)

bucket_name, schema_name, table_name = "colabfit.dev.co_wip".split(".")
new_table_name = "co_wip_unduplicated"

prefixes = list(range(1000, 10000))
prefix = prefixes[ACTUAL_INDEX]
id_prefix = f"CO_{prefix:04d}"
print(f" --> id_prefix={id_prefix}", flush=True)
start = time()

with session.transaction() as tx:
    table = tx.bucket(bucket_name).schema(schema_name).table(table_name)
    reader = table.select(
        predicate=table["id"].startswith(id_prefix),
        internal_row_id=True,
    )
    df_batch = reader.read_all().to_struct_array().to_pandas()
    for x in df_batch:
        if x["metadata_size"] is not None:
            x["metadata_size"] = int(x["metadata_size"])
        else:
            x["metadata_size"] = None
    df_batch = spark.createDataFrame(df_batch, schema=config_row_id_schema)
    duplicate_ids = df_batch.groupBy("id").count().filter("count > 1").select("id")
    print("grouped", flush=True)
    count = duplicate_ids.count()
    if count == 0:
        print("No duplicates found", flush=True)
        exit()
    print(f"Found {count} duplicates", flush=True)
    dup_rows = df_batch.join(duplicate_ids, on="id", how="inner")
    print("joined", flush=True)
    dup_rows.write.parquet(
        f"co_duplicate_parquets/co_wip_duplicate_rows_{id_prefix}.parquet"
    )
    # if not spark.catalog.tableExists(
    #     f"ndb.{bucket_name}.{schema_name}.{new_table_name}"
    # ):
    #     print(f"Creating table {new_table_name}")
    #     with session.transaction() as tx:
    #         schema = tx.bucket(bucket_name).schema(schema_name)
    #         schema.create_table(new_table_name, arrow_schema)
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
    # with session.transaction() as tx:
    #     table = tx.bucket(bucket_name).schema(schema_name).table(new_table_name)
    #     table.insert(update_table)
    print("done writing batch to file")
    print(f"Time taken: {time() - start}")

print("Finished!")
