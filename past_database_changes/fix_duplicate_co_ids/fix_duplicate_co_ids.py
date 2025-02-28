import os
from ast import literal_eval
from time import time

import pyarrow as pa
from colabfit.tools.schema import config_schema
from colabfit.tools.utilities import spark_schema_to_arrow_schema
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from vastdb.session import Session


load_dotenv()
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("VAST_DB_ENDPOINT")
n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1
spark_ui_port = os.getenv("__SPARK_UI_PORT")
print(spark_ui_port)
# jars = os.getenv("VASTDB_CONNECTOR_JARS")
# print(jars)
SLURM_ARRAY_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
SLURM_JOB_ID = int(os.getenv("SLURM_JOB_ID"))
spark = (
    SparkSession.builder.appName(f"colabfit_{SLURM_JOB_ID}_{SLURM_ARRAY_TASK_ID}")
    .master(f"local[{n_cpus}]")
    .config("spark.ui.port", f"{spark_ui_port}")
    .getOrCreate()
)
session = Session(access=access, secret=secret, endpoint=endpoint)


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
prefix = prefixes[SLURM_ARRAY_TASK_ID]
id_prefix = f"CO_{prefix:04d}"
print(f" --> id_prefix={id_prefix}")
start = time()

with session.transaction() as tx:
    table = tx.bucket(bucket_name).schema(schema_name).table(table_name)
    reader = table.select(
        predicate=table["id"].startswith(id_prefix),
        internal_row_id=False,
    )
    df_batch = reader.read_all().to_struct_array().to_pandas()
    df_batch = spark.createDataFrame(df_batch, schema=config_schema)
    duplicate_ids = df_batch.groupBy("id").count().filter("count > 1").select("id")
    print("grouped")
    if duplicate_ids.count() == 0:
        print("No duplicates found")
        exit()
    dup_rows = df_batch.join(F.broadcast(duplicate_ids), on="id", how="inner")
    print("joined")
    if not spark.catalog.tableExists(
        f"ndb.{bucket_name}.{schema_name}.{new_table_name}"
    ):
        print(f"Creating table {new_table_name}")
        with session.transaction() as tx:
            schema = tx.bucket(bucket_name).schema(schema_name)
            schema.create_table(new_table_name, arrow_schema)
    update_table = pa.table(
        [
            pa.array(col)
            for col in zip(
                *dup_rows.select([field.name for field in arrow_schema]).collect()
            )
        ],
        schema=arrow_schema,
    )
    print("trying table insert")
    with session.transaction() as tx:
        table = tx.bucket(bucket_name).schema(schema_name).table(new_table_name)
        table.insert(update_table)
    print("done writing batch to table")
    print(f"Time taken: {time() - start}")

print("Finished!")
