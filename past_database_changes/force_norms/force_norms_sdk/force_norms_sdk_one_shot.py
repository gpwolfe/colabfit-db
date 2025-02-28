import os
from ast import literal_eval
from copy import deepcopy
from time import time

import numpy as np
import pyarrow as pa
from colabfit.tools.schema import property_object_schema
from colabfit.tools.utilities import spark_schema_to_arrow_schema
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StructField, StructType
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

unstring_udf = F.udf(
    lambda x: [] if x is None else literal_eval(x), ArrayType(ArrayType(DoubleType()))
)


def compute_force_stats(forces):
    if forces is None or len(forces) == 0:
        return (None, None)
    assert forces[0] is not None and len(forces[0]) == 3, "Invalid force format"
    assert all(len(f) == 3 for f in forces), "Invalid force format"
    try:
        norms = [np.linalg.norm(f) for f in forces]
        return (
            float(np.mean(norms)) if norms else None,
            float(np.max(norms)) if norms else None,
        )
    except Exception as e:
        print(f"Error: {e}")
        print(f"Forces: {forces}")
        raise e


compute_force_stats_udf = F.udf(
    compute_force_stats,
    StructType(
        [
            StructField("mean_force_norm", DoubleType(), True),
            StructField("max_force_norm", DoubleType(), True),
        ]
    ),
)
new_property_object_schema = deepcopy(property_object_schema)
new_property_object_schema.add(StructField("mean_force_norm", DoubleType(), True)).add(
    StructField("max_force_norm", DoubleType(), True)
)
arrow_schema = spark_schema_to_arrow_schema(new_property_object_schema)
read_schema = property_object_schema

bucket_name, schema_name, table_name = "colabfit.dev.po_wip".split(".")
new_table_name = "po_wip_force_norms"
print(f"new_table_name={new_table_name}")
prefixes = list(range(1000, 1340))
prefix = prefixes[SLURM_ARRAY_TASK_ID]
id_prefix = f"PO_{prefix:04d}"
print(f" --> id_prefix={id_prefix}")
start = time()

with session.transaction() as tx:
    table = tx.bucket(bucket_name).schema(schema_name).table(table_name)
    reader = table.select(
        predicate=table["id"].startswith(id_prefix),
        internal_row_id=False,
    )
    df_batch = reader.read_all().to_struct_array().to_pandas()

    pos_batch = spark.createDataFrame(df_batch, schema=read_schema)
    print("counting")
    count = pos_batch.count()
    print(f"count: {count}")
    pos_batch = pos_batch.withColumn(
        "atomic_forces_unstr", unstring_udf(F.col("atomic_forces_00"))
    )
    print(" after unstring")

    pos_batch = pos_batch.withColumn(
        "force_stats", compute_force_stats_udf(F.col("atomic_forces_unstr"))
    )
    print(" after force_stats")

    pos_batch = pos_batch.withColumn(
        "mean_force_norm", F.col("force_stats.mean_force_norm")
    ).withColumn("max_force_norm", F.col("force_stats.max_force_norm"))
    print(" after mean and max")

    pos_batch = pos_batch.drop("atomic_forces_unstr", "force_stats")
    print("Writing to table")

    print("arrow schema")
    print(arrow_schema)
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
                *pos_batch.select([field.name for field in arrow_schema]).collect()
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

print("complete")
