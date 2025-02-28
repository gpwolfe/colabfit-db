import os
from ast import literal_eval
from time import time
import pyarrow as pa
import numpy as np
import pyspark.sql.functions as sf
from colabfit.tools.database import batched
from colabfit.tools.utilities import spark_schema_to_arrow_schema
from colabfit.tools.schema import *
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
    DoubleType,
    ArrayType,
)
from colabfit.tools.schema import config_schema
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from vastdb.session import Session

start = time()


n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1

spark_ui_port = os.getenv("__SPARK_UI_PORT")
jars = os.getenv("VASTDB_CONNECTOR_JARS")

spark = (
    SparkSession.builder.appName("ForceNorms")
    .master(f"local[{n_cpus}]")
    .config("spark.executor.memoryOverhead", "600")
    .config("spark.ui.port", f"{spark_ui_port}")
    .config("spark.jars", jars)
    .config("spark.driver.maxResultSize", 0)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.rpc.message.maxSize", "2047")
    .getOrCreate()
)
load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
sess = Session(access=access, secret=secret, endpoint=endpoint)
SLURM_ARRAY_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))

read_schema = StructType(
    [
        StructField("$row_id", IntegerType(), True),
        StructField("id", StringType(), True),
        StructField("atomic_forces_00", StringType(), True),
        StructField("mean_force_norm", DoubleType(), True),
        StructField("max_force_norm", DoubleType(), True),
    ]
)
unstring_udf = sf.udf(
    lambda x: [] if x is None else literal_eval(x), ArrayType(ArrayType(DoubleType()))
)


def compute_force_stats(forces):
    if forces is None or len(forces) == 0:
        return (None, None)
    assert forces[0] is not None and len(forces[0]) == 3, "Invalid force format"
    assert all(len(f) == 3 for f in forces), "Invalid force format"
    try:
        # Compute norms for each force
        norms = [np.linalg.norm(f) for f in forces]
        # Return mean and max norms
        return (
            float(np.mean(norms)) if norms else None,
            float(np.max(norms)) if norms else None,
        )
    except Exception as e:
        print(f"Error: {e}")
        print(f"Forces: {forces}")
        raise e


compute_force_stats_udf = sf.udf(
    compute_force_stats,
    StructType(
        [
            StructField("mean_force_norm", DoubleType(), True),
            StructField("max_force_norm", DoubleType(), True),
        ]
    ),
)

pos = spark.table("ndb.colabfit.dev.po_wip")
# rows_without_norms = pos.filter(
#     'dataset_id = "DS_weq0x6qxqbau_0" and mean_force_norm is null'
# )
# print(f"Rows without norms: {rows_without_norms.count()}")
# ids = [x["id"] for x in rows_without_norms.select("id").collect()]
with open("missing_force_norms.txt", "r") as f:
    ids = [x.strip() for x in f.readlines()]
batched_ids = [list(batch) for batch in batched(ids, 1000)]
batch = batched_ids[SLURM_ARRAY_TASK_ID]
print(f"Batch {SLURM_ARRAY_TASK_ID} has {len(batch)} records")


with sess.transaction() as tx:
    table = tx.bucket("colabfit").schema("dev").table("po_wip")
    reader = table.select(
        predicate=table["id"].isin(batch),
        columns=["id", "atomic_forces_00", "mean_force_norm", "max_force_norm"],
        internal_row_id=True,
    )
    rec_batch = reader.read_all()
if len(rec_batch) == 0:
    print("Batch is empty")
    exit(0)
print(f"Batch has {len(rec_batch)} records")
pos_batch = spark.createDataFrame(
    rec_batch.to_struct_array().to_pandas(), schema=read_schema
)
pos_batch = pos_batch.withColumn(
    "forces", unstring_udf(sf.col("atomic_forces_00"))
).withColumn("force_stats", compute_force_stats_udf(sf.col("forces")))
pos_batch = pos_batch.withColumn(
    "mean_force_norm", sf.col("force_stats.mean_force_norm")
).withColumn("max_force_norm", sf.col("force_stats.max_force_norm"))
pos_batch = pos_batch.drop("forces", "force_stats")
update_table = pa.Table.from_pandas(
    pos_batch.toPandas(), schema=spark_schema_to_arrow_schema(read_schema)
)
with sess.transaction() as tx:
    table = tx.bucket("colabfit").schema("dev").table("po_wip")
    table.update(rows=update_table, columns=["mean_force_norm", "max_force_norm"])

print("Finished!")
print(f"Elapsed time: {time() - start}")
