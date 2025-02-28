import os
from datetime import datetime
import dateutil.parser
from pathlib import Path
from time import time

import numpy as np
import pyspark.sql.functions as sf
from colabfit.tools.vast.database import VastDataLoader, batched
from colabfit.tools.vast.schema import *
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from tqdm import tqdm
from vastdb.session import Session

with open("/scratch/gw2338/colabfit-tools/.env") as f:
    envvars = dict(x.strip().split("=") for x in f.readlines())
INDEX = int(os.getenv("SLURM_ARRAY_TASK_ID"))
access = envvars.get("SPARK_ID")
secret = envvars.get("SPARK_KEY")
endpoint = envvars.get("SPARK_ENDPOINT")
sess = Session(access=access, secret=secret, endpoint=endpoint)
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("colabfit")
    .config("spark.jars", jars)
    .config("spark.executor.memoryOverhead", "600")
    .config("spark.sql.shuffle.partitions", 2400)
    .config("spark.driver.maxResultSize", 0)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.task.maxFailures", 1)
    .getOrCreate()
)

loader = VastDataLoader(
    table_prefix="ndb.colabfit.dev",
)
loader.set_spark_session(spark)
ids = spark.read.parquet("/scratch/gw2338/cos_with_positions_01")
int_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("hash", StringType(), True),
        StructField("last_modified", TimestampType(), True),
        StructField("dataset_ids", StringType(), True),
        StructField("chemical_formula_hill", StringType(), True),
        StructField("chemical_formula_reduced", StringType(), True),
        StructField("chemical_formula_anonymous", StringType(), True),
        StructField("elements", StringType(), True),
        StructField("elements_ratios", StringType(), True),
        StructField("atomic_numbers", StringType(), True),
        StructField("nsites", IntegerType(), True),
        StructField("nelements", IntegerType(), True),
        StructField("nperiodic_dimensions", IntegerType(), True),
        StructField("cell", StringType(), True),
        StructField("dimension_types", StringType(), True),
        StructField("pbc", StringType(), True),
        StructField("names", StringType(), True),
        StructField("labels", StringType(), True),
        StructField("metadata_id", StringType(), True),
        StructField("metadata_path", StringType(), True),
        StructField("metadata_size", IntegerType(), True),
        StructField("structure_hash", StringType(), True),
        StructField("positions_00", StringType(), True),
        StructField("positions_01", StringType(), True),
        StructField("positions_02", StringType(), True),
        StructField("positions_03", StringType(), True),
        StructField("positions_04", StringType(), True),
        StructField("positions_05", StringType(), True),
        StructField("positions_06", StringType(), True),
        StructField("positions_07", StringType(), True),
        StructField("positions_08", StringType(), True),
        StructField("positions_09", StringType(), True),
        StructField("positions_10", StringType(), True),
        StructField("positions_11", StringType(), True),
        StructField("positions_12", StringType(), True),
        StructField("positions_13", StringType(), True),
        StructField("positions_14", StringType(), True),
        StructField("positions_15", StringType(), True),
        StructField("positions_16", StringType(), True),
        StructField("positions_17", StringType(), True),
        StructField("positions_18", StringType(), True),
        StructField("positions_19", StringType(), True),
        StructField("$row_id", IntegerType(), True),
    ]
)
ids_batched = list(batched([x["id"] for x in ids.collect()], 500))

list_of_rows = []
with sess.transaction() as tx:
    table = tx.bucket("colabfit").schema("dev").table("co_wip")
    batch = ids_batched[INDEX]
    reader = table.select(predicate=table["id"].isin(batch), internal_row_id=True)
    data = reader.read_all().to_struct_array().to_pandas()
    data.replace(np.nan, None, inplace=True)
    for x in data:
        if x["metadata_size"] is not None:
            x["metadata_size"] = int(x["metadata_size"])
        else:
            x["metadata_size"] = None
    df = spark.createDataFrame(data, int_schema)
    # last_modified = dateutil.parser.parse(
    #     datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # )
    # df = df.withColumn("last_modified", sf.lit(last_modified))
    df.write.parquet(f"/scratch/gw2338/to_fix_positions/to_fix_positions_{INDEX:02}")
