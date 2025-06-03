import logging
import os
import sys
from ast import literal_eval
from hashlib import sha512
from time import time

import numpy as np
import pandas as pd
import pyspark.sql.functions as sf
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from vastdb.session import Session

load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
sess = Session(access=access, secret=secret, endpoint=endpoint)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("colabfit")
    .config("spark.jars", jars)
    .config("spark.executor.memoryOverhead", "600")
    # .config("spark.sql.shuffle.partitions", 8)
    .config("spark.driver.maxResultSize", 0)
    # .config("spark.sql.adaptive.enabled", "true")
    .config("spark.driver.memory", "16g")
    .config("spark.executor.memory", "16g")
    .config("spark.memory.fraction", "0.8")
    .config("spark.memory.storageFraction", "0.2")
    .getOrCreate()
)
unique_identifier_kw = ["atomic_numbers", "cell", "metadata_id", "pbc", "positions"]
struct_identifier_kw = [
    "atomic_numbers",
    "positions",
    "cell",
    "pbc",
]


def format_for_hash(v):
    if isinstance(v, np.ndarray):
        if np.issubdtype(v.dtype, np.floating):
            return np.round(v.astype(np.float64), decimals=16)
        elif np.issubdtype(v.dtype, (np.integer, bool)):
            return v.astype(np.int64)
        else:
            return v
    elif isinstance(v, (list, tuple)):
        return np.array(v).data.tobytes()
    elif isinstance(v, dict):
        return str(v).encode("utf-8")
    elif isinstance(v, str):
        return v.encode("utf-8")
    elif isinstance(v, (int, float)):
        return np.array(v).data.tobytes()
    else:
        return v


def main():

    prefixes = range(1000, 1000)
    for prefix in prefixes:
        id_prefix = f"CO_{prefix}"
        logger.info(f"Processing {id_prefix}")
        t1 = time()
        select_cols = [
            "id",
            "atomic_numbers",
            "pbc",
            "cell",
            "positions_00",
            "metadata_id",
        ]

        with sess.transaction() as tx:
            table = tx.bucket("colabfit").schema("dev").table("co_new_pbc")
            reader = table.select(
                predicate=table["id"].startswith(id_prefix), columns=select_cols
            )
            t2 = time()
            logger.info(f"selection time: {t2-t1}")
            cos = reader.read_all()
            logger.info(f"read all time: {time()-t2}")
        logger.info(f"num rows: {cos.num_rows}")

        t3 = time()

        co_pandas = cos.to_struct_array().to_pandas()
        co_pandas = pd.json_normalize(co_pandas)

        cells = (
            co_pandas["cell"].str.replace("[", "").str.replace("]", "").str.split(",")
        )
        cells = [np.array(x, dtype="float").reshape(3, 3).tolist() for x in cells]

        atomic_numbers = (
            co_pandas["atomic_numbers"]
            .str.replace("[", "")
            .str.replace("]", "")
            .str.split(",")
        )
        atomic_numbers = [np.array(x, dtype="int").tolist() for x in atomic_numbers]

        atomic_numbers_lens = [len(x) for x in atomic_numbers]

        positions = (
            co_pandas["positions_00"]
            .str.replace("[", "")
            .str.replace("]", "")
            .str.split(",")
        )
        positions = [
            np.array(x, dtype="float").reshape(atomic_numbers_lens[i], 3).tolist()
            for i, x in enumerate(positions)
        ]

        pbc = [literal_eval(y) for y in co_pandas["pbc"]]
        metadata_id = co_pandas["metadata_id"]
        ids = co_pandas["id"]
        logger.info(f"Time to unstring rows: {time() - t3}")

        t4 = time()
        data_schema = StructType(
            [
                StructField("id", StringType()),
                StructField("pbc", ArrayType(BooleanType())),
                StructField("positions", ArrayType(ArrayType(DoubleType()))),
                StructField("atomic_numbers", ArrayType(IntegerType())),
                StructField("cell", ArrayType(ArrayType(DoubleType()))),
                StructField("metadata_id", StringType()),
            ]
        )
        df = spark.createDataFrame(
            list(zip(ids, pbc, positions, atomic_numbers, cells, metadata_id)),
            schema=data_schema,
        )

        # as used by configuration from utilities module
        @sf.udf("string")
        def hash_udf(row):
            row_dict = row.asDict()
            new_hash = sha512()
            for k in ["atomic_numbers", "cell", "metadata_id", "pbc", "positions"]:
                if row_dict[k] is None or row_dict[k] == "[]":
                    continue
                new_hash.update(format_for_hash(row_dict[k]))
            return str(int(new_hash.hexdigest(), 16))

        @sf.udf("string")
        def struct_hash_udf(row):
            row = row.asDict()
            _hash = sha512()
            for k in ["atomic_numbers", "cell", "pbc", "positions"]:
                v = row[k]
                if v is None or v == "[]":
                    continue
                _hash.update(format_for_hash(v))
            return str(int(_hash.hexdigest(), 16))

        hash_columns = [sf.col(col) for col in unique_identifier_kw]
        df1 = df.withColumn("new_hash", hash_udf(sf.struct(hash_columns)))
        t5 = time()
        logger.info(f"Hashing time: {t5-t4}")
        df2 = df1.withColumn(
            "new_id", sf.concat(sf.lit("CO_"), sf.substring(sf.col("new_hash"), 1, 25))
        )
        t6 = time()
        logger.info(f"New ID time: {t6-t5}")
        struct_columns = [sf.col(col) for col in struct_identifier_kw]
        df3 = df2.withColumn(
            "new_struct_hash", struct_hash_udf(sf.struct(struct_columns))
        )
        t7 = time()
        logger.info(f"Struct hashing time: {t7-t6}")
        df3.select("id", "new_id", "new_hash", "new_struct_hash").write.mode(
            "append"
        ).saveAsTable("ndb.colabfit.dev.co_new_hashes")
        t8 = time()
        logger.info(f"Write time: {t8-t7}")
        logger.info(f"Total time for {id_prefix}: {t8-t1}")
    logger.info("Done!")


if __name__ == "__main__":
    main()
