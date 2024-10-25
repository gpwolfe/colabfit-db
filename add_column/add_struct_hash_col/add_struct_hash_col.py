import os
from ast import literal_eval
from hashlib import sha512
from time import time

import numpy as np
import pyspark.sql.functions as sf
from colabfit.tools.utilities import _format_for_hash, get_stringified_schema
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
    ArrayType,
    DoubleType,
    IntegerType,
    BooleanType,
)
from vastdb.session import Session

load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
sess = Session(access=access, secret=secret, endpoint=endpoint)

spark = SparkSession.builder.appName("copy_to_dev").getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")

begin = time()

co_table = "ndb.colabfit.dev.co_wip"
to_co_table = "ndb.colabfit.dev.co_with_struct_hash_long_str"


print("loading cos")
cos = spark.table(co_table)

config_df_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("hash", StringType(), True),
        StructField("last_modified", TimestampType(), True),
        StructField("dataset_ids", ArrayType(StringType()), True),
        StructField("configuration_set_ids", ArrayType(StringType()), True),
        StructField("chemical_formula_hill", StringType(), True),
        StructField("chemical_formula_reduced", StringType(), True),
        StructField("chemical_formula_anonymous", StringType(), True),
        StructField("elements", ArrayType(StringType()), True),
        StructField("elements_ratios", ArrayType(DoubleType()), True),
        StructField("atomic_numbers", ArrayType(IntegerType()), True),
        StructField("nsites", IntegerType(), True),
        StructField("nelements", IntegerType(), True),
        StructField("nperiodic_dimensions", IntegerType(), True),
        StructField("cell", ArrayType(ArrayType(DoubleType())), True),
        StructField("dimension_types", ArrayType(IntegerType()), True),
        StructField("pbc", ArrayType(BooleanType()), True),
        StructField("names", ArrayType(StringType()), True),
        StructField("labels", ArrayType(StringType()), True),
        StructField("metadata_id", StringType(), True),
        StructField("metadata_path", StringType(), True),
        StructField("metadata_size", IntegerType(), True),
        StructField("structure_hash", StringType(), True),
    ]
    + [
        StructField(f"positions_{i:02d}", ArrayType(ArrayType(DoubleType())), True)
        for i in range(20)
    ]
)
config_schema = get_stringified_schema(config_df_schema)


@sf.udf(returnType=StringType())
def config_struct_hash_udf(atomic_numbers, cell, pbc, *positions_cols):
    """
    Will hash in the following order: atomic_numbers, cell, pbc, positions

    Position columns will be concatenated and sorted by x, y, z
    Atomic numbers will be sorted by the position sorting
    Perform on existing rows in written tables (will unstring)
    """
    _hash = sha512()
    positions = []
    for v in positions_cols:
        if v is None or v == "[]":
            continue
        else:
            positions.extend(literal_eval(v))
    positions = np.array(positions)
    sort_ixs = np.lexsort(
        (
            positions[:, 2],
            positions[:, 1],
            positions[:, 0],
        )
    )
    sorted_positions = positions[sort_ixs]
    atomic_numbers = np.array(literal_eval(atomic_numbers))
    sorted_atomic_numbers = atomic_numbers[sort_ixs]
    _hash.update(bytes(_format_for_hash(sorted_atomic_numbers)))
    _hash.update(bytes(_format_for_hash(literal_eval(cell))))
    _hash.update(bytes(_format_for_hash(literal_eval(pbc))))
    _hash.update(bytes(_format_for_hash(sorted_positions)))
    return str(int(_hash.hexdigest(), 16))


cos = cos.withColumn(
    "structure_hash",
    config_struct_hash_udf(
        sf.col("atomic_numbers"),
        sf.col("cell"),
        sf.col("pbc"),
        *[sf.col(f"positions_{i:02d}") for i in range(0, 19)],
    ),
)


print("writing cos to table")
cos.printSchema()
cos.write.mode("errorifexists").saveAsTable(to_co_table, schema=config_schema)

_, bucket, schema, table_name = to_co_table.split(".")
# Make projections on cos
sorted_columns = ["dataset_ids"]
unsorted_columns = ["id"]
with sess.transaction() as tx:
    table = tx.bucket(bucket).schema(schema).table(table_name)
    table.create_projection(
        projection_name="co-dataset_ids",
        sorted_columns=sorted_columns,
        unsorted_columns=unsorted_columns,
    )

sorted_columns = ["id"]
unsorted_columns = [
    col for col in config_schema.fieldNames() if col not in sorted_columns
]
with sess.transaction() as tx:
    table = tx.bucket(bucket).schema(schema).table(table_name)
    table.create_projection(
        projection_name="co-id-all",
        sorted_columns=sorted_columns,
        unsorted_columns=unsorted_columns,
    )
    print(table.projections())

print(f"Time elapsed: {time() - begin:.2f} seconds")
spark.stop()
