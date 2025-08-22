import os
import logging

from colabfit.tools.vast.schema import config_prop_arr_schema
from pyspark.sql.types import (
    ArrayType,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, substring, col
from vastdb.session import Session

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark_ui_port = os.getenv("__SPARK_UI_PORT")

spark = (
    SparkSession.builder.appName("Test")
    .config("spark.ui.port", f"{spark_ui_port}")
    .config("spark.jars", jars)
    .config("spark.executor.heartbeatInterval", 10000000)
    .config("spark.network.timeout", "30000000ms")
    .config("spark.executor.memoryOverhead", "600")
    .config("spark.port.maxRetries", "50")
    .config("spark.rpc.message.maxSize", "2047")
    .config("spark.sql.shuffle.partitions", "9600")
    .config("spark.task.maxFailures", "1")
    .config("spark.driver.memory", "30g")
    .config("spark.driver.maxResultSize", "0")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

with open("/scratch/gw2338/vast/data-lake-main/spark/scripts/.env") as f:
    envvars = dict(x.strip().split("=") for x in f.readlines())

access_key = envvars.get("SPARK_ID")
access_secret = envvars.get("SPARK_KEY")
endpoint = envvars.get("SPARK_ENDPOINT")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)


def _eval_or_empty(lststr):
    if lststr == "[]" or lststr is None:
        return None
    if "nan" in lststr:
        logger.info("Replacing 'nan' with math.nan")
        lststr = lststr.replace("nan", "math.nan")
        logger.info(f"updated to {lststr}")
    return eval(lststr)


@udf(returnType=ArrayType(StringType()))
def eval_array_str(lststr):
    return _eval_or_empty(lststr)


@udf(returnType=ArrayType(IntegerType()))
def eval_array_int(lststr):
    return _eval_or_empty(lststr)


@udf(returnType=ArrayType(ArrayType(IntegerType())))
def eval_array_array_int(lststr):
    return _eval_or_empty(lststr)


@udf(returnType=ArrayType(ArrayType(DoubleType())))
def eval_array_array_double(lststr):
    return _eval_or_empty(lststr)


@udf(returnType=ArrayType(DoubleType()))
def eval_array_double(lststr):
    return _eval_or_empty(lststr)


@udf(returnType=ArrayType(BooleanType()))
def eval_array_bool(lststr):
    return _eval_or_empty(lststr)


spark.udf.register("evalArrayString", eval_array_str)
spark.udf.register("evalArrayInt", eval_array_int)
spark.udf.register("evalArrayArrayInt", eval_array_array_int)
spark.udf.register("evalArrayArrayDouble", eval_array_array_double)
spark.udf.register("evalArrayDouble", eval_array_double)

table = "ndb.`colabfit-prod`.prod.co_po_merged_innerjoin"
df = spark.table(table)


array_cols = {
    x.name: x.dataType.simpleString()
    for x in config_prop_arr_schema
    if x.dataType.typeName() == "array"
}
print(array_cols)
ids = list(range(10000, 13400))
if TASK_ID > len(ids):
    raise ValueError(f"SLURM_TASK_ID {TASK_ID} exceeds number of prefixes {len(ids)}")
# if TASK_ID < 10000:
#     raise ValueError(f"SLURM_TASK_ID {TASK_ID} must be >= 1000")
prefix = f"PO_{ids[TASK_ID]:05d}"
df = spark.table(table).filter(f'property_id like "{prefix}%"')


# df = df.withColumn("prefix_partition", lit(prefix))


df = df.withColumn("prefix_partition", substring(col("property_id"), 1, 8))

df = df.select(
    "*",
    *[
        eval_array_str(col).alias(f"{col}_arr")
        for col in array_cols
        if array_cols[col] == "array<string>"
    ],
    *[
        eval_array_int(col).alias(f"{col}_arr")
        for col in array_cols
        if array_cols[col] == "array<int>"
    ],
    *[
        eval_array_array_int(col).alias(f"{col}_arr")
        for col in array_cols
        if array_cols[col] == "array<array<int>>"
    ],
    *[
        eval_array_array_double(col).alias(f"{col}_arr")
        for col in array_cols
        if array_cols[col] == "array<array<double>>"
    ],
    *[
        eval_array_double(col).alias(f"{col}_arr")
        for col in array_cols
        if array_cols[col] == "array<double>"
    ],
    *[
        eval_array_bool(col).alias(f"{col}_arr")
        for col in array_cols
        if array_cols[col] == "array<boolean>"
    ],
)
logger.info(f"Columns after arrayification: {df.columns}")
df = df.drop(*array_cols.keys()).withColumnsRenamed(
    {f"{col}_arr": col for col in array_cols.keys()}
)
logger.info(f"Columns after rename: {df.columns}")
df = df.select(
    [col.name for col in config_prop_arr_schema if col.name != "id"]
    + ["prefix_partition"]
)
logger.info(f"Columns after select: {df.columns}")


try:
    spark.sql("select ndb.create_tx()").show()
    df.write.mode("append").partitionBy("prefix_partition").saveAsTable(
        "ndb.colabfit.dev.copo_make_arrays",
    )
    spark.sql("select ndb.commit_tx()").show()
    logger.info(f"Successfully wrote data for prefix {prefix}")
except Exception as e:
    logger.error(f"Write failed for prefix {prefix}: {e}")
    try:
        spark.sql("select ndb.rollback_tx()").show()
    except Exception:
        pass
    raise

# for col in array_cols:
#     spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({col} {array_cols[col]})")


# spark.sql("select ndb.create_tx()").show()
# for col in array_cols:
#     spark.sql(f"ALTER TABLE {table} RENAME COLUMN {col} TO {col}_str")
# spark.sql("select ndb.commit_tx()").show()

# spark.sql("select ndb.create_tx()").show()
# spark.sql(f"UPDATE {table} SET atomic_forces = evalArrayArrayDouble(atomic_forces_str)")
# spark.sql("select ndb.commit_tx()").show()


# spark.sql("select ndb.create_tx()").show()
# spark.sql(f"UPDATE {table} SET cauchy_stress = evalArrayArrayDouble(cauchy_stress_str)")
# spark.sql("select ndb.commit_tx()").show()
