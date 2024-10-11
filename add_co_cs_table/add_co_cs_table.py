import os
from ast import literal_eval

import pyspark.sql.functions as sf

# from colabfit.tools.schema import co_cs_mapping_schema
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from vastdb.session import Session

load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
sess = Session(access=access, secret=secret, endpoint=endpoint)

spark = SparkSession.builder.appName("copy_to_dev").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


cos = spark.table("ndb.colabfit.dev.co_wip")
lit_eval_udf = sf.udf(literal_eval, ArrayType(StringType()))
cs_co = (
    cos.select("id", "configuration_set_ids")
    .where('configuration_set_ids != "[]"')
    .where("configuration_set_ids is not null")
)

cs_co = cs_co.withColumn(
    "configuration_set_ids",
    lit_eval_udf(sf.col("configuration_set_ids")),
)

cs_co = (
    cs_co.withColumn("configuration_set_id", sf.explode("configuration_set_ids"))
    .drop("configuration_set_ids")
    .withColumnRenamed("id", "configuration_id")
)
cs_co.show(5, False)
cs_co_table_name = "ndb.colabfit.dev.cs_co_map_wip"
cs_co.write.mode("overwrite").saveAsTable(cs_co_table_name)


sorted_columns = ["configuration_set_id", "configuration_id"]
unsorted_columns = []
with sess.transaction() as tx:
    table_split = cs_co_table_name.split(".")
    table = tx.bucket(table_split[1]).schema(table_split[2]).table(table_split[3])
    table.create_projection(
        projection_name="cs_id-co_id",
        sorted_columns=sorted_columns,
        unsorted_columns=unsorted_columns,
    )
