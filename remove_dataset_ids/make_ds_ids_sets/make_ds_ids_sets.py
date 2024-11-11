from ast import literal_eval

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("make_ds_ids_sets").getOrCreate()

# pos = spark.table("ndb.colabfit.dev.po_wip")
cos = spark.table("ndb.colabfit.dev.co_wip")
# css = spark.table('ndb.colabfit.dev.cs_wip')
# dss = spark.table("ndb.colabfit.dev.ds_wip")


unstring_and_make_set_list_udf = sf.udf(
    lambda x: str(list(set(literal_eval(x)))), StringType()
)

cos = cos.withColumn("dataset_ids", unstring_and_make_set_list_udf("dataset_ids"))

cos.write.mode("errorifexists").saveAsTable("ndb.colabfit.dev.co_dataset_id_sets")
