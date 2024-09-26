print("top of script")
import os

from dotenv import load_dotenv

from colabfit.tools.database import SparkDataLoader

import pyspark.sql.functions as sf

print("load dotenv")
load_dotenv()
loader = SparkDataLoader(table_prefix="ndb.colabfit.dev")
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
loader.set_vastdb_session(
    endpoint=endpoint, access_key=access_key, access_secret=access_secret
)
# # To set different table names for more stable test db:
loader.config_table = "ndb.colabfit.dev.co_convert"
loader.config_set_table = "ndb.colabfit.dev.cs_convert"
loader.dataset_table = "ndb.colabfit.dev.ds_convert"
loader.prop_object_table = "ndb.colabfit.dev.pos_convert_v3"
print("loading ds")
# dss = loader.spark.table("ndb.colabfit.ingest.ds")
# dss = dss.filter(sf.col("nconfigurations") > 1000).limit(30)
# dss.write.mode("append").saveAsTable(loader.dataset_table)
# print("loading pos")
# ds_ids = [
#     x["id"] for x in loader.spark.table(loader.dataset_table).select("id").collect()
# ]
# pos = loader.spark.table("ndb.colabfit.ingest.po_v3")
# pos = pos.filter(sf.col("dataset_id").isin(ds_ids))
# pos.write.mode("overwrite").saveAsTable(loader.prop_object_table)
# dss = loader.spark.table(loader.dataset_table)
# ds_ids = [x["id"] for x in dss.select("id").collect()]
# print("loading cs")
# css = loader.spark.table("ndb.colabfit.ingest.cs")
# css = css.filter(sf.col("dataset_id").isin(ds_ids))
# css.write.mode("append").saveAsTable(loader.config_set_table)
print("loading co")
pos = loader.spark.table(loader.prop_object_table)
co_ids = [x["configuration_id"] for x in pos.select("configuration_id").collect()]
co_ids_ingested = [
    x["id"] for x in loader.spark.table(loader.config_table).select("id").collect()
]
co_ids = list(set(co_ids) - set(co_ids_ingested))
print(co_ids[0])
cos = loader.spark.table("ndb.colabfit.ingest.co")
for i, po_id_batch in enumerate(
    [co_ids[i : i + 500] for i in range(0, len(co_ids), 500)]
):
    print(i)
    cos_to_write = cos.filter(sf.col("id").isin(po_id_batch))
    cos_to_write.write.mode("append").saveAsTable(loader.config_table)
