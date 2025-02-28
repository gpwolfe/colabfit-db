from time import time
from colabfit.tools.schema import (
    dataset_schema,
    property_object_schema,
    config_schema,
)
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

t = time()
spark = SparkSession.builder.appName("copy_to_dev").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

co_table = "ndb.colabfit.dev.co_remove_dataset_ids_stage3"
to_co_table = "ndb.colabfit.dev.co_remove_dataset_ids_stage4"
co = spark.table(co_table)


@sf.udf
def trim_prefix(metadata_path):
    if metadata_path is None:
        return None
    if metadata_path.startswith("/vdev/colabfit-data/MD"):
        return metadata_path.replace("/vdev/colabfit-data", "data")
    elif metadata_path.startswith("/vdev/colabfit-data/data"):
        return metadata_path.replace("/vdev/colabfit-data/data", "data")
    else:
        return metadata_path


co = co.withColumn("metadata_path", trim_prefix(co.metadata_path))
print("writing cos to table")
co.printSchema()
co.write.mode("overwrite").saveAsTable(to_co_table, schema=property_object_schema)
print(f"Time taken: {time() - t}")
spark.stop()
