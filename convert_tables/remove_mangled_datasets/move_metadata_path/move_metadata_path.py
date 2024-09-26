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

po_table = "ndb.colabfit.dev.po_remove_dataset_ids_stage2"
to_po_table = "ndb.colabfit.dev.po_remove_dataset_ids_stage3"
po = spark.table(po_table)


@sf.udf
def trim_prefix(metadata_path):
    if metadata_path is None:
        return None
    if metadata_path.startswith("/vdev/colabfit-data/MD"):
        return metadata_path.replace("/vdev/colabfit-data", "data")
    elif metadata_path.startswith("/vdev/colabfit-data/data"):
        return metadata_path.replace("/vdev/colabfit-data/data", "data")

    return metadata_path


po = po.withColumn("metadata_path", trim_prefix(po.metadata_path))
print("writing pos to table")
po.printSchema()
po.write.mode("overwrite").saveAsTable(to_po_table, schema=property_object_schema)
print(f"Time taken: {time() - t}")
spark.stop()
