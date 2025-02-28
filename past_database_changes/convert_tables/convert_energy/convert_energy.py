import os
import pyspark.sql.functions as sf
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from time import time
from colabfit.tools.database import SparkDataLoader

"""
Notes:


"""
begin = time()
load_dotenv()
loader = SparkDataLoader(
    table_prefix="ndb.colabfit.dev",
    access_key=os.getenv("SPARK_ID"),
    access_secret=os.getenv("SPARK_KEY"),
    endpoint=os.getenv("SPARK_ENDPOINT"),
)


spark = SparkSession.builder.appName("per_atom_convert").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

old_table = "ndb.colabfit.dev.pos_converted_per_atom_final"
new_table = "ndb.colabfit.dev.pos_converted_to_energy_final"
db = spark.table(old_table)
db = db.withColumn(
    "energy", sf.coalesce(sf.col("free_energy"), sf.col("potential_energy"))
)
db = db.drop("free_energy", "potential_energy")
db.write.mode("overwrite").saveAsTable(new_table)

print(f"Table {new_table} created in {time()-begin:.2f} seconds")
print(db.printSchema())
print(db.first())
spark.stop()
