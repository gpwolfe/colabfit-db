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
cos = spark.table("ndb.colabfit.ingest.co")
pos_name = "ndb.colabfit.dev.pos_converted_units"
pos = spark.table(pos_name)
pos = pos.join(
    cos.select("id", "nsites").withColumnRenamed("id", "configuration_id"),
    on="configuration_id",
    how="left",
)

table_name = "ndb.colabfit.dev.pos_converted_per_atom_final"
table_split = table_name.split(".")


for col in [
    "potential_energy",
    "free_energy",
    "adsorption_energy",
    "atomization_energy",
    "formation_energy",
]:
    pos = pos.withColumn(
        col,
        sf.when(sf.col(f"{col}_per_atom"), sf.col(col) * sf.col("nsites")).otherwise(
            sf.col(col)
        ),
    )

pos = pos.drop(
    "potential_energy_per_atom",
    "free_energy_per_atom",
    "formation_energy_per_atom",
    "adsorption_energy_per_atom",
    "atomization_energy_per_atom",
    "nsites",
    "old_id",
)
print(pos.printSchema())
print(pos.columns)
print(pos.schema)
print(pos.first())
pos.write.mode("overwrite").saveAsTable(table_name)
print(f"Time elapsed: {time() - begin}")
spark.stop()
