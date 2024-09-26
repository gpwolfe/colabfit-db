from time import time
import pyspark.sql.functions as sf
from colabfit.tools.schema import dataset_schema
from pyspark.sql import SparkSession

begin = time()
spark = SparkSession.builder.appName("per_atom_convert").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

old_ds_table = "ndb.colabfit.ingest.ds"
new_ds_table = "ndb.colabfit.dev.datasets_recreated"
ds_df = spark.table(old_ds_table)
prop_df = spark.table("ndb.colabfit.dev.pos_converted_rehashed_final")
print("prop_df distinct df count", ds_df.select("id").distinct().count())
en_props = (
    prop_df.groupBy("dataset_id")
    .agg(
        sf.mean("energy").alias("energy_mean"),
        sf.variance("energy").alias("energy_variance"),
    )
    .select("dataset_id", "energy_mean", "energy_variance")
).withColumnRenamed("dataset_id", "id")
print("en_props distinct count", en_props.select("id").distinct().count())
new_ds_df = ds_df.join(en_props, on="id", how="left")
print(new_ds_df.columns)
print("new_ds_df dsids count", new_ds_df.select("id").distinct().count())
aggregate_df = (
    prop_df.groupBy("dataset_id")
    .agg(
        sf.count("energy").alias("energy_count"),
        sf.count("electronic_band_gap").alias("electronic_band_gap_count"),
        sf.count("formation_energy").alias("formation_energy_count"),
        sf.count("adsorption_energy").alias("adsorption_energy_count"),
        sf.count("atomization_energy").alias("atomization_energy_count"),
        sf.count("atomic_forces_00").alias("atomic_forces_count"),
        sf.count("cauchy_stress").alias("cauchy_stress_count"),
        sf.count("*").alias("nproperty_objects"),
    )
    .drop("id")
    .withColumnRenamed("dataset_id", "id")
)
print("aggregate count", aggregate_df.count())
print("aggregate_df_cols", aggregate_df.columns)
drop_cols = [
    "electronic_band_gap_count",
    "formation_energy_count",
    "adsorption_energy_count",
    "atomization_energy_count",
    "atomic_forces_count",
    "cauchy_stress_count",
]
new_ds_df = new_ds_df.drop(*drop_cols)
new_ds_df = new_ds_df.join(aggregate_df, on="id", how="left").select(
    *[name for name in dataset_schema.fieldNames()]
)
print(new_ds_df.columns)
print(
    "new_ds_df dsids count after count joins",
    new_ds_df.select("id").distinct().count(),
)

print(new_ds_df.printSchema())
print(new_ds_df.first())
new_ds_df.write.mode("overwrite").saveAsTable(new_ds_table)
spark.stop()
print("Time taken: ", time() - begin)
