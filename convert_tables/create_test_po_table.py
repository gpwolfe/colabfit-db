import pyspark.sql.functions as sf

from dotenv import load_dotenv

from colabfit.tools.database import SparkDataLoader

load_dotenv()
loader = SparkDataLoader(table_prefix="ndb.colabfit.dev")
ds = loader.spark.table("ndb.colabfit.ingest.po")
ds = ds.withColumn("energy_conjugate_with_atomic_forces_unit", sf.lit(1))
ds = ds.withColumn("energy_conjugate_with_atomic_forces", sf.lit(1))
for col in [
    "potential_energy_per_atom",
    "free_energy_reference_unit",
    "potential_energy_reference_unit",
    "potential_energy",
    "formation_energy_reference",
    "old_id",
    "potential_energy_reference",
    "potential_energy_unit",
    "atomization_energy_per_atom",
    "atomization_energy_reference",
    "formation_energy_per_atom",
    "electronic_band_gap_property_id",
    "free_energy_unit",
    "free_energy_reference",
    "free_energy",
    "free_energy_per_atom",
    "atomization_energy_reference_unit",
    "adsorption_energy_reference",
    "formation_energy_reference_unit",
    "adsorption_energy_per_atom",
    "adsorption_energy_reference_unit",
]:
    ds = ds.drop(col)
ds = ds.limit(20000000)
ds.write.mode("append").saveAsTable("ndb.colabfit.dev.pos_from_ingest_large")
