import os
from time import time

import pyspark.sql.functions as sf
from dotenv import load_dotenv
from pyspark.sql import SparkSession

"""

"""

load_dotenv()
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
spark = SparkSession.builder.appName("per_atom_convert").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

begin = time()

checknum = spark.table("ndb.colabfit.dev.pos_converted_units_checknums")
# checknum.filter(sf.col("cauchy_stress") != sf.col("cauchy_stress_checknum")).count()
# cauch = checknum.filter(
#     (sf.col("cauchy_stress") == sf.col("cauchy_stress_checknum"))
#     & (
#         (
#             ~sf.col("cauchy_stress_unit").isin(
#                 ["eV/angstrom^3", "eV/atom", "eV", "eV/angstrom"]
#             )
#         )
#     )
# )
# print(
#     "cauchy stress equal to checknum but not in eV/angstrom^3, eV/atom, or eV",
#     cauch.count(),
# )
# cauch.write.mode("overwrite").csv("cauchy_stress_unit_zeros.csv")
# print(
#     "atomic_forces_00",
#     checknum.filter(
#         sf.col("atomic_forces_00") != sf.col("atomic_forces_00_checknum")
#     ).count(),
# )
# print(
#     "atomic_forces_01",
#     checknum.filter(
#         sf.col("atomic_forces_01") != sf.col("atomic_forces_01_checknum")
#     ).count(),
# )
# print(
#     "atomic_forces_unit",
#     checknum.filter(sf.col("atomic_forces_unit") != "eV/angstrom").count(),
# )
print(
    "atomic_forces units distinct:",
    checknum.select("atomic_forces_unit").distinct().show(),
)
for unit_val in [
    "kcal/mol/angstrom",
    "hartree/angstrom",
    "eV/angstrom",
    "eV/angstrom^3",
]:
    print(
        f"atomic_forces unit {unit_val}",
        checknum.filter(sf.col("atomic_forces_unit") == unit_val).count(),
    )

# # atomization energy
# print(
#     "atomization_energy unit",
#     checknum.filter(
#         sf.col("atomization_energy") != sf.col("atomization_energy_unit_standardized")
#     ).count(),
# )
# print(
#     "atomization_energy unit and ref en",
#     checknum.filter(
#         sf.col("atomization_energy")
#         != sf.col("atomization_energy_unit_and_ref_en_checknum")
#     ).count(),
# )
# print(
#     "num units != eV",
#     checknum.filter(sf.col("atomization_energy_unit") != "eV").count(),
# )
# print(
#     "num ref energies",
#     checknum.filter(sf.col("atomization_energy_reference").isNotNull()).count(),
#     "\n",
# )
# # electronic band gap (just units)
# print(
#     "electronic_band_gap",
#     checknum.filter(
#         sf.col("electronic_band_gap") != sf.col("electronic_band_gap_unit_standardized")
#     ).count(),
# )
# print(
#     "num units != eV",
#     checknum.filter(sf.col("electronic_band_gap_unit") != "eV").count(),
#     "\n",
# )
# potential energy


print(
    "distinct potential_energy units:",
    checknum.select("potential_energy_unit").distinct().show(),
)
print("potential_energy == 0", checknum.filter(sf.col("potential_energy") == 0).count())
for distinct_unit in checknum.select("potential_energy_unit").distinct().collect():
    print(
        f"potential_energy unit {distinct_unit[0]}",
        checknum.filter(sf.col("potential_energy_unit") == distinct_unit[0]).count(),
    )
# print(
#     "potential_energy unit and refen",
#     checknum.filter(
#         sf.col("potential_energy")
#         != sf.col("potential_energy_unit_and_ref_en_checknum")
#     ).count(),
# )
# print(
#     "num units != eV", checknum.filter(sf.col("potential_energy_unit") != "eV").count()
# )
# print(
#     "num ref energies",
#     checknum.filter(sf.col("potential_energy_reference").isNotNull()).count(),
#     "\n",
# )
# # free energy
# print(
#     "free_energy unit",
#     checknum.filter(
#         sf.col("free_energy") != sf.col("free_energy_unit_standardized")
#     ).count(),
# )
# print(
#     "free_energy unit and refen",
#     checknum.filter(
#         sf.col("free_energy") != sf.col("free_energy_unit_and_ref_en_checknum")
#     ).count(),
# )
# print("num units != eV", checknum.filter(sf.col("free_energy_unit") != "eV").count())
# print(
#     "num ref energies",
#     checknum.filter(sf.col("free_energy_reference").isNotNull()).count(),
#     "\n",
# )
# # formation energy
# print(
#     "formation_energy unit",
#     checknum.filter(
#         sf.col("formation_energy") != sf.col("formation_energy_unit_standardized")
#     ).count(),
# )
# print(
#     "formation_energy unit and refen",
#     checknum.filter(
#         sf.col("formation_energy")
#         != sf.col("formation_energy_unit_and_ref_en_checknum")
#     ).count(),
# )
# print(
#     "num units != eV", checknum.filter(sf.col("formation_energy_unit") != "eV").count()
# )
# print(
#     "num ref energies",
#     checknum.filter(sf.col("formation_energy_reference").isNotNull()).count(),
#     "\n",
# )
# # adsorption energy
# print(
#     "adsorption_energy unit",
#     checknum.filter(
#         sf.col("adsorption_energy") != sf.col("adsorption_energy_unit_standardized")
#     ).count(),
# )
# print(
#     "adsorption_energy unit and refen",
#     checknum.filter(
#         sf.col("adsorption_energy")
#         != sf.col("adsorption_energy_unit_and_ref_en_checknum")
#     ).count(),
# )
# print(
#     "num units != eV", checknum.filter(sf.col("adsorption_energy_unit") != "eV").count()
# )
# print(
#     "num ref energies",
#     checknum.filter(sf.col("adsorption_energy_reference").isNotNull()).count(),
#     "\n",
# )
