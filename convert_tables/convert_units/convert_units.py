import itertools
import os
from ast import literal_eval
from collections import namedtuple
from time import time

import numpy as np
import pyarrow as pa
import pyspark.sql.functions as sf
from ase.units import create_units
from dotenv import load_dotenv
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import ArrayType, DoubleType, StringType

from colabfit.tools.database import SparkDataLoader
from colabfit.tools.utilities import spark_schema_to_arrow_schema, stringify_df_val

"""
Notes:
following ids should have reference energies removed:
DS_m6etjqhlu40m_0
DS_15m7etw1om8a_0
DS_0dbwoxq96gga_0

"""
begin = time()
load_dotenv()
loader = SparkDataLoader(
    table_prefix="ndb.colabfit.dev",
    access_key=os.getenv("SPARK_ID"),
    access_secret=os.getenv("SPARK_KEY"),
    endpoint=os.getenv("SPARK_ENDPOINT"),
)


UNITS = create_units("2014")

UNITS["angstrom"] = UNITS["Ang"]
UNITS["bohr"] = UNITS["Bohr"]
UNITS["hartree"] = UNITS["Hartree"]
UNITS["rydberg"] = UNITS["Rydberg"]
UNITS["debye"] = UNITS["Debye"]
UNITS["kbar"] = UNITS["bar"] * 1000
UNITS["kilobar"] = UNITS["kbar"]


prop_info = namedtuple("prop_info", ["unit", "dtype"])
energy_info = prop_info(["eV"], float)
force_info = prop_info(["eV/angstrom", "eV/angstrom^3"], list)
stress_info = prop_info(["eV/angstrom^3"], list)
MAIN_KEY_MAP = {
    "potential_energy": energy_info,
    "atomic_forces_00": force_info,
    "cauchy_stress": stress_info,
    "atomization_energy": energy_info,
    "formation_energy": energy_info,
    "free_energy": energy_info,
    "electronic_band_gap": energy_info,
}


# def standardize_energy(row: Row):
#     """
#     For each key in :attr:`self.property_map`, convert :attr:`self.edn[key]`
#     from its original units to the expected ColabFit-compliant units.
#     """
#     rowdict = row.asDict()
#     for prop_name, val in rowdict.items():
#         if prop_name not in MAIN_KEY_MAP.keys():
#             continue
#         if val is None:
#             continue
#         p_info = MAIN_KEY_MAP[prop_name]
#         unit_col = f"{prop_name}_unit"
#         if prop_name[-2:] == "00":
#             unit_col = f"{prop_name[:-3]}_unit"
#         units = rowdict[unit_col]
#         if p_info.dtype == list:
#             val = literal_eval(val)
#             prop_val = np.array(val, dtype=np.float64)
#         else:
#             prop_val = val
#         ref_en_col = f"{prop_name}_reference"
#         if ref_en_col in rowdict and rowdict[ref_en_col] is not None:
#             if rowdict[f"{ref_en_col}_unit"] != units:
#                 raise RuntimeError(
#                     "Units of the reference energy and energy must be the same"
#                 )
#             else:
#                 prop_val += rowdict[ref_en_col]
#         per_atom_col = f"{prop_name}_per_atom"
#         if per_atom_col in rowdict:
#             if rowdict[per_atom_col] is True:
#                 if rowdict["nsites"] is None:
#                     raise RuntimeError("nsites must be provided to convert per-atom")
#                 prop_val *= rowdict["nsites"]
#         if units not in p_info.unit:
#             split_units = list(
#                 itertools.chain.from_iterable(
#                     [
#                         sp.split("^")
#                         for sp in itertools.chain.from_iterable(
#                             [sp.split("/") for sp in units.split("*")]
#                         )
#                     ]
#                 )
#             )
#             prop_val *= float(UNITS[split_units[0]])
#             for u in split_units[1:]:
#                 if units[units.find(u) - 1] == "*":
#                     prop_val *= UNITS[u]
#                 elif units[units.find(u) - 1] == "/":
#                     prop_val /= UNITS[u]
#                 elif units[units.find(u) - 1] == "^":
#                     try:
#                         prop_val = np.power(prop_val, int(u))
#                     except Exception:
#                         raise RuntimeError(
#                             f"There may be something wrong with the units: {u}"
#                         )
#                 else:
#                     raise RuntimeError(
#                         f"There may be something wrong with the units: {u}"
#                     )
#         if p_info.dtype == list:
#             prop_val = prop_val.tolist()
#         rowdict[prop_name] = prop_val
#         rowdict[unit_col] = p_info.unit[0]
#     return Row(**rowdict)


@sf.udf(returnType=DoubleType())
def standardize_energy_udf(en_col, unit_col):
    val = en_col
    if val is None:
        return None
    units = unit_col
    prop_val = val
    if units != "eV":
        split_units = list(
            itertools.chain.from_iterable(
                [
                    sp.split("^")
                    for sp in itertools.chain.from_iterable(
                        [sp.split("/") for sp in units.split("*")]
                    )
                ]
            )
        )
        prop_val *= float(UNITS[split_units[0]])
        for u in split_units[1:]:
            if units[units.find(u) - 1] == "*":
                prop_val *= UNITS[u]
            elif units[units.find(u) - 1] == "/":
                prop_val /= UNITS[u]
            elif units[units.find(u) - 1] == "^":
                try:
                    prop_val = np.power(prop_val, int(u))
                except Exception:
                    raise RuntimeError(
                        f"There may be something wrong with the units: {u}"
                    )
            else:
                raise RuntimeError(f"There may be something wrong with the units: {u}")
    return prop_val


ref_en_to_null = [
    "DS_m6etjqhlu40m_0",
    "DS_15m7etw1om8a_0",
    "DS_0dbwoxq96gga_0",
    "DS_qde4v70hhmmj_0",
]


@sf.udf(returnType=DoubleType())
def standardize_ref_energy_udf(en_col, unit_col, ds_id_col):
    val = en_col
    if val is None or ds_id_col in ref_en_to_null:
        return None
    units = unit_col
    prop_val = val
    if units != "eV":
        split_units = list(
            itertools.chain.from_iterable(
                [
                    sp.split("^")
                    for sp in itertools.chain.from_iterable(
                        [sp.split("/") for sp in units.split("*")]
                    )
                ]
            )
        )
        prop_val *= float(UNITS[split_units[0]])
        for u in split_units[1:]:
            if units[units.find(u) - 1] == "*":
                prop_val *= UNITS[u]
            elif units[units.find(u) - 1] == "/":
                prop_val /= UNITS[u]
            elif units[units.find(u) - 1] == "^":
                try:
                    prop_val = np.power(prop_val, int(u))
                except Exception:
                    raise RuntimeError(
                        f"There may be something wrong with the units: {u}"
                    )
            else:
                raise RuntimeError(f"There may be something wrong with the units: {u}")
    return prop_val


@sf.udf(returnType=StringType())
def standardize_array_udf(af_col, unit_col, unit):
    val = af_col
    if val is None or val == "[]":
        return "[]"
    units = unit_col
    prop_val = val
    val = literal_eval(val)
    prop_val = np.array(val, dtype=np.float64)
    if units not in unit:
        try:
            split_units = list(
                itertools.chain.from_iterable(
                    [
                        sp.split("^")
                        for sp in itertools.chain.from_iterable(
                            [sp.split("/") for sp in units.split("*")]
                        )
                    ]
                )
            )
            prop_val *= float(UNITS[split_units[0]])
            for u in split_units[1:]:
                if units[units.find(u) - 1] == "*":
                    prop_val *= UNITS[u]
                elif units[units.find(u) - 1] == "/":
                    prop_val /= UNITS[u]
                elif units[units.find(u) - 1] == "^":
                    try:
                        prop_val = np.power(prop_val, int(u))
                    except Exception:
                        raise RuntimeError(
                            f"There may be something wrong with the units: {u}"
                        )
                else:
                    raise RuntimeError(
                        f"There may be something wrong with the units: {u}"
                    )
        except Exception as e:
            print(f"error: {e}")
            print(f"units: {units}")
    prop_val = str(prop_val.tolist())
    return prop_val


spark = SparkSession.builder.appName("convert_units").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
all_pos_name = "ndb.colabfit.ingest.po_v3"
all_pos = spark.table(all_pos_name)


table_name = "ndb.colabfit.dev.pos_converted_units"
table_split = table_name.split(".")

pos = all_pos
for col in [
    "potential_energy",
    "free_energy",
    "electronic_band_gap",
    "adsorption_energy",
    "atomization_energy",
    "formation_energy",
]:
    pos = pos.withColumn(
        col,
        standardize_energy_udf(sf.col(col), sf.col(f"{col}_unit")),
    )

for ref_col in [
    "potential_energy_reference",
    "free_energy_reference",
    "adsorption_energy_reference",
    "atomization_energy_reference",
    "formation_energy_reference",
]:
    pos = pos.withColumn(
        ref_col,
        standardize_ref_energy_udf(
            sf.col(ref_col), sf.col(f"{ref_col}_unit"), sf.col("dataset_id")
        ),
    )

for col in [
    "potential_energy",
    "free_energy",
    "adsorption_energy",
    "atomization_energy",
    "formation_energy",
]:
    pos = pos.withColumn(
        f"{col}",
        sf.when(
            sf.col(f"{col}_reference").isNotNull(),
            sf.col(col) + sf.col(f"{col}_reference"),
        ).otherwise(sf.col(col)),
    )

for col in ["atomic_forces_00", "atomic_forces_01"]:
    pos = pos.withColumn(
        f"{col}",
        standardize_array_udf(
            sf.col(col),
            sf.col("atomic_forces_unit"),
            sf.lit(force_info.unit),
        ),
    )

pos = pos.withColumn(
    "cauchy_stress",
    standardize_array_udf(
        sf.col("cauchy_stress"),
        sf.col("cauchy_stress_unit"),
        sf.lit(stress_info.unit),
    ),
)

pos = pos.drop(
    "potential_energy_unit",
    "free_energy_unit",
    "electronic_band_gap_unit",
    "adsorption_energy_unit",
    "atomization_energy_unit",
    "formation_energy_unit",
    "atomic_forces_unit",
    "cauchy_stress_unit",
    "potential_energy_reference",
    "free_energy_reference",
    "adsorption_energy_reference",
    "atomization_energy_reference",
    "formation_energy_reference",
    "potential_energy_reference_unit",
    "free_energy_reference_unit",
    "adsorption_energy_reference_unit",
    "atomization_energy_reference_unit",
    "formation_energy_reference_unit",
)
print(pos.printSchema())
print(pos.columns)
print(pos.schema)
print(pos.first())
pos.write.mode("overwrite").saveAsTable(table_name)

print(f"Time: {time() - begin}")

# for col in [
#     "potential_energy",
#     "free_energy",
#     "adsorption_energy",
#     "atomization_energy",
#     "formation_energy",
# ]:
#     print(
#         col,
#         pos.select(col, f"{col}_standardized_units")
#         .filter(sf.col(col) != sf.col(f"{col}_standardized_units"))
#         .count(),
#     )

spark.stop()
