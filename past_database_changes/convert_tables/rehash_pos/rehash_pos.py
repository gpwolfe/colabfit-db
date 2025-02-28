import datetime
import itertools
from ast import literal_eval
from time import time

import dateutil.parser
from pyspark.sql import Row, SparkSession

from colabfit.tools.utilities import _hash

spark = SparkSession.builder.appName("per_atom_convert").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

begin = time()


def rehash_property_objects(spark_row: Row):
    """
    Rehash property object row after changing values of one or
    more of the columns corresponding to hash_keys defined below.

    """
    hash_keys = [
        "adsorption_energy",
        "atomic_forces",
        "atomization_energy",
        "cauchy_stress",
        "cauchy_stress_volume_normalized",
        "chemical_formula_hill",
        "configuration_id",
        "dataset_id",
        "electronic_band_gap",
        "electronic_band_gap_type",
        "energy",
        "formation_energy",
        "metadata_id",
        "method",
        "software",
    ]
    spark_dict = spark_row.asDict()
    if spark_dict["atomic_forces_01"] is None:
        spark_dict["atomic_forces"] = literal_eval(spark_dict["atomic_forces_00"])
    else:
        spark_dict["atomic_forces"] = list(
            itertools.chain(
                *[
                    literal_eval(spark_dict[f"atomic_forces_{i:02}"])
                    for i in range(1, 19)
                ]
            )
        )
    if spark_dict["cauchy_stress"] is not None:
        spark_dict["cauchy_stress"] = literal_eval(spark_dict["cauchy_stress"])
    spark_dict["last_modified"] = dateutil.parser.parse(
        datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    spark_dict["old_hash"] = spark_dict["hash"]
    spark_dict["hash"] = _hash(spark_dict, hash_keys, include_keys_in_hash=False)
    if spark_dict["cauchy_stress"] is not None:
        spark_dict["cauchy_stress"] = str(spark_dict["cauchy_stress"])
    id = f'PO_{spark_dict["hash"]}'
    if len(id) > 28:
        id = id[:28]
    spark_dict["id"] = id
    return Row(**{k: v for k, v in spark_dict.items() if k != "atomic_forces"})


old_table = "ndb.colabfit.dev.pos_converted_to_energy_final"
new_table = "ndb.colabfit.dev.pos_converted_rehashed_final"

df = spark.table(old_table)
schema = df.schema
new_schema = schema.add("old_hash", "string")
df = df.rdd.map(rehash_property_objects).toDF(schema=new_schema)
df.write.mode("overwrite").saveAsTable(new_table)
