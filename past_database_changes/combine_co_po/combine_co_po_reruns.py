from __future__ import print_function

import logging
import os
import sys
from time import time

import pyarrow as pa
import vastdb
from vastdb.config import QueryConfig

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")
logger.addHandler(handler)

SLURM_TASK_ID = int(os.environ.get("SLURM_ARRAY_TASK_ID"))
# SLURM_TASK_ID = 1

config = QueryConfig(
    limit_rows_per_sub_split=10_000,
    rows_per_split=1_000_000,
)

PREFIXES = [
    f"PO_1{x}"
    for x in [
        2062,
        2063,
        2065,
        2068,
        2070,
        2076,
        3907,
        3908,
        3909,
        3910,
        3911,
        3912,
        3915,
        3918,
        3929,
        3930,
    ]
]


def get_vastdb_session():
    endpoint = "http://10.32.38.210"
    with open(f"/home/{os.environ['USER']}/.vast-dev/access_key_id", "r") as f:
        access_key = f.read().rstrip("\n")
    with open(f"/home/{os.environ['USER']}/.vast-dev/secret_access_key", "r") as f:
        secret_key = f.read().rstrip("\n")
    return vastdb.connect(endpoint=endpoint, access=access_key, secret=secret_key)


def main():
    prefixes = PREFIXES
    task_id = SLURM_TASK_ID
    if task_id > len(prefixes):
        raise ValueError(
            f"SLURM_TASK_ID {task_id} exceeds number of prefixes {len(prefixes)}"
        )
    prefix = prefixes[task_id]
    start = time()
    logger.info(f"Starting combine_co_po with prefix: {prefix}")
    session = get_vastdb_session()

    with session.transaction() as tx:
        po = tx.bucket("colabfit-prod").schema("prod").table("po")
        po_table = po.select(predicate=po["id"].startswith(prefix), config=config)
        po_table = po_table.read_all()
        logger.info(f"Read PO rows: {po_table.num_rows}")

    co_ids = list(set(po_table.column("configuration_id").to_pylist()))
    co_id_batches = [co_ids[i : i + 10000] for i in range(0, len(co_ids), 10000)]
    co_table_batches = []
    with session.transaction() as tx:
        for batch in co_id_batches:
            logger.info(f"CO rows: {len(batch)}")
            co = tx.bucket("colabfit-prod").schema("prod").table("co")
            co_table = co.select(
                predicate=co["id"].isin(batch), config=config
            ).read_all()
            co_table_batches.append(co_table)
            # TODO: solve metadata
    co_table_full = pa.concat_tables(co_table_batches)
    co_table_full = co_table_full.drop_columns(
        [
            "last_modified",
            "dataset_ids",
            "metadata_id",
            # "metadata_path",
            "metadata_size",
        ]
    )
    co_column_names = [
        (
            name
            if name not in ["id", "hash", "metadata_path"]
            else f"configuration_{name}"
        )
        for name in co_table_full.column_names
    ]
    co_table_full = co_table_full.rename_columns(co_column_names)
    logger.info(f"CO num rows: {co_table_full.num_rows}")
    logger.info(f"CO column names: {co_table_full.column_names}")

    po_table = po_table.drop_columns(
        [
            "chemical_formula_hill",
            "metadata_id",
            # "metadata_path",
            "metadata_size",
        ]
    )
    po_column_names = [
        name if name not in ["id", "hash", "metadata_path"] else f"property_{name}"
        for name in po_table.column_names
    ]
    po_table = po_table.rename_columns(po_column_names)
    logger.info(f"PO num rows: {po_table.num_rows}")
    logger.info(f"PO column names: {po_table.column_names}")
    final_cols = [
        "property_id",
        "property_hash",
        "last_modified",
        "dataset_id",
        "multiplicity",
        "software",
        "method",
        "energy",
        "atomic_forces",
        "cauchy_stress",
        "cauchy_stress_volume_normalized",
        "electronic_band_gap",
        "electronic_band_gap_type",
        "formation_energy",
        "adsorption_energy",
        "atomization_energy",
        "max_force_norm",
        "mean_force_norm",
        "energy_above_hull",
        "configuration_id",
        "configuration_hash",
        "structure_hash",
        "cell",
        "positions",
        "pbc",
        "chemical_formula_hill",
        "chemical_formula_reduced",
        "chemical_formula_anonymous",
        "elements",
        "elements_ratios",
        "atomic_numbers",
        "nsites",
        "nelements",
        "nperiodic_dimensions",
        "dimension_types",
        "names",
        "labels",
        "property_metadata_path",
        "configuration_metadata_path",
    ]

    # Join
    merge_table = po_table.join(
        co_table_full,
        keys="configuration_id",
        right_keys="configuration_id",
        join_type="inner",
    )
    merge_table = merge_table.select(final_cols)
    logger.info(f"Combined: {merge_table.num_rows}")
    logger.info(f"Combined column names: {merge_table.column_names}")
    # if SLURM_TASK_ID == 0:
    #     with session.transaction() as tx:
    #         schema = tx.bucket("colabfit-prod").schema("prod")
    #         schema.create_table("co_po_merged_innerjoin", merge_table.schema)
    #         t = (
    #             tx.bucket("colabfit-prod")
    #             .schema("prod")
    #             .table("co_po_merged_innerjoin")
    #         )
    #         t.insert(merge_table)
    # else:
    with session.transaction() as tx:
        t = tx.bucket("colabfit-prod").schema("prod").table("co_po_merged_innerjoin")
        t.insert(merge_table)

    logger.info(f"Total runtime: {time() - start:.2f} seconds")


if __name__ == "__main__":
    main()
