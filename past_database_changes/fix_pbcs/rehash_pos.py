import logging
import os
import sys
from ast import literal_eval
from hashlib import sha512
from multiprocessing import Pool
from time import time

import numpy as np
import pandas as pd
import pyarrow as pa
from colabfit.tools.vast.schema import property_object_schema
from dotenv import load_dotenv
from vastdb.session import Session

print("top")
table_name = "po_new_hashes"
load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
sess = Session(access=access, secret=secret, endpoint=endpoint)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(f"{__name__}.hasher")
logger.setLevel("INFO")
logger.addHandler(handler)
logger.info(f"table_name: {table_name}")


_hash_ignored_fields = [
    "id",
    "hash",
    "last_modified",
    "multiplicity",
    "metadata_path",
    "metadata_size",
    "mean_force_norm",
    "max_force_norm",
]
unique_identifier_kw = [
    k for k in property_object_schema.fieldNames() if k not in _hash_ignored_fields
]
po_id_schema = pa.schema(
    [("id", pa.string()), ("new_id", pa.string()), ("new_hash", pa.string())]
)


def format_for_hash(v):
    if isinstance(v, np.ndarray):
        if np.issubdtype(v.dtype, np.floating):
            return np.round(v.astype(np.float64), decimals=16)
        elif np.issubdtype(v.dtype, np.integer):
            return v.astype(np.int64)
        elif np.issubtype(v.dtype, np.bool):
            return v.astype(np.int64)
        else:
            return v
    elif isinstance(v, list):
        return np.array(v).data.tobytes()
    elif isinstance(v, tuple):
        return np.array(v).data.tobytes()
    elif isinstance(v, dict):
        return str(v).encode("utf-8")
    elif isinstance(v, str):
        return v.encode("utf-8")
    elif isinstance(v, (int, float)):
        return np.array(v).data.tobytes()
    else:
        return v


def rehash(row):
    new_hash = sha512()
    for k in row:
        if k is None or k == "[]":
            continue
        new_hash.update(format_for_hash(k))
    return str(int(new_hash.hexdigest(), 16))


def get_session():
    endpoint = os.getenv("VAST_DB_ENDPOINT")
    access = os.getenv("VAST_DB_ACCESS")
    secret = os.getenv("VAST_DB_SECRET")
    return Session(access=access, secret=secret, endpoint=endpoint)


def get_rows_by_prefix(id_prefix):
    select_cols = sorted(unique_identifier_kw)
    sess = get_session()
    with sess.transaction() as tx:
        table = tx.bucket("colabfit").schema("dev").table("po_remapped_co_ids")
        reader = table.select(
            predicate=table["id"].startswith(id_prefix), columns=["id"] + select_cols
        )
        pos = reader.read_all()
    return pos


def get_forces(force_col_vals):
    forces = []
    for f in force_col_vals:
        try:
            if f is None:
                forces.append(None)
            else:
                forces.append(literal_eval(f))
        except Exception as e:
            print(e)
            arr = np.array(
                [float(x) for x in f.replace("[", "").replace("]", "").split(", ")]
            )
            forces.append(np.reshape(arr, (arr.shape[0] // 3, 3)).tolist())
            print(f)
    return forces


def process_rows(pos):
    po_pandas = pos.to_struct_array().to_pandas()
    po_pandas = pd.json_normalize(po_pandas)
    forces = get_forces(po_pandas["atomic_forces_00"].values)
    stress = (
        po_pandas["cauchy_stress"]
        .str.replace("[", "")
        .str.replace("]", "")
        .str.split(",")
    )
    stress = [None if x == [""] else x for x in stress.values]
    stress = [
        np.array(x, dtype="float").reshape(3, 3).tolist() if x is not None else x
        for x in stress
    ]

    po_pandas = po_pandas.to_dict(orient="list")
    ids = po_pandas["id"]

    # The below are in sorted key order
    new_hashes = [
        rehash(row)
        for row in zip(
            po_pandas["adsorption_energy"],
            forces,
            po_pandas["atomization_energy"],
            stress,
            po_pandas["cauchy_stress_volume_normalized"],
            po_pandas["chemical_formula_hill"],
            po_pandas["configuration_id"],
            po_pandas["dataset_id"],
            po_pandas["electronic_band_gap"],
            po_pandas["electronic_band_gap_type"],
            po_pandas["energy"],
            po_pandas["formation_energy"],
            po_pandas["metadata_id"],
            po_pandas["method"],
            po_pandas["software"],
        )
    ]

    new_ids = [f"PO_{n_hash[:25]}" for n_hash in new_hashes]
    id_table = pa.Table.from_arrays([ids, new_ids, new_hashes], schema=po_id_schema)

    return id_table


def insert_new_hashes(id_table):
    sess = get_session()
    with sess.transaction() as tx:
        table = tx.bucket("colabfit").schema("dev").table("po_rehashed")
        table.insert(id_table)


def process_prefix(prefix):
    id_prefix = f"PO_{prefix}"
    logger.info(f"Processing {id_prefix}")
    t1 = time()

    pos = get_rows_by_prefix(id_prefix)

    logger.info(f"Number of rows for {id_prefix}: {pos.num_rows}")
    t2 = time()
    logger.info(f"Selection time for {id_prefix}: {t2 - t1}")

    new_columns = process_rows(pos)
    insert_new_hashes(new_columns)
    t3 = time()
    logger.info(
        f"------>> {id_prefix} finished in: {t3 - t1} sec -- {((t3 - t1) / 60)} min"
    )


missing = [
    1008,
    1009,
    1018,
    1019,
    1020,
    1021,
    1022,
    1023,
    1024,
    1025,
    1026,
    1027,
    1028,
    1029,
    1030,
    1031,
    1032,
    1033,
    1034,
    1035,
    1036,
    1037,
    1038,
    1039,
]


def main(start):
    end = start + 8
    if start > len(missing):
        logger.info("Start index exceeds missing list length. Exiting.")
        return
    if end > len(missing):
        end = len(missing)
    prefixes = missing[start:end]
    logger.info(f"Processing missing prefixes: {prefixes}")
    logger.info(f"Processing prefixes from index {start} to {end}")
    with Pool(processes=4) as pool:
        pool.map(process_prefix, prefixes)
    logger.info("Done!")


if __name__ == "__main__":
    start = int(sys.argv[1])
    main(start)
