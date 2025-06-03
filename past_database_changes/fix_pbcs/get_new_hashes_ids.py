import logging
import os
import sys
from ast import literal_eval
from hashlib import sha512
from time import time

import numpy as np
import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv
from vastdb.session import Session

print("top")
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
    elif isinstance(v, (list, tuple)):
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


def config_struct_hash(row):
    atomic_nums, cell, pbc, positions = row
    _hash = sha512()
    positions = np.array(positions)
    sort_ixs = np.lexsort(
        (
            positions[:, 2],
            positions[:, 1],
            positions[:, 0],
        )
    )
    sorted_positions = positions[sort_ixs]
    atomic_nums = np.array(atomic_nums)
    sorted_atomic_nums = atomic_nums[sort_ixs]
    _hash.update(bytes(format_for_hash(sorted_atomic_nums)))
    _hash.update(bytes(format_for_hash(cell)))
    _hash.update(bytes(format_for_hash(pbc)))
    _hash.update(bytes(format_for_hash(sorted_positions)))
    return str(int(_hash.hexdigest(), 16))


def main():
    prefixes = range(1000, 2000)
    for prefix in prefixes:
        id_prefix = f"CO_{prefix}"
        logger.info(f"Processing {id_prefix}")

        t1 = time()
        select_cols = sorted(
            ["id", "atomic_numbers", "pbc", "cell", "positions_00", "metadata_id"]
        )
        with sess.transaction() as tx:
            table = tx.bucket("colabfit").schema("dev").table("co_new_pbc")
            reader = table.select(
                predicate=table["id"].startswith(id_prefix), columns=select_cols
            )
            t2 = time()
            logger.info(f"selection: {t2-t1}")
            cos = reader.read_all()
            t3 = time()
            logger.info(f"read all: {t3-t2}")
        nrows = cos.num_rows
        logger.info(f"Number of rows: {nrows}")
        co_pandas = cos.to_struct_array().to_pandas()
        co_pandas = pd.json_normalize(co_pandas)

        cells = (
            co_pandas["cell"].str.replace("[", "").str.replace("]", "").str.split(",")
        )
        cells = [np.array(x, dtype="float").reshape(3, 3).tolist() for x in cells]

        atomic_numbers = (
            co_pandas["atomic_numbers"]
            .str.replace("[", "")
            .str.replace("]", "")
            .str.split(",")
        )
        atomic_numbers = [np.array(x, dtype="int").tolist() for x in atomic_numbers]

        atomic_numbers_lens = [len(x) for x in atomic_numbers]

        positions = (
            co_pandas["positions_00"]
            .str.replace("[", "")
            .str.replace("]", "")
            .str.split(",")
        )
        positions = [
            np.array(x, dtype="float").reshape(atomic_numbers_lens[i], 3).tolist()
            for i, x in enumerate(positions)
        ]

        pbc = [literal_eval(y) for y in co_pandas["pbc"]]
        metadata_id = co_pandas["metadata_id"]
        ids = co_pandas["id"]

        t4 = time()
        logger.info(f"Time to unstring rows: {t4-t3}")

        new_hashes = [
            rehash(row)
            for row in zip(atomic_numbers, cells, metadata_id, pbc, positions)
        ]
        new_struct_hashes = [
            config_struct_hash(row)
            for row in zip(atomic_numbers, cells, pbc, positions)
        ]
        new_ids = [f"CO_{n_hash[:25]}" for n_hash in new_hashes]
        id_hash_table_schema = pa.schema(
            [
                (col, pa.string())
                for col in ["id", "new_id", "new_hash", "new_struct_hash"]
            ]
        )
        new_table = pa.table(
            [pa.array(col) for col in [ids, new_ids, new_hashes, new_struct_hashes]],
            schema=id_hash_table_schema,
        )
        logger.info(f"Create table time: {time() - t4}")
        with sess.transaction() as tx:
            table = tx.bucket("colabfit").schema("dev").table("co_new_hashes")
            table.insert(new_table)
        logger.info(f"Insert time for {id_prefix}: {time() - t1}")


if __name__ == "__main__":
    main()
