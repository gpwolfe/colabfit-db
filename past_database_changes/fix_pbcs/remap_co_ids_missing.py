import logging
import os
import sys
from time import time
from multiprocessing import Pool

import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv
from vastdb.session import Session
from colabfit.tools.vast.schema import config_schema
from colabfit.tools.vast.utilities import (
    spark_schema_to_arrow_schema,
)

print("top")
table_name = "co_remapped_hashes"

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


arrow_schema = spark_schema_to_arrow_schema(config_schema)
int_cols = {
    col: "int32"
    for col in arrow_schema.names
    if pa.types.is_integer(arrow_schema.field(col).type)
}


def get_session():
    endpoint = os.getenv("VAST_DB_ENDPOINT")
    access = os.getenv("VAST_DB_ACCESS")
    secret = os.getenv("VAST_DB_SECRET")
    return Session(access=access, secret=secret, endpoint=endpoint)


def get_new_hash_rows_by_prefix(id_prefix):
    select_cols = ["id", "new_id", "new_hash", "new_struct_hash"]
    sess = get_session()
    with sess.transaction() as tx:
        table = tx.bucket("colabfit").schema("dev").table("co_new_hashes")
        reader = table.select(
            predicate=table["id"].startswith(id_prefix), columns=select_cols
        )
        cos = reader.read_all()

    return cos


def get_old_rows_by_prefix(id_prefix):
    sess = get_session()
    with sess.transaction() as tx:
        table = tx.bucket("colabfit").schema("dev").table("co_new_pbc")
        reader = table.select(predicate=table["id"].startswith(id_prefix))
        cos = reader.read_all()
    return cos


def remap_new_values(old_rows, new_rows):
    old_rows = old_rows.to_struct_array().to_pandas()
    new_rows = new_rows.to_struct_array().to_pandas()
    old_rows = pd.json_normalize(old_rows)
    new_rows = pd.json_normalize(new_rows)
    combined = old_rows.merge(new_rows, on="id", how="inner")
    combined.drop(columns=["id", "hash", "structure_hash"], inplace=True)
    combined.rename(
        columns={
            "new_id": "id",
            "new_hash": "hash",
            "new_struct_hash": "structure_hash",
        },
        inplace=True,
    )
    combined = combined.astype(int_cols)
    update = pa.Table.from_pandas(combined).select(arrow_schema.names)
    return update


def insert_new_hashes(update):
    sess = get_session()
    with sess.transaction() as tx:
        table = tx.bucket("colabfit").schema("dev").table(table_name)
        table.insert(update)


def process_prefix(prefix):
    id_prefix = f"CO_{prefix}"
    logger.info(f"Processing {id_prefix}")
    t1 = time()

    new_hashes = get_new_hash_rows_by_prefix(id_prefix)
    old_rows = get_old_rows_by_prefix(id_prefix)

    logger.info(f"Number of new rows for {id_prefix}: {new_hashes.num_rows}")
    logger.info(f"Number of old rows for {id_prefix}: {old_rows.num_rows}")
    t2 = time()
    logger.info(f"Selection time for {id_prefix}: {t2 - t1}")

    new_columns = remap_new_values(old_rows, new_hashes)
    insert_new_hashes(new_columns)
    t4 = time()
    logger.info(
        f"------>> {id_prefix} finished in: {t4 - t1} sec -- {((t4 - t2) / 60)} min"
    )


def main():

    prefixes = [
        107,
        108,
        109,
        110,
        111,
        112,
        113,
        114,
        115,
        116,
        117,
        118,
        119,
        120,
        123,
        124,
        125,
        126,
        127,
    ]
    logger.info(f"Processing prefixes: {prefixes}")
    with Pool(processes=4) as pool:
        pool.map(process_prefix, prefixes)
    logger.info("Done!")


if __name__ == "__main__":
    main()
