import logging
import os
import sys
from multiprocessing import Pool
from time import time

import pandas as pd
import pyarrow as pa
from colabfit.tools.vast.schema import property_object_schema
from colabfit.tools.vast.utilities import spark_schema_to_arrow_schema
from dotenv import load_dotenv
from vastdb.session import Session

load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
sess = Session(access=access, secret=secret, endpoint=endpoint)


table_name = "po_remapped_co_ids"

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(f"{__name__}.hasher")
logger.setLevel("INFO")
logger.addHandler(handler)
logger.info(f"table_name: {table_name}")


arrow_schema = spark_schema_to_arrow_schema(property_object_schema)
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
    select_cols = ["id", "new_id"]
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
        table = tx.bucket("colabfit").schema("dev").table("po_wip")
        reader = table.select(predicate=table["configuration_id"].startswith(id_prefix))
        cos = reader.read_all()
    return cos


def remap_new_values(old_rows, new_rows):
    old_rows = old_rows.to_struct_array().to_pandas()
    new_rows = new_rows.to_struct_array().to_pandas()
    old_rows = pd.json_normalize(old_rows)
    new_rows = pd.json_normalize(new_rows)
    new_rows.rename(columns={"id": "configuration_id"}, inplace=True)
    combined = old_rows.merge(new_rows, on="configuration_id", how="inner")
    combined.drop(columns=["configuration_id"], inplace=True)
    combined.rename(
        columns={
            "new_id": "configuration_id",
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
        f"------>> {id_prefix} finished in: {t4 - t1} sec -- {((t4 - t1) / 60)} min"
    )


def main(start):
    end = start + 100
    logger.info(f"Processing prefixes from {start} to {end}")
    prefixes = range(start, end)
    with Pool(processes=12) as pool:
        pool.map(process_prefix, prefixes)
    logger.info("Done!")


if __name__ == "__main__":
    start = int(sys.argv[1])
    main(start)
