import logging
import os
import sys
from multiprocessing import Pool
from time import time
import dateutil.parser
import datetime
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


handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(f"{__name__}.hasher")
logger.setLevel("INFO")
logger.addHandler(handler)


old_row_table = "po_remapped_co_ids"
new_id_table = "po_rehashed"
finished_table = "po_with_new_hashes"
logger.info(f"new table name: {finished_table}")

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
    select_cols = ["id", "new_id", "new_hash"]
    sess = get_session()
    with sess.transaction() as tx:
        table = tx.bucket("colabfit").schema("dev").table(new_id_table)
        reader = table.select(
            predicate=table["id"].startswith(id_prefix), columns=select_cols
        )
        pos = reader.read_all()
    return pos


def get_old_rows_by_prefix(id_prefix):
    sess = get_session()
    with sess.transaction() as tx:
        table = tx.bucket("colabfit").schema("dev").table(old_row_table)
        reader = table.select(predicate=table["id"].startswith(id_prefix))
        pos = reader.read_all()
    return pos


def remap_new_values(old_rows, new_rows):
    old_rows = old_rows.to_struct_array().to_pandas()
    new_rows = new_rows.to_struct_array().to_pandas()
    old_rows = pd.json_normalize(old_rows)
    new_rows = pd.json_normalize(new_rows)
    combined = old_rows.merge(new_rows, on="id", how="inner")
    combined.drop(columns=["id", "hash"], inplace=True)
    combined.rename(
        columns={
            "new_id": "id",
            "new_hash": "hash",
        },
        inplace=True,
    )
    combined = combined.astype(int_cols)
    combined["last_modified"] = dateutil.parser.parse(
        datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    update = pa.Table.from_pandas(combined).select(arrow_schema.names)
    return update


def insert_new_hashes(update):
    sess = get_session()
    with sess.transaction() as tx:
        table = tx.bucket("colabfit").schema("dev").table(finished_table)
        table.insert(update)


def process_prefix(prefix):
    id_prefix = f"PO_{prefix}"
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


missing = [100, 101, 112, 113, 122, 123, 130, 131]


def main():
    # end = start + 10

    prefixes = missing
    logger.info(f"Processing prefixes {missing}")
    with Pool(processes=1) as pool:
        pool.map(process_prefix, prefixes)
    logger.info("Done!")


if __name__ == "__main__":
    # start = int(sys.argv[1])
    main()
