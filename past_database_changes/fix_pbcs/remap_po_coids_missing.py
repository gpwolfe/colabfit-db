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

missing_ids = [
    292,
    293,
    294,
    295,
    296,
    297,
    298,
    299,
    369,
    370,
    372,
    373,
    375,
    376,
    378,
    379,
    381,
    382,
    384,
    385,
    387,
    388,
    390,
    391,
    392,
    393,
    394,
    395,
    396,
    397,
    398,
    399,
    469,
    470,
    472,
    473,
    475,
    476,
    478,
    479,
    481,
    482,
    484,
    485,
    487,
    488,
    490,
    491,
    492,
    493,
    494,
    495,
    496,
    497,
    498,
    499,
    569,
    570,
    572,
    573,
    575,
    576,
    578,
    579,
    581,
    582,
    584,
    585,
    587,
    588,
    590,
    591,
    592,
    593,
    594,
    595,
    596,
    597,
    598,
    599,
    669,
    670,
    672,
    673,
    675,
    676,
    678,
    679,
    681,
    682,
    684,
    685,
    687,
    688,
    689,
    690,
    691,
    692,
    693,
    694,
    695,
    696,
    697,
    698,
    699,
    769,
    770,
    772,
    773,
    775,
    776,
    778,
    779,
    781,
    782,
    784,
    785,
    787,
    788,
    790,
    791,
    792,
    793,
    794,
    795,
    796,
    797,
    798,
    799,
    869,
    870,
    872,
    873,
    875,
    876,
    878,
    879,
    881,
    882,
    884,
    885,
    887,
    888,
    890,
    891,
    892,
    893,
    894,
    895,
    896,
    897,
    898,
    899,
    969,
    970,
    972,
    973,
    975,
    976,
    978,
    979,
    981,
    982,
    984,
    985,
    987,
    988,
    990,
    991,
    992,
    993,
    994,
    995,
    996,
    997,
    998,
    999,
]

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
    end = start + 20
    if start > len(missing_ids):
        logger.error("Start index is out of range.")
        sys.exit(1)
    if end > len(missing_ids):
        end = len(missing_ids)
    logger.info(f"Processing missing prefixes from index {start} to {end}")
    prefixes = missing_ids[start:end]
    logger.info(f"Processing prefixes {prefixes}")
    with Pool(processes=5) as pool:
        pool.map(process_prefix, prefixes)
    logger.info("Done!")


if __name__ == "__main__":
    start = int(sys.argv[1])
    main(start)
