import logging
import os
import sys
from time import time

import numpy as np
import pandas as pd
import pyarrow as pa
from colabfit.tools.vast.schema import config_schema
from colabfit.tools.vast.utilities import (
    get_pbc_from_cell,
    spark_schema_to_arrow_schema,
)
from dotenv import load_dotenv

# from tqdm import tqdm
from vastdb.session import Session
from multiprocessing import Pool

missing_prefixes = [
    121,
    599,
    600,
    601,
    602,
    603,
    604,
    605,
    606,
    607,
    608,
    609,
    610,
    611,
    612,
    613,
    614,
    615,
    616,
    617,
    618,
    619,
    620,
    621,
    622,
    627,
    628,
    629,
    630,
    631,
    632,
    633,
    634,
    635,
    636,
    637,
    638,
    639,
    640,
    641,
    642,
    643,
    644,
    645,
    646,
    647,
    699,
    722,
    723,
    724,
    725,
    726,
    727,
]
load_dotenv()
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")
logger.addHandler(handler)


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


def get_rows_by_prefix(id_prefix):
    sess = get_session()
    with sess.transaction() as tx:
        table = tx.bucket("colabfit").schema("dev").table("co_wip")
        reader = table.select(predicate=table["id"].startswith(id_prefix))
        cos = reader.read_all()
    return cos


def process_rows(cos):
    co_pandas = cos.to_struct_array().to_pandas()
    for x in co_pandas:
        if x["metadata_size"] is not None:
            x["metadata_size"] = np.int32(x["metadata_size"])
        else:
            x["metadata_size"] = 0
    co_pandas = pd.json_normalize(co_pandas)
    cells = co_pandas["cell"].str.replace("[", "").str.replace("]", "").str.split(",")

    cells = np.array([np.array(x, dtype="float").reshape(3, 3) for x in cells])
    pbcs = [str([any(get_pbc_from_cell(x))] * 3) for x in cells]
    co_pandas["pbc"] = pbcs
    co_pandas = co_pandas.astype(int_cols)
    update = pa.Table.from_pandas(co_pandas).select(arrow_schema.names)
    return update


def insert_rows(update):
    sess = get_session()
    with sess.transaction() as tx:
        table = tx.bucket("colabfit").schema("dev").table("co_new_pbc")
        table.insert(update)


def process_and_insert(id_prefix):
    id_prefix = f"CO_{id_prefix}"
    logger.info(f"Processing {id_prefix}")
    t1 = time()
    cos = get_rows_by_prefix(id_prefix)
    t2 = time()
    logger.info(f"Selection time {t2 - t1}")

    update = process_rows(cos)
    t3 = time()
    logger.info(f"Processing time {t3 - t1}")

    insert_rows(update)
    t4 = time()
    logger.info(f"Insert time {t4 - t3}")
    logger.info(
        f"------>> {id_prefix} finished in: {t4 - t1} sec -- {((t4 - t1) / 60)} min"
    )


def main():
    with Pool(processes=4) as p:
        p.map(process_and_insert, missing_prefixes)
    logger.info("Done!")


if __name__ == "__main__":
    main()
