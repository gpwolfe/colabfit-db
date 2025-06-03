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
from tqdm import tqdm
from vastdb.session import Session


def main():

    load_dotenv()
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger = logging.getLogger(__name__)
    logger.setLevel("INFO")
    logger.addHandler(handler)
    endpoint = os.getenv("VAST_DB_ENDPOINT")
    access = os.getenv("VAST_DB_ACCESS")
    secret = os.getenv("VAST_DB_SECRET")
    sess = Session(access=access, secret=secret, endpoint=endpoint)
    # ACTUAL_INDEX = int(os.getenv("ACTUAL_INDEX"))
    # logger.info(f"ACTUAL_INDEX: {ACTUAL_INDEX}")
    arrow_schema = spark_schema_to_arrow_schema(config_schema)
    int_cols = {
        col: "int32"
        for col in arrow_schema.names
        if pa.types.is_integer(arrow_schema.field(col).type)
    }
    start = 900
    # co_prefixes = list(range(start, 10000))
    co_prefixes = list(range(start, 1000))

    for prefix in tqdm(co_prefixes, total=len(co_prefixes)):
        # logger.info(f"task id: {ACTUAL_INDEX}")
        # prefix = co_prefixes[ACTUAL_INDEX]
        id_prefix = f"CO_{prefix}"
        logger.info(f"Processing {id_prefix}")
        t1 = time()
        with sess.transaction() as tx:
            table = tx.bucket("colabfit").schema("dev").table("co_wip")
            reader = table.select(predicate=table["id"].startswith(id_prefix))
            cos = reader.read_all()
            nrows = len(cos)
            if nrows == 0:
                logger.info(f"No rows found for prefix {id_prefix}")
                exit(0)
            logger.info(f"Number of rows: {nrows}")
        logger.info(f"Selection time {time() - t1}")
        t1 = time()
        cos1 = cos.to_struct_array().to_pandas()
        for x in cos1:
            if x["metadata_size"] is not None:
                x["metadata_size"] = np.int32(x["metadata_size"])
            else:
                x["metadata_size"] = 0
        co_pandas = pd.json_normalize(cos1)
        cells = (
            co_pandas["cell"].str.replace("[", "").str.replace("]", "").str.split(",")
        )

        cells = np.array([np.array(x, dtype="float").reshape(3, 3) for x in cells])
        pbcs = [str(get_pbc_from_cell(x)) for x in cells]
        co_pandas["pbc"] = pbcs
        co_pandas = co_pandas.astype(int_cols)
        # logger.info(co_pandas[co_pandas['pbc'] == '[False, False, False]'])
        update = pa.Table.from_pandas(co_pandas).select(arrow_schema.names)
        logger.info(f"update table length: {len(update)}")
        assert len(update) == nrows
        logger.info(f"Create table time: {time() - t1}")

        t1 = time()
        with sess.transaction() as tx:
            table = tx.bucket("colabfit").schema("dev").table("co_new_pbc")
            table.insert(update)
        logger.info(f"Insert time for {id_prefix}: {time() - t1}")
    logger.info("Done!")


if __name__ == "__main__":
    main()
