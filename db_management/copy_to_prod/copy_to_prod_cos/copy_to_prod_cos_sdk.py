import logging
import os
import sys
from time import time

import pyarrow as pa
from colabfit.tools.vast.schema import config_schema
from colabfit.tools.vast.utilities import spark_schema_to_arrow_schema
from dotenv import load_dotenv
from tqdm import tqdm
from vastdb.session import Session

load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
session = Session(access=access, secret=secret, endpoint=endpoint)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(f"{__name__}.hasher")
logger.setLevel("INFO")
logger.addHandler(handler)


def get_session():
    load_dotenv()
    access = os.getenv("VAST_DB_ACCESS")
    secret = os.getenv("VAST_DB_SECRET")
    endpoint = os.getenv("VAST_DB_ENDPOINT")
    return Session(access=access, secret=secret, endpoint=endpoint)


def get_cos(prefix):
    session = get_session()
    with session.transaction() as tx:
        table = tx.bucket("colabfit").schema("dev").table("co_wip")
        reader = table.select(
            predicate=table["id"].startswith(prefix),
        )
        batch_rows = 0
        table_batch = []
        for batch in reader:
            batch_rows += batch.num_rows
            table_batch.append(batch)
            if batch_rows >= 10000:
                pos = pa.Table.from_batches(table_batch)
                yield pos
                batch_rows = 0
                table_batch = []
        if batch_rows > 0:
            pos = pa.Table.from_batches(table_batch)
            yield pos


if __name__ == "__main__":

    SRC = "colabfit.dev.co_wip"
    DEST = "colabfit-prod.prod.co_tmp"
    logger.info(f"Copying {SRC} to {DEST}")
    start = time()
    arrow_schema = spark_schema_to_arrow_schema(config_schema)
    dest_path = DEST.split(".")
    src_path = SRC.split(".")
    with session.transaction() as tx:
        logger.info(f"Creating table {DEST}")
        dest_schema = tx.bucket("colabfit-prod").schema("prod")
        dest_schema.create_table("co_tmp", arrow_schema)
    for i in range(100, 1000):
        with session.transaction() as tx:
            dest_table = (
                tx.bucket(dest_path[0]).schema(dest_path[1]).table(dest_path[2])
            )
            src_table = tx.bucket(src_path[0]).schema(src_path[1]).table(src_path[2])
            # src_reader = src_table.select()
            # Copy
            nrows = 0
            num_batches = 0
            src_reader = src_table.select(
                predicate=src_table["id"].startswith(f"CO_{i}"),
            )
            # r_table = r.read_all()
            # dest_table.insert(r_table)
            for batch in tqdm(iter(src_reader.read_next_batch, None)):
                nrows += batch.num_rows
                dest_table.insert(batch)
                num_batches += 1
                if num_batches % 20 == 0:
                    logger.info(f"Inserted {nrows} total rows")
            logger.info(f"Copy {i} complete!")
            logger.info(f"Copy {i} runtime: {time() - start}")
            logger.info(f"Copied {i} from {SRC} to {DEST}")
            logger.info(f"Total batches: {num_batches}")

    with session.transaction() as tx:
        logger.info("Creating projection")
        sorted_columns = ["id"]
        unsorted_columns = [
            col for col in config_schema.fieldNames() if col not in sorted_columns
        ]
        dest_table = tx.bucket(dest_path[0]).schema(dest_path[1]).table(dest_path[2])
        dest_table.create_projection(
            projection_name="co-id-all",
            sorted_columns=sorted_columns,
            unsorted_columns=unsorted_columns,
        )
        logger.info(dest_table.projections())
        logger.info("Done!")
