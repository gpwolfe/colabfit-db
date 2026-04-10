import logging
import os
from time import time, sleep

from colabfit.tools.vast.schema import property_object_schema
from colabfit.tools.vast.utilities import spark_schema_to_arrow_schema
from dotenv import load_dotenv
from tqdm import tqdm
from vastdb.session import Session

load_dotenv()
# endpoint = os.getenv("VAST_DB_ENDPOINT")
# access = os.getenv("VAST_DB_ACCESS")
# secret = os.getenv("VAST_DB_SECRET")
# session = Session(access=access, secret=secret, endpoint=endpoint)

logger = logging.getLogger(f"{__name__}.hasher")
logger.setLevel("INFO")


def get_session():
    load_dotenv()
    access = os.getenv("VAST_DB_ACCESS")
    secret = os.getenv("VAST_DB_SECRET")
    endpoint = os.getenv("VAST_DB_ENDPOINT")
    return Session(access=access, secret=secret, endpoint=endpoint)


def main():
    session = get_session()
    begin = time()
    SRC = "colabfit.dev.po_wip"
    DEST = "colabfit-prod.prod.po_tmp"
    logger.info(f"Copying {SRC} to {DEST}")
    start = time()
    arrow_schema = spark_schema_to_arrow_schema(property_object_schema)
    dest_path = DEST.split(".")
    po_prefixes = list(range(1000, 1350))
    po_prefixes.extend(list(range(135, 1000)))
    po_prefixes = [f"PO_{prefix}" for prefix in po_prefixes]
    src_path = SRC.split(".")
    with session.transaction() as tx:
        logger.info(f"Creating table {DEST}")
        dest_schema = tx.bucket(dest_path[0]).schema(dest_path[1])
        dest_schema.create_table(dest_path[2], arrow_schema)
    for prefix in po_prefixes:
        logger.info(f"Processing {prefix}")
        with session.transaction() as tx:
            dest_table = (
                tx.bucket(dest_path[0]).schema(dest_path[1]).table(dest_path[2])
            )
            src_table = tx.bucket(src_path[0]).schema(src_path[1]).table(src_path[2])
            src_reader = src_table.select(predicate=src_table["id"].startswith(prefix))
            # for batch in src_reader:
            #     logger.info(f"Processing batch with {batch.num_rows} rows")
            # p_table = src_reader.read_all()
            # dest_table.insert(ptable)
            # Copy
            nrows = 0
            num_batches = 0
            for batch in tqdm(src_reader):
                nrows += batch.num_rows
                dest_table.insert(batch)
                num_batches += 1
                if num_batches % 50 == 0:
                    logger.info(f"Inserted {nrows} total rows")
    logger.info("Copy complete!")
    logger.info(f"Copy runtime: {time() - start}")
    logger.info(f"Copied {SRC} to {DEST}")
    # logger.info(f"Total batches: {num_batches}")
    with session.transaction() as tx:
        dest_table = tx.bucket(dest_path[0]).schema(dest_path[1]).table(dest_path[2])
        # Projections
        logger.info("Creating projection")
        sorted_columns = ["dataset_id"]
        unsorted_columns = ["id"]
        dest_table.create_projection(
            projection_name="po-dataset_id",
            sorted_columns=sorted_columns,
            unsorted_columns=unsorted_columns,
        )
        sorted_columns = ["id"]
        unsorted_columns = [
            col
            for col in property_object_schema.fieldNames()
            if col not in sorted_columns
        ]
        dest_table.create_projection(
            projection_name="po-id-all",
            sorted_columns=sorted_columns,
            unsorted_columns=unsorted_columns,
        )
        sorted_columns = ["configuration_id"]
        unsorted_columns = ["id"]
        dest_table.create_projection(
            projection_name="po-configuration_id",
            sorted_columns=sorted_columns,
            unsorted_columns=unsorted_columns,
        )
    print(dest_table.projections())
    print(f"Time elapsed: {time() - begin:.2f} seconds")
    logger.info("Done!")


if __name__ == "__main__":
    main()
