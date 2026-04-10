import logging
import os
from multiprocessing import Pool
from time import time

import pyarrow as pa
from colabfit.tools.vast.schema import property_object_schema
from colabfit.tools.vast.utilities import spark_schema_to_arrow_schema
from dotenv import load_dotenv
from vastdb.session import Session

load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
session = Session(access=access, secret=secret, endpoint=endpoint)

logger = logging.getLogger(f"{__name__}.hasher")
logger.info("copy_to_prod_pos_mp.py")
SRC = "colabfit.dev.po_wip"
DEST = "colabfit-prod.prod.po_tmp"
arrow_schema = spark_schema_to_arrow_schema(property_object_schema)
arrow_inter_schema = pa.schema(
    [
        (
            field
            if field.name != "last_modified"
            else pa.field("last_modified", pa.timestamp("ns"))
        )
        for field in arrow_schema
    ],
)


def convert_table_to_timestamp_us(table):
    """
    Convert the timestamp column in the table to microseconds.
    """
    # Assuming the timestamp column is named 'last_modified'
    # and is of type pa.timestamp('ns')
    # You can adjust the column name as per your schema
    if "last_modified" not in table.schema.names:
        raise ValueError("Column 'last_modified' not found in the table schema.")

    # Convert the timestamp column to microseconds
    table.set_column(
        table.schema.get_field_index("last_modified"),
        "last_modified",
        table.column("last_modified").cast(pa.timestamp("ns")),
    )
    return table


def get_session():
    load_dotenv()
    access = os.getenv("VAST_DB_ACCESS")
    secret = os.getenv("VAST_DB_SECRET")
    endpoint = os.getenv("VAST_DB_ENDPOINT")
    return Session(access=access, secret=secret, endpoint=endpoint)


def get_pos(prefix):
    logger.info(f"Processing {prefix}")
    session = get_session()
    b1, s1, t1 = SRC.split(".")
    b2, s2, t2 = DEST.split(".")
    with session.transaction() as tx:
        table = tx.bucket(b1).schema(s1).table(t1)
        table2 = tx.bucket(b2).schema(s2).table(t2)
        reader = table.select(
            predicate=table["id"].startswith(prefix),
        )
        # batch_rows = 0
        # table_batch = []
        # for batch in reader:
        #     batch_rows += batch.num_rows
        #     table_batch.append(batch)
        #     if batch_rows >= 25000:
        #         pos = pa.Table.from_batches(table_batch, schema=arrow_inter_schema)
        #         pos = convert_table_to_timestamp_us(pos)
        #         table2.insert(pos)
        #         logger.info(f"{prefix}: Inserted {batch_rows} rows")
        #         batch_rows = 0
        #         table_batch = []
        # if batch_rows > 0:
        table_batch = reader.read_all()
        batch_rows = table_batch.num_rows
        # pos = pa.Table.from_batches(table_batch, schema=arrow_inter_schema)
        # pos = convert_table_to_timestamp_us(table_batch)
        table2.insert(table_batch)
        logger.info(f"Inserted {batch_rows} rows to finish {prefix}")
        logger.info(f"Finished {prefix}")


def create_table():
    session = get_session()
    b2, s2, t2 = DEST.split(".")
    with session.transaction() as tx:
        logger.info(f"Creating table {DEST}")
        dest_schema = tx.bucket(b2).schema(s2)
        dest_schema.create_table(t2, arrow_schema)
        logger.info(f"Created table {DEST}")
        logger.info(f"Table {DEST} created")


def create_projections():
    session = get_session()
    b2, s2, t2 = DEST.split(".")
    with session.transaction() as tx:
        logger.info(f"Creating table {DEST}")
        table = tx.bucket(b2).schema(s2).table(t2)
        sorted_columns = ["id"]
        unsorted_columns = [
            col
            for col in property_object_schema.fieldNames()
            if col not in sorted_columns
        ]
        table.create_projection(
            projection_name="co-id-all",
            sorted_columns=sorted_columns,
            unsorted_columns=unsorted_columns,
        )
        logger.info(table.projections())


if __name__ == "__main__":
    logger.info(f"Copying {SRC} to {DEST}")
    start = time()
    p = Pool(12)
    co_prefixes = list(range(1000, 1350))
    co_prefixes.extend(list(range(135, 1000)))
    co_prefixes = [f"PO_{prefix}" for prefix in co_prefixes]
    create_table()
    p.map(get_pos, co_prefixes)
    logger.info("Copy complete!")
    logger.info(f"Copy runtime: {time() - start}")
    logger.info(f"Copied {SRC} to {DEST}")
    logger.info("Creating projection")
    create_projections()
    logger.info("Done!")
