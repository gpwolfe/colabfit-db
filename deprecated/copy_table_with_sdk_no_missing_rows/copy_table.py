import logging
import os
from time import time

import pyarrow as pa
import vastdb
from colabfit.tools.vast.schema import (
    config_prop_str_schema,
)
from colabfit.tools.vast.utils import spark_schema_to_arrow_schema

begin = time()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
from_table = "".split(".")
to_table = "".split(".")

query_config = vastdb.config.QueryConfig(use_semi_sorted_projections=False)


def get_vastdb_session():
    endpoint = ""
    with open(f"/home/{os.environ['USER']}/.vast-dev/access_key_id", "r") as f:
        access_key = f.read().rstrip("\n")
    with open(f"/home/{os.environ['USER']}/.vast-dev/secret_access_key", "r") as f:
        secret_key = f.read().rstrip("\n")
    return vastdb.connect(endpoint=endpoint, access=access_key, secret=secret_key)


def batch_manager(data_iterator, target_batch_size=10_000):
    leftover_table = None
    batch_num = 0
    for raw_batch in data_iterator:
        if raw_batch.num_rows == 0:
            logger.info("Skipping empty raw batch")
            continue
        raw_table = pa.Table.from_batches([raw_batch])
        if leftover_table is not None:
            combined_table = pa.concat_tables([leftover_table, raw_table])
            leftover_table = None
        else:
            combined_table = raw_table
        current_offset = 0
        while current_offset + target_batch_size <= combined_table.num_rows:
            batch_to_yield = combined_table.slice(current_offset, target_batch_size)
            logger.info(
                f"Yielding batch {batch_num} with " f"{batch_to_yield.num_rows} rows"
            )
            yield batch_to_yield
            batch_num += 1
            current_offset += target_batch_size
        remaining_rows = combined_table.num_rows - current_offset
        if remaining_rows > 0:
            leftover_table = combined_table.slice(current_offset, remaining_rows)
    if leftover_table is not None and leftover_table.num_rows > 0:
        logger.info(
            f"Yielding final leftover batch {batch_num} with "
            f"{leftover_table.num_rows} rows"
        )
        yield leftover_table


co_write_schema = spark_schema_to_arrow_schema(config_prop_str_schema)
co_write_schema = co_write_schema.append(pa.field("prefix_partition", pa.string()))


def write_to_array_table():
    start = time()
    ids = list(range(135, 1000))
    if TASK_ID >= len(ids):
        raise ValueError(
            f"SLURM_TASK_ID {TASK_ID} exceeds number of prefixes {len(ids)}"
        )
    prefix = f"PO_{ids[TASK_ID]:03d}"
    logger.info(f"Processing prefix {prefix} (task ID {TASK_ID})")
    session = get_vastdb_session()
    batch_count = 0
    with session.transaction() as tx:
        co_table = tx.bucket(from_table[1]).schema(from_table[2]).table(from_table[3])
        logger.info(f"Querying co_po_merged_innerjoin for prefix: {prefix}")
        co_data = co_table.select(
            predicate=co_table["property_id"].startswith(prefix),
            config=query_config,
        )
        write_rows = 0
        # write_tables = []
        # try:
        # managed_batches = batch_manager(co_data, target_batch_size=50_000)
        # for i, co_batch in enumerate(managed_batches):
        # for i, co_batch in enumerate(co_data):
        co_batch = co_data.read_all()
        batch_count += 1
        batch_rows = co_batch.num_rows
        write_rows += batch_rows
        logger.info(f"Read CO batch {batch_count}: {batch_rows} rows")
        if batch_rows == 0:
            logger.warning(f"CO batch {batch_count} is empty, exiting")
            return
        write_table = co_batch
        assert write_table.num_rows == write_rows
        write_table = write_table.append_column(
            pa.field("prefix_partition", pa.string()),
            pa.array([prefix] * write_rows),
        )
        write_table = write_table.select(co_write_schema.names)
        print(write_table.schema)

    with session.transaction() as tx:
        vast_schema = tx.bucket(to_table[1]).schema(to_table[2])
        try:
            vast_schema.create_table(
                to_table[3], columns=write_table.schema, fail_if_exists=False
            )
        except Exception as e:
            logger.info(f"Table {to_table[3]} exists?: {e}")
        new_co_table = tx.bucket(to_table[1]).schema(to_table[2]).table(to_table[3])
        new_co_table.insert(write_table)
    logger.info(f"Wrote {write_table.num_rows} rows to {to_table[3]}")
    logger.info(
        f"CO processing complete: {batch_count} batches, "
        f"{write_table.num_rows} total rows"
    )
    logger.info(f"finished in {time() - start} seconds")


if __name__ == "__main__":
    write_to_array_table()
