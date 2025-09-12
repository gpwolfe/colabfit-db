import logging
import math
import os
from ast import literal_eval
from time import time

import pyarrow as pa
import vastdb
from colabfit.tools.vast.schema import (
    config_prop_arr_schema,
)
from colabfit.tools.vast.utilities import spark_schema_to_arrow_schema

begin = time()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
from_table = "ndb.colabfit-prod.prod.co_po_merged_innerjoin".split(".")
to_table = "ndb.colabfit-prod.prod.copo_arrays_no_pyspark".split(".")


def get_vastdb_session():
    endpoint = "http://10.32.38.210"
    with open(f"/home/{os.environ['USER']}/.vast-dev/access_key_id", "r") as f:
        access_key = f.read().rstrip("\n")
    with open(f"/home/{os.environ['USER']}/.vast-dev/secret_access_key", "r") as f:
        secret_key = f.read().rstrip("\n")
    return vastdb.connect(endpoint=endpoint, access=access_key, secret=secret_key)


def str_to_arrayof_double(val):
    """Convert string representation of array to actual array of doubles"""
    if val is None:
        return None
    if isinstance(val, str) and len(val) > 0 and val[0] == "[":
        return literal_eval(val)
    return val


def str_to_arrayof_int(val):
    """Convert string representation of array to actual array of integers"""
    if val is None:
        return None
    if isinstance(val, str) and len(val) > 0 and val[0] == "[":
        return literal_eval(val)
    return val


def str_to_arrayof_str(val):
    """Convert string representation of array to actual array of strings"""
    if val is None:
        return None
    if isinstance(val, str) and len(val) > 0 and val[0] == "[":
        return literal_eval(val)
    return val


def str_to_arrayof_bool(val):
    """Convert string representation of array to actual array of booleans"""
    if val is None:
        return None
    if isinstance(val, str) and len(val) > 0 and val[0] == "[":
        return literal_eval(val)
    return val


def str_to_nestedarrayof_double(val):
    """Convert string representation of nested array to actual nested array
    of doubles"""
    if val is None:
        return None
    if isinstance(val, str) and len(val) > 0 and val[0] == "[":
        try:
            custom_globals = {"nan": math.nan}
            return eval(val, custom_globals)
        except (ValueError, SyntaxError) as e:
            logger.error(f"Failed to parse nested array: {val[:100]}... Error: {e}")
            return None
    return val


def str_to_nestedarrayof_int(val):
    """Convert string representation of nested array to actual nested array
    of integers"""
    if val is None:
        return None
    if isinstance(val, str) and len(val) > 0 and val[0] == "[":
        return literal_eval(val)
    return val


co_arrow_schema = spark_schema_to_arrow_schema(config_prop_arr_schema)


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


co_write_schema = spark_schema_to_arrow_schema(config_prop_arr_schema)
co_write_schema = co_write_schema.append(pa.field("prefix_partition", pa.string()))
co_write_schema = co_write_schema.remove(0)


co_nested_arr_cols = [
    field.name
    for field in co_arrow_schema
    if field.type == pa.list_(pa.list_(pa.float64()))
]
co_double_arr_cols = [
    field.name for field in co_arrow_schema if field.type == pa.list_(pa.float64())
]
co_str_arr_cols = [
    field.name for field in co_arrow_schema if field.type == pa.list_(pa.string())
]
co_int_arr_cols = [
    field.name
    for field in co_arrow_schema
    if (field.type == pa.list_(pa.int64()) or field.type == pa.list_(pa.int32()))
]
co_bool_arr_cols = [
    field.name for field in co_arrow_schema if field.type == pa.list_(pa.bool_())
]


def transform_table_arrays(table, col_type_map):
    """Transform string columns to array columns based on type mapping"""
    arrays = []
    names = []
    for col_name in table.column_names:
        col_array = table.column(col_name)
        names.append(col_name)
        if col_name in col_type_map.get("nested_double", []):
            pylist = col_array.to_pylist()
            transformed_list = [str_to_nestedarrayof_double(val) for val in pylist]
            nested_double_type = pa.list_(pa.list_(pa.float64()))
            arrays.append(pa.array(transformed_list, type=nested_double_type))
        elif col_name in col_type_map.get("double_array", []):
            pylist = col_array.to_pylist()
            transformed_list = [str_to_arrayof_double(val) for val in pylist]
            arrays.append(pa.array(transformed_list, type=pa.list_(pa.float64())))
        elif col_name in col_type_map.get("int_array", []):
            pylist = col_array.to_pylist()
            transformed_list = [str_to_arrayof_int(val) for val in pylist]
            arrays.append(pa.array(transformed_list, type=pa.list_(pa.int64())))
        elif col_name in col_type_map.get("str_array", []):
            pylist = col_array.to_pylist()
            transformed_list = [str_to_arrayof_str(val) for val in pylist]
            arrays.append(pa.array(transformed_list, type=pa.list_(pa.string())))
        elif col_name in col_type_map.get("bool_array", []):
            pylist = col_array.to_pylist()
            transformed_list = [str_to_arrayof_bool(val) for val in pylist]
            arrays.append(pa.array(transformed_list, type=pa.list_(pa.bool_())))
        elif col_name in col_type_map.get("nested_int", []):
            pylist = col_array.to_pylist()
            transformed_list = [str_to_nestedarrayof_int(val) for val in pylist]
            nested_int_type = pa.list_(pa.list_(pa.int64()))
            arrays.append(pa.array(transformed_list, type=nested_int_type))
        else:
            arrays.append(col_array)
    return pa.table(arrays, names=names)


def write_to_array_table():
    start = time()
    ids = list(range(200, 1000))
    if TASK_ID >= len(ids):
        raise ValueError(
            f"SLURM_TASK_ID {TASK_ID} exceeds number of prefixes {len(ids)}"
        )
    prefix = f"PO_{ids[TASK_ID]:03d}"
    logger.info(f"Processing prefix {prefix} (task ID {TASK_ID})")
    session = get_vastdb_session()

    co_type_map = {
        "nested_double": co_nested_arr_cols,
        "double_array": co_double_arr_cols,
        "int_array": co_int_arr_cols,
        "str_array": co_str_arr_cols,
        "bool_array": co_bool_arr_cols,
    }
    batch_count = 0
    with session.transaction() as tx:
        co_table = tx.bucket(from_table[1]).schema(from_table[2]).table(from_table[3])
        logger.info(f"Querying co_po_merged_innerjoin for prefix: {prefix}")
        co_data = co_table.select(
            predicate=co_table["property_id"].startswith(prefix),
        )
        write_rows = 0
        write_tables = []
        try:
            managed_batches = batch_manager(co_data, target_batch_size=50_000)
            for i, co_batch in enumerate(managed_batches):
                batch_count += 1
                batch_rows = co_batch.num_rows
                write_rows += batch_rows
                logger.info(f"Read CO batch {i}: {batch_rows} rows")
                if batch_rows == 0:
                    logger.warning(f"CO batch {i} is empty, skipping")
                    continue
                co_data_transformed = transform_table_arrays(co_batch, co_type_map)
                logger.info(
                    f"Transformed CO batch {i}: {co_data_transformed.num_rows} rows"
                )
                write_tables.append(co_data_transformed)
            if not write_tables:
                raise ValueError(f"No data found for prefix {prefix}")
            write_table = pa.concat_tables(write_tables)
            write_table = write_table.append_column(
                pa.field("prefix_partition", pa.string()),
                pa.array([prefix] * write_rows),
            )
            write_table = write_table.select(co_write_schema.names)
            write_table = write_table.cast(co_write_schema)
            print(write_table.schema)

        except Exception as e:
            logger.error(f"Error processing CO data for dataset {prefix}: {str(e)}")
            raise
    with session.transaction() as tx:
        # vast_schema = tx.bucket(to_table[1]).schema(to_table[2])
        # vast_schema.create_table(to_table[3], columns=write_table.schema)
        new_co_table = tx.bucket(to_table[1]).schema(to_table[2]).table(to_table[3])
        new_co_table.insert(write_table)
    logger.info(f"Wrote {write_table.num_rows} rows to {to_table[3]}")
    logger.info(
        f"CO processing complete: {batch_count} batches, {write_table.num_rows} total rows"
    )
    logger.info(f"finished in {time() - start} seconds")


if __name__ == "__main__":
    write_to_array_table()
