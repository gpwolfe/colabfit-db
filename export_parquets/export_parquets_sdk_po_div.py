from __future__ import print_function

import logging
import os
import sys
from ast import literal_eval
from pathlib import Path
from time import time

import pyarrow as pa
import vastdb
from vastdb.config import QueryConfig
from colabfit.tools.vast.schema import (
    config_arr_schema,
    configuration_set_arr_schema,
    dataset_arr_schema,
    property_object_arr_schema,
)
from colabfit.tools.vast.utilities import spark_schema_to_arrow_schema

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

config = QueryConfig(
    limit_rows_per_sub_split=100_000,
    rows_per_split=1_000_000,
    num_sub_splits=10,
)


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
        return literal_eval(val)
    return val


def str_to_nestedarrayof_int(val):
    """Convert string representation of nested array to actual nested array
    of integers"""
    if val is None:
        return None
    if isinstance(val, str) and len(val) > 0 and val[0] == "[":
        return literal_eval(val)
    return val


# Define column type mappings based on schemas
# Convert PySpark schemas to PyArrow schemas
co_arrow_schema = spark_schema_to_arrow_schema(config_arr_schema)
po_arrow_schema = spark_schema_to_arrow_schema(property_object_arr_schema)
ds_arrow_schema = spark_schema_to_arrow_schema(dataset_arr_schema)
cs_arrow_schema = spark_schema_to_arrow_schema(configuration_set_arr_schema)

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
    field.name for field in co_arrow_schema if field.type == pa.list_(pa.int64())
]
co_bool_arr_cols = [
    field.name for field in co_arrow_schema if field.type == pa.list_(pa.bool_())
]

po_nested_arr_cols = [
    field.name
    for field in po_arrow_schema
    if field.type == pa.list_(pa.list_(pa.float64()))
]
po_double_arr_cols = [
    field.name for field in po_arrow_schema if field.type == pa.list_(pa.float64())
]
po_str_arr_cols = [
    field.name for field in po_arrow_schema if field.type == pa.list_(pa.string())
]
po_int_arr_cols = [
    field.name for field in po_arrow_schema if field.type == pa.list_(pa.int64())
]
po_bool_arr_cols = [
    field.name for field in po_arrow_schema if field.type == pa.list_(pa.bool_())
]

ds_nested_arr_cols = [
    field.name
    for field in ds_arrow_schema
    if field.type == pa.list_(pa.list_(pa.int64()))
]
ds_double_arr_cols = [
    field.name for field in ds_arrow_schema if field.type == pa.list_(pa.float64())
]
ds_str_arr_cols = [
    field.name for field in ds_arrow_schema if field.type == pa.list_(pa.string())
]
ds_int_arr_cols = [
    field.name for field in ds_arrow_schema if field.type == pa.list_(pa.int64())
]

cs_nested_arr_cols = [
    field.name
    for field in cs_arrow_schema
    if field.type == pa.list_(pa.list_(pa.int64()))
]
cs_double_arr_cols = [
    field.name for field in cs_arrow_schema if field.type == pa.list_(pa.float64())
]
cs_str_arr_cols = [
    field.name for field in cs_arrow_schema if field.type == pa.list_(pa.string())
]
cs_int_arr_cols = [
    field.name for field in cs_arrow_schema if field.type == pa.list_(pa.int64())
]


def transform_table_arrays(table, col_type_map):
    """Transform string columns to array columns based on type mapping"""
    arrays = []
    names = []

    for col_name in table.column_names:
        col_array = table.column(col_name)
        names.append(col_name)

        if col_name in col_type_map.get("nested_double", []):
            # Transform nested double arrays
            pylist = col_array.to_pylist()
            transformed_list = [str_to_nestedarrayof_double(val) for val in pylist]
            nested_double_type = pa.list_(pa.list_(pa.float64()))
            arrays.append(pa.array(transformed_list, type=nested_double_type))
        elif col_name in col_type_map.get("double_array", []):
            # Transform double arrays
            pylist = col_array.to_pylist()
            transformed_list = [str_to_arrayof_double(val) for val in pylist]
            arrays.append(pa.array(transformed_list, type=pa.list_(pa.float64())))
        elif col_name in col_type_map.get("int_array", []):
            # Transform int arrays
            pylist = col_array.to_pylist()
            transformed_list = [str_to_arrayof_int(val) for val in pylist]
            arrays.append(pa.array(transformed_list, type=pa.list_(pa.int64())))
        elif col_name in col_type_map.get("str_array", []):
            # Transform string arrays
            pylist = col_array.to_pylist()
            transformed_list = [str_to_arrayof_str(val) for val in pylist]
            arrays.append(pa.array(transformed_list, type=pa.list_(pa.string())))
        elif col_name in col_type_map.get("bool_array", []):
            # Transform bool arrays
            pylist = col_array.to_pylist()
            transformed_list = [str_to_arrayof_bool(val) for val in pylist]
            arrays.append(pa.array(transformed_list, type=pa.list_(pa.bool_())))
        elif col_name in col_type_map.get("nested_int", []):
            # Transform nested int arrays
            pylist = col_array.to_pylist()
            transformed_list = [str_to_nestedarrayof_int(val) for val in pylist]
            nested_int_type = pa.list_(pa.list_(pa.int64()))
            arrays.append(pa.array(transformed_list, type=nested_int_type))
        else:
            # Keep column as-is
            arrays.append(col_array)

    return pa.table(arrays, names=names)


def get_vastdb_session():
    endpoint = "http://10.32.38.210"
    with open(f"/home/{os.environ['USER']}/.vast-dev/access_key_id", "r") as f:
        access_key = f.read().rstrip("\n")
    with open(f"/home/{os.environ['USER']}/.vast-dev/secret_access_key", "r") as f:
        secret_key = f.read().rstrip("\n")
    return vastdb.connect(endpoint=endpoint, access=access_key, secret=secret_key)


def export_dataset_parquets(dataset_id, output_dir):
    """
    Export dataset parquet files using VastDB SDK

    Args:
        dataset_id: The dataset ID to export
        output_dir: Directory to save the parquet files
    """
    start = time()
    logger.info(f"Starting export for dataset: {dataset_id}")

    # Create output directory
    dataset_dir = Path(output_dir) / dataset_id
    if dataset_dir.exists():
        logger.info(f"Dataset {dataset_id} pos already exported, skipping")
        return
    dataset_dir.mkdir(parents=True, exist_ok=True)

    session = get_vastdb_session()
    # po_batch_size = 100000
    # Get property objects (PO) for the dataset
    po_type_map = {
        "nested_double": po_nested_arr_cols,
        "double_array": po_double_arr_cols,
        "int_array": po_int_arr_cols,
        "str_array": po_str_arr_cols,
        "bool_array": po_bool_arr_cols,
    }
    po_output_path = dataset_dir / "po"
    po_output_path.mkdir(parents=True, exist_ok=True)
    with session.transaction() as tx:
        po_table = tx.bucket("colabfit-prod").schema("prod").table("po")
        po_data = po_table.select(
            predicate=po_table["dataset_id"] == dataset_id, config=config
        )
        # po_data = po_data.read_all()
        for i, po_batch in enumerate(po_data):
            logger.info(f"Read PO rows: {po_batch.num_rows}")
            po_data_transformed = transform_table_arrays(po_batch, po_type_map)
            pa.parquet.write_table(
                po_data_transformed, po_output_path / f"po_{i}.parquet"
            )
    # if po_data.num_rows == 0:
    #     logger.warning(f"No property objects found for dataset {dataset_id}")
    #     return
    # for i, po_batch in enumerate(
    #     [
    #         po_data.slice(i, po_batch_size)
    #         for i in range(0, po_data.num_rows, po_batch_size)
    #     ]
    # ):
    #     po_data_transformed = transform_table_arrays(po_batch, po_type_map)
    #     pa.parquet.write_table(po_data_transformed, po_output_path / f"po_{i}.parquet")
    logger.info(f"Saved PO data to: {po_output_path}")
    del po_data
    co_type_map = {
        "nested_double": co_nested_arr_cols,
        "double_array": co_double_arr_cols,
        "int_array": co_int_arr_cols,
        "str_array": co_str_arr_cols,
        "bool_array": co_bool_arr_cols,
    }
    co_output_path = dataset_dir / "co"
    co_output_path.mkdir(parents=True, exist_ok=True)
    with session.transaction() as tx:
        co_table = tx.bucket("colabfit-prod").schema("prod").table("co")
        co_data = co_table.select(
            predicate=co_table["dataset_ids"].contains(dataset_id), config=config
        )
        # co_data = co_data.read_all()
        # logger.info(f"Read CO rows: {co_data.num_rows}")
        # if co_data.num_rows == 0:
        #     logger.warning(f"No configuration objects found for dataset {dataset_id}")
        #     return
        # co_batch_size = 100000
        for i, co_batch in enumerate(co_data):
            logger.info(f"Read CO rows: {co_batch.num_rows}")
            co_data_transformed = transform_table_arrays(co_batch, co_type_map)
            pa.parquet.write_table(
                co_data_transformed, co_output_path / f"co_{i}.parquet"
            )

    # for i, co_batch in enumerate(
    #     [
    #         co_data.slice(i, co_batch_size)
    #         for i in range(0, co_data.num_rows, co_batch_size)
    #     ]
    # ):
    #     batch_output_path = co_output_path / f"co_{i}.parquet"
    #     co_data_transformed = transform_table_arrays(co_batch, co_type_map)
    #     pa.parquet.write_table(co_data_transformed, batch_output_path)
    #     logger.info(f"Wrote CO batch {i} to: {batch_output_path}")
    del co_data
    with session.transaction() as tx:
        ds_table = tx.bucket("colabfit-prod").schema("prod").table("ds")
        ds_data = ds_table.select(predicate=ds_table["id"] == dataset_id)
        ds_data = ds_data.read_all()
        logger.info(f"Read DS rows: {ds_data.num_rows}")
    if ds_data.num_rows > 0:
        # Transform DS data arrays
        ds_type_map = {
            "nested_int": ds_nested_arr_cols,
            "double_array": ds_double_arr_cols,
            "int_array": ds_int_arr_cols,
            "str_array": ds_str_arr_cols,
        }
        ds_data_transformed = transform_table_arrays(ds_data, ds_type_map)

        # Save DS data to parquet
        ds_output_path = dataset_dir / "ds.parquet"
        pa.parquet.write_table(ds_data_transformed, ds_output_path)
        logger.info(f"Saved DS data to: {ds_output_path}")

    # Get configuration sets if they exist
    with session.transaction() as tx:
        cs_table = tx.bucket("colabfit-prod").schema("prod").table("cs")
        cs_data = cs_table.select(predicate=cs_table["dataset_id"] == dataset_id)
        cs_data = cs_data.read_all()
        logger.info(f"Read CS rows: {cs_data.num_rows}")

    if cs_data.num_rows > 0:
        # Transform CS data arrays
        cs_type_map = {
            "nested_int": cs_nested_arr_cols,
            "double_array": cs_double_arr_cols,
            "int_array": cs_int_arr_cols,
            "str_array": cs_str_arr_cols,
        }
        cs_data_transformed = transform_table_arrays(cs_data, cs_type_map)

        # Save CS data to parquet
        cs_output_path = dataset_dir / "cs.parquet"
        pa.parquet.write_table(cs_data_transformed, cs_output_path)
        logger.info(f"Saved CS data to: {cs_output_path}")

        # Get CS-CO mapping if configuration sets exist
        cs_ids = cs_data.column("id").to_pylist()
        with session.transaction() as tx:
            cs_co_map_table = (
                tx.bucket("colabfit-prod").schema("prod").table("cs_co_map")
            )
            cs_co_map_data = cs_co_map_table.select(
                predicate=cs_co_map_table["configuration_set_id"].isin(cs_ids)
            )
            cs_co_map_data = cs_co_map_data.read_all()
            logger.info(f"Read CS-CO mapping rows: {cs_co_map_data.num_rows}")

        if cs_co_map_data.num_rows > 0:
            cs_co_map_output_path = dataset_dir / "cs_co_map.parquet"
            pa.parquet.write_table(cs_co_map_data, cs_co_map_output_path)
            logger.info(f"Saved CS-CO mapping to: {cs_co_map_output_path}")

    logger.info(
        f"Export completed for dataset {dataset_id} in {time() - start:.2f} seconds"
    )


def process_datasets_from_file(id_file, index):
    """
    Process multiple datasets from a file containing dataset IDs

    Args:
        id_file: Path to file containing dataset IDs (one per line)
        output_dir: Directory to save the parquet files
    """
    logger.info(f"Processing datasets from file: {id_file}")
    output_dir = Path().cwd()
    with open(id_file, "r") as f:
        dataset_ids = [line.strip() for line in f.readlines() if line.strip()][::-1][
            index:
        ]

    logger.info(f"Found {len(dataset_ids)} datasets to process")

    for i, dataset_id in enumerate(dataset_ids, 1):
        logger.info(f"Processing dataset {i}/{len(dataset_ids)}: {dataset_id}")
        try:
            export_dataset_parquets(dataset_id, output_dir)
        except Exception as e:
            logger.error(f"Error processing dataset {dataset_id}: {str(e)}")
            continue


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python export_parquets_sdk.py <dataset_id_or_file> <index>")
        print(
            "  dataset_id_or_file: Single dataset ID or path to file with "
            "dataset IDs"
        )
        sys.exit(1)

    input_arg = sys.argv[1]
    index = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    # Check if input is a file or a single dataset ID
    if Path(input_arg).is_file():
        process_datasets_from_file(input_arg, index)
    else:
        # Treat as single dataset ID
        export_dataset_parquets(input_arg)
