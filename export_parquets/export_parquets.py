import logging
import math
import os
import sys
from ast import literal_eval
from pathlib import Path
from time import time

import boto3
from ibis import _
import pyarrow as pa
import vastdb
from concurrent.futures import ThreadPoolExecutor
from colabfit.tools.vast.schema import (
    config_prop_arr_schema,
    configuration_set_arr_schema,
    dataset_arr_schema,
)
from colabfit.tools.vast.utilities import spark_schema_to_arrow_schema
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

CONFIG = {
    "MAX_WORKERS": 10,  # More reasonable for S3 connections
    "CO_BATCH_SIZE": 200_000,  # Match your current usage
    "CS_BATCH_SIZE": 100_000,
    "FILE_ROW_LIMIT": 1_000_000,  # Match your current usage
    "CSCO_BATCH_SIZE": 10_000,
    "COMPRESSION_LEVEL": 18,
    "LARGE_DATASET_THRESHOLD": 5_000_000,
}


class S3FileManager:
    def __init__(self, bucket_name, access_id, secret_key, endpoint_url=None):
        self.bucket_name = bucket_name
        self.access_id = access_id
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self._client = self.get_client()

    def get_client(self):
        return boto3.client(
            "s3",
            use_ssl=False,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_id,
            aws_secret_access_key=self.secret_key,
            region_name="fake-region",
            config=boto3.session.Config(
                signature_version="s3v4", s3={"addressing_style": "path"}
            ),
        )

    def write_file(self, content, file_key):
        try:
            client = self.get_client()
            client.put_object(Bucket=self.bucket_name, Key=file_key, Body=content)
        except Exception as e:
            return f"Error: {str(e)}"

    def read_file(self, file_key):
        try:
            response = self._client.get_object(Bucket=self.bucket_name, Key=file_key)
            return response["Body"].read().decode("utf-8")
        except Exception as e:
            return f"Error: {str(e)}"


def get_s3_file_manager():
    endpoint = "http://10.32.38.210"
    with open(f"/home/{os.environ['USER']}/.vast-dev/access_key_id", "r") as f:
        access_key = f.read().rstrip("\n")
    with open(f"/home/{os.environ['USER']}/.vast-dev/secret_access_key", "r") as f:
        secret_key = f.read().rstrip("\n")
    return S3FileManager(
        bucket_name="colabfit-data",
        access_id=access_key,
        secret_key=secret_key,
        endpoint_url=endpoint,
    )


def write_parquet_file(table, output_path, compression_level=None):
    """Unified function to write parquet files with consistent settings"""
    if compression_level is None:
        compression_level = CONFIG["COMPRESSION_LEVEL"]
    
    with pa.parquet.ParquetWriter(
        output_path,
        table.schema,
        compression="zstd",
        compression_level=compression_level,
    ) as writer:
        writer.write_table(table)


def read_metadata_column(table: pa.Table):
    prop_paths = table["property_metadata_path"].to_pylist()
    config_paths = table["configuration_metadata_path"].to_pylist()
    max_workers = CONFIG["MAX_WORKERS"]

    def safe_read(path_s3_tuple):
        path, s3_mgr = path_s3_tuple
        if path is None:
            return None
        try:
            return s3_mgr.read_file(path)
        except Exception as e:
            return f"Error: {str(e)}"

    prop_unique = list({p for p in prop_paths if p is not None})
    config_unique = list({c for c in config_paths if c is not None})
    all_unique_paths = prop_unique + config_unique
    logger.info(f"Found {len(all_unique_paths)} distinct metadata paths to read")

    start_md = time()
    if not all_unique_paths:
        prop_metadata_list = [None] * len(prop_paths)
        config_metadata_list = [None] * len(config_paths)
    else:
        max_workers = min(max_workers, len(all_unique_paths))
        s3s = [get_s3_file_manager() for _ in range(max_workers)]
        all_unique_s3_map = [
            (path, s3s[i % max_workers]) for i, path in enumerate(all_unique_paths)
        ]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            all_results = list(executor.map(safe_read, all_unique_s3_map))
        path_to_content = dict(zip(all_unique_paths, all_results))

        prop_metadata_list = [
            path_to_content.get(p) if p is not None else None for p in prop_paths
        ]
        config_metadata_list = [
            path_to_content.get(c) if c is not None else None for c in config_paths
        ]

    end_md = time()
    logger.info(f"Metadata read completed in {end_md - start_md:.2f} seconds")

    prop_metadata_array = pa.array(prop_metadata_list).cast("string")
    config_metadata_array = pa.array(config_metadata_list).cast("string")

    table = table.append_column(
        pa.field("configuration_metadata", pa.string(), nullable=True),
        config_metadata_array,
    )
    table = table.append_column(
        pa.field("property_metadata", pa.string(), nullable=True), prop_metadata_array
    )
    logger.info(f"MD ops finished in {time() - start_md:.2f} seconds")
    return table


def batch_manager(data_iterator, target_batch_size=100_000):
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
    field.name
    for field in co_arrow_schema
    if (field.type == pa.list_(pa.int64()) or field.type == pa.list_(pa.int32()))
]
co_bool_arr_cols = [
    field.name for field in co_arrow_schema if field.type == pa.list_(pa.bool_())
]

CO_TYPE_MAP = {
    "nested_double": co_nested_arr_cols,
    "double_array": co_double_arr_cols,
    "int_array": co_int_arr_cols,
    "str_array": co_str_arr_cols,
    "bool_array": co_bool_arr_cols,
}


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


def get_vastdb_session():
    endpoint = "http://10.32.38.210"
    with open(f"/home/{os.environ['USER']}/.vast-dev/access_key_id", "r") as f:
        access_key = f.read().rstrip("\n")
    with open(f"/home/{os.environ['USER']}/.vast-dev/secret_access_key", "r") as f:
        secret_key = f.read().rstrip("\n")
    return vastdb.connect(endpoint=endpoint, access=access_key, secret=secret_key)


def export_configuration_parquets(dataset_id, dataset_dir, session):
    """
    Export configuration parquet files using VastDB SDK

    Args:
        dataset_id: The dataset ID to export
        dataset_dir: Directory to save the parquet files
        session: VastDB session
    """
    start = time()
    logger.info(f"Starting export for dataset: {dataset_id}")

    co_output_path = dataset_dir / "co"
    co_output_path.mkdir(parents=True, exist_ok=True)

    predicate = _.dataset_id == dataset_id
    batch_count, file_count, total_rows = _export_configs(
        predicate, co_output_path, session, 0
    )

    logger.info(
        f"CO processing complete: {batch_count} batches, {total_rows} total rows"
    )
    logger.info(f"CO export took {time() - start:.2f} seconds")

    if batch_count == 0:
        logger.warning(f"No CO batches found for dataset {dataset_id}")
    if total_rows == 0:
        logger.warning(f"No CO rows found for dataset {dataset_id}")


def _export_configs(predicate, co_output_path, session, initial_file_count):

    with session.transaction() as tx:
        co_table = (
            tx.bucket("colabfit-prod").schema("prod").table("co_po_merged_innerjoin")
        )
        co_data = co_table.select(predicate=predicate)
        batch_count = 0
        file_rows = 0
        file_tables = []
        file_row_size = CONFIG["FILE_ROW_LIMIT"]
        file_count = initial_file_count
        total_rows = 0
        try:
            managed_batches = batch_manager(
                co_data, target_batch_size=CONFIG["CO_BATCH_SIZE"]
            )
            for i, co_batch in enumerate(managed_batches):
                batch_count += 1
                batch_rows = co_batch.num_rows
                total_rows += batch_rows
                file_rows += batch_rows
                logger.info(f"Read CO batch {i}: {batch_rows} rows")
                if batch_rows == 0:
                    logger.warning(f"CO batch {i} is empty, skipping")
                    continue
                co_data_transformed = transform_table_arrays(co_batch, CO_TYPE_MAP)
                co_data_transformed = read_metadata_column(co_data_transformed)
                file_tables.append(co_data_transformed)

                if file_rows >= file_row_size:
                    file_table = pa.concat_tables(file_tables)
                    file_tables = []
                    output_file = co_output_path / f"co_{file_count}.parquet"
                    logger.info(f"Saving CO batch {file_count} to {output_file}")
                    write_parquet_file(
                        file_table, output_file, CONFIG["COMPRESSION_LEVEL"]
                    )
                    logger.info(f"Successfully saved CO batch {file_count}")
                    file_count += 1
                    file_rows = 0
            if file_tables:
                file_table = pa.concat_tables(file_tables)
                file_tables = []
                output_file = co_output_path / f"co_{file_count}.parquet"
                logger.info(f"Saving final CO batch {file_count} to {output_file}")
                write_parquet_file(
                    file_table, output_file, CONFIG["COMPRESSION_LEVEL"]
                )
                logger.info(f"Successfully saved final CO batch {file_count}")

        except Exception as e:
            logger.error(f"Error processing CO data: {e}")
            raise
    return batch_count, file_count, total_rows


def export_configurations_in_batches(dataset_id, dataset_dir, session):
    """
    Export configuration parquet files using VastDB SDK in batches

    Args:
        dataset_id: The dataset ID to export
        output_dir: Directory to save the parquet files
    """
    start = time()
    logger.info(f"Starting export for dataset: {dataset_id}")

    co_output_path = dataset_dir / "co"
    co_output_path.mkdir(parents=True, exist_ok=True)
    total_batch_count = 0
    total_rows = 0
    prefix_div = [f"PO_{i:02d}" for i in range(10, 100)]
    file_count = 0
    for prefix in prefix_div:
        logger.info(f"Processing prefix: {prefix} for dataset: {dataset_id}")
        predicate = (_.dataset_id == dataset_id) & (_.property_id.startswith(prefix))
        batch_count, file_count, batch_rows = _export_configs(
            predicate, co_output_path, session, file_count
        )
        total_batch_count += batch_count
        total_rows += batch_rows
        logger.info("CO processing complete")
        logger.info(f"Prefix {prefix}: {batch_count} batches, {batch_rows} total rows")

    logger.info(f"CO export took {time() - start:.2f} seconds")


def export_configuration_sets(dataset_id, dataset_dir, session):
    cs_dir_made = False
    cs_dir = dataset_dir / "cs"
    cs_ids_all = []
    with session.transaction() as tx:
        cs_table = (
            tx.bucket("colabfit-prod").schema("prod").table("configuration_set_arrays")
        )
        cs_data = cs_table.select(predicate=cs_table["dataset_id"] == dataset_id)
        for i, batch in enumerate(
            batch_manager(cs_data, target_batch_size=CONFIG["CS_BATCH_SIZE"])
        ):
            logger.info(f"Read CS batch {i}: {batch.num_rows} rows")
            if batch.num_rows == 0:
                logger.warning(f"CS batch {i} is empty, skipping")
                continue
            if not cs_dir_made:
                cs_dir.mkdir(parents=True, exist_ok=True)
                cs_dir_made = True
            cs_output_path = cs_dir / f"cs_{i}.parquet"
            write_parquet_file(batch, cs_output_path, CONFIG["COMPRESSION_LEVEL"])
            logger.info(f"Saved CS data to: {cs_output_path}")

            cs_ids = batch.column("id").to_pylist()
            cs_ids_all.extend(cs_ids)
    return cs_ids_all


def export_cs_co_mapping(cs_ids_all, dataset_dir, session):
    if not cs_ids_all:
        return

    cs_co_map_dir = dataset_dir / "cs_co_map"
    cs_co_map_dir.mkdir(parents=True, exist_ok=True)

    batch_size = CONFIG["CSCO_BATCH_SIZE"]
    file_count = 0
    file_tables = []
    file_rows = 0

    with session.transaction() as tx:
        cs_co_map_table = tx.bucket("colabfit-prod").schema("prod").table("cs_co_map")

        for i in range(0, len(cs_ids_all), batch_size):
            cs_id_batch = cs_ids_all[i : i + batch_size]  # noqa: E203
            cs_co_map_data = cs_co_map_table.select(
                predicate=cs_co_map_table["configuration_set_id"].isin(cs_id_batch)
            ).read_all()

            logger.info(f"Read CS-CO mapping batch: {cs_co_map_data.num_rows} rows")
            file_tables.append(cs_co_map_data)
            file_rows += cs_co_map_data.num_rows

            if file_rows >= CONFIG["FILE_ROW_LIMIT"]:
                output_file = cs_co_map_dir / f"cs_co_map_{file_count}.parquet"
                write_parquet_file(pa.concat_tables(file_tables), output_file)
                file_tables = []
                file_rows = 0
                file_count += 1

        if file_tables:
            output_file = cs_co_map_dir / f"cs_co_map_{file_count}.parquet"
            write_parquet_file(pa.concat_tables(file_tables), output_file)


def get_dataset_data(dataset_id, session):
    with session.transaction() as tx:
        ds_table = tx.bucket("colabfit-prod").schema("prod").table("dataset_arrays")
        ds_data = ds_table.select(predicate=ds_table["id"] == dataset_id)
        ds_data = ds_data.read_all()
        logger.info(f"Read DS rows: {ds_data.num_rows}")
    return ds_data


def write_dataset_parquet(ds_data, dataset_dir):
    if ds_data.num_rows > 0:
        ds_output_path = dataset_dir / "ds.parquet"
        write_parquet_file(ds_data, ds_output_path, CONFIG["COMPRESSION_LEVEL"])
        logger.info(f"Saved DS data to: {ds_output_path}")


def process_datasets_from_file(id_file, index):
    """
    Process multiple datasets from a file containing dataset IDs

    Args:
        id_file: Path to file containing dataset IDs (one per line)
        output_dir: Directory to save the parquet files
    """
    logger.info(f"Processing datasets from file: {id_file}")
    start = time()
    output_dir = Path().cwd()
    with open(id_file, "r") as f:
        dataset_ids = [line.strip() for line in f.readlines() if line.strip()][index:]

    logger.info(f"Found {len(dataset_ids)} datasets to process")

    for i, dataset_id in enumerate(dataset_ids, 1):
        logger.info(f"Processing dataset {i}/{len(dataset_ids)}: {dataset_id}")
        try:
            dataset_dir = Path(output_dir) / dataset_id
            if dataset_dir.exists():
                logger.info(f"Dataset {dataset_id} already exported, skipping")
                continue
            possible_tar_file = Path("tarfiles") / f"{dataset_id}.tar.gz"
            if possible_tar_file.exists():
                logger.info(f"Dataset {dataset_id} tar file already exists, skipping")
                continue
            dataset_dir.mkdir(parents=True, exist_ok=True)
            session = get_vastdb_session()
            ds_data = get_dataset_data(dataset_id, session)
            nconfigs = ds_data.column("nconfigurations")[0].as_py()
            if nconfigs > CONFIG["LARGE_DATASET_THRESHOLD"]:
                logger.info(
                    f"Dataset {dataset_id} has {nconfigs} configurations. "
                    "Using batches."
                )
                export_configurations_in_batches(dataset_id, dataset_dir, session)
            else:
                logger.info(
                    f"Dataset {dataset_id} has {nconfigs} configurations. "
                    "Selecting all at once."
                )
                export_configuration_parquets(dataset_id, dataset_dir, session)
            cs_ids_all = export_configuration_sets(dataset_id, dataset_dir, session)
            if cs_ids_all:
                export_cs_co_mapping(cs_ids_all, dataset_dir, session)
            write_dataset_parquet(ds_data, dataset_dir)
        except Exception as e:
            logger.error(f"Error processing dataset {dataset_id}: {str(e)}")
            continue
    logger.info(
        f"Export completed for dataset {dataset_id} in {time() - start:.2f} seconds"
    )


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
    if Path(input_arg).is_file():
        process_datasets_from_file(input_arg, index)
