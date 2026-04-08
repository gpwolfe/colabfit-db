import logging
import math
import os
import sys
import threading
from ast import literal_eval
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from time import sleep, time

import boto3
import pyarrow as pa
import vastdb
from botocore.config import Config as BotoConfig
from colabfit.tools.vast.schema import (
    config_prop_schema,
    configuration_set_schema,
    dataset_schema,
)
from colabfit.tools.vast.utils import spark_schema_to_arrow_schema
from dotenv import load_dotenv
from ibis import _

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

CONFIG = {
    "MAX_WORKERS": 96,
    "CO_BATCH_SIZE": 100_000,
    "CS_BATCH_SIZE": 100_000,
    "FILE_ROW_LIMIT": 500_000,
    "CSCO_BATCH_SIZE": 10_000,
    "COMPRESSION_LEVEL": 18,
    "LARGE_DATASET_THRESHOLD": 5_000_000,
    "S3_CACHE_SIZE": 4096,
    "S3_CONNECT_TIMEOUT": 5,
    "S3_READ_TIMEOUT": 60,
    "S3_MAX_ATTEMPTS": 8,
    "S3_MAX_POOL_CONNECTIONS": 256,
    "S3_BACKOFF_BASE": 0.05,
    "S3_BACKOFF_MAX": 2.0,
}


# BOTO_CLIENT_CONFIG = BotoConfig(
#     signature_version="s3v4",
#     s3={"addressing_style": "path"},
#     max_pool_connections=CONFIG["S3_MAX_POOL_CONNECTIONS"],
#     read_timeout=CONFIG["S3_READ_TIMEOUT"],
#     connect_timeout=CONFIG["S3_CONNECT_TIMEOUT"],
#     retries={"max_attempts": CONFIG["S3_MAX_ATTEMPTS"], "mode": "standard"},
# )


# class S3FileManager:
#     CACHE_MAX = CONFIG["S3_CACHE_SIZE"]
#     CACHE_LOCK = threading.Lock()
#     CACHE = OrderedDict()

#     def __init__(self, bucket_name, access_id, secret_key, endpoint_url=None):
#         self.bucket_name = bucket_name
#         self.access_id = access_id
#         self.secret_key = secret_key
#         self.endpoint_url = endpoint_url
#         self._client = self._create_client()

#     def _create_client(self):
#         return boto3.client(
#             "s3",
#             use_ssl=False,
#             endpoint_url=self.endpoint_url,
#             aws_access_key_id=self.access_id,
#             aws_secret_access_key=self.secret_key,
#             region_name="fake-region",
#             config=BOTO_CLIENT_CONFIG,
#         )

#     def get_client(self):
#         return self._client

#     def write_file(self, content, file_key):
#         try:
#             self._client.put_object(Bucket=self.bucket_name, Key=file_key, Body=content)
#         except Exception as e:
#             return f"Error: {str(e)}"

#     def read_file(self, file_key):
#         cache_key = (self.bucket_name, file_key)
#         if S3FileManager.CACHE_MAX:
#             with S3FileManager.CACHE_LOCK:
#                 cached = S3FileManager.CACHE.get(cache_key)
#                 if cached is not None:
#                     S3FileManager.CACHE.move_to_end(cache_key)
#                     return cached

#         response = self._client.get_object(Bucket=self.bucket_name, Key=file_key)
#         content = response["Body"].read().decode("utf-8")

#         if S3FileManager.CACHE_MAX:
#             with S3FileManager.CACHE_LOCK:
#                 S3FileManager.CACHE[cache_key] = content
#                 if len(S3FileManager.CACHE) > S3FileManager.CACHE_MAX:
#                     S3FileManager.CACHE.popitem(last=False)

#         return content


# @lru_cache(maxsize=1)
# def _load_s3_credentials():
#     endpoint = "http://10.32.38.210"
#     user_home = f"/home/{os.environ['USER']}"
#     with open(f"{user_home}/.vast-dev/access_key_id", "r") as f:
#         access_key = f.read().rstrip("\n")
#     with open(f"{user_home}/.vast-dev/secret_access_key", "r") as f:
#         secret_key = f.read().rstrip("\n")
#     return endpoint, access_key, secret_key


# def get_s3_file_manager():
#     endpoint, access_key, secret_key = _load_s3_credentials()
#     return S3FileManager(
#         bucket_name="colabfit-data",
#         access_id=access_key,
#         secret_key=secret_key,
#         endpoint_url=endpoint,
#     )


def write_parquet_file(table, output_path, compression_level=None):
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
    #     prop_paths = table["property_metadata_path"].to_pylist()
    #     config_paths = table["configuration_metadata_path"].to_pylist()
    #     max_workers = CONFIG["MAX_WORKERS"]

    #     def safe_read(path, s3_mgr):
    #         if path is None:
    #             return None

    #         retries = CONFIG["S3_MAX_ATTEMPTS"]
    #         delay = CONFIG["S3_BACKOFF_BASE"]
    #         max_delay = CONFIG["S3_BACKOFF_MAX"]
    #         last_exc = None

    #         for attempt in range(1, retries + 1):
    #             try:
    #                 return s3_mgr.read_file(path)
    #             except Exception as exc:
    #                 last_exc = exc
    #                 if attempt < retries:
    #                     sleep(delay)
    #                     delay = min(delay * 2, max_delay)
    #                 else:
    #                     logger.error(
    #                         "Failed to read metadata from %s after %s attempts",
    #                         path,
    #                         retries,
    #                     )
    #         raise last_exc

    #     prop_unique = list({p for p in prop_paths if p is not None})
    #     config_unique = list({c for c in config_paths if c is not None})
    #     all_unique_paths = prop_unique + config_unique
    #     logger.info(f"Found {len(all_unique_paths)} distinct metadata paths to read")

    start_md = time()
    #     if not all_unique_paths:
    #         prop_metadata_list = [None] * len(prop_paths)
    #         config_metadata_list = [None] * len(config_paths)
    #     else:
    #         max_workers = min(max_workers, len(all_unique_paths)) or 1
    #         s3s = [get_s3_file_manager() for _ in range(max_workers)]
    #         path_to_content = {}

    #         with ThreadPoolExecutor(
    #             max_workers=max_workers, thread_name_prefix="s3-md"
    #         ) as executor:
    #             future_to_path = {
    #                 executor.submit(
    #                     safe_read,
    #                     path,
    #                     s3s[index % max_workers],
    #                 ): path
    #                 for index, path in enumerate(all_unique_paths)
    #             }

    #             for future in as_completed(future_to_path):
    #                 path = future_to_path[future]
    #                 try:
    #                     path_to_content[path] = future.result()
    #                 except Exception as exc:
    #                     path_to_content[path] = f"Error: {str(exc)}"

    metadata_list = pa.array([None for p in range(table.num_rows)]).cast("string")

    #     end_md = time()
    #     logger.info(f"Metadata read completed in {end_md - start_md:.2f} seconds")

    #     prop_metadata_array = pa.array(prop_metadata_list).cast("string")
    #     config_metadata_array = pa.array(config_metadata_list).cast("string")

    table = table.append_column(
        pa.field("configuration_metadata", pa.string(), nullable=True),
        metadata_list,
    )
    table = table.append_column(
        pa.field("property_metadata", pa.string(), nullable=True), metadata_list
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


co_arrow_schema = spark_schema_to_arrow_schema(config_prop_schema)
ds_arrow_schema = spark_schema_to_arrow_schema(dataset_schema)
cs_arrow_schema = spark_schema_to_arrow_schema(configuration_set_schema)

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
    """Export configuration parquet files using VastDB SDK"""
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
                write_parquet_file(file_table, output_file, CONFIG["COMPRESSION_LEVEL"])
                logger.info(f"Successfully saved final CO batch {file_count}")
                file_count += 1

        except Exception as e:
            logger.error(f"Error processing CO data: {e}")
            raise
    return batch_count, file_count, total_rows


def export_configurations_in_batches(dataset_id, dataset_dir, session):
    """Export configuration parquet files using VastDB SDK in batches"""
    start = time()
    logger.info(f"Starting export for dataset: {dataset_id}")
    existing_files = list((dataset_dir / "co" / "tmp").glob("*.parquet"))
    if existing_files:
        logger.info(
            f"Found {len(existing_files)} existing temporary CO files, "
            "they will be removed."
        )
        for tmp_file in existing_files:
            tmp_file.unlink()
    co_dir = dataset_dir / "co"
    if not co_dir.exists():
        co_dir.mkdir(parents=True, exist_ok=True)
    co_tmp_path = dataset_dir / "co" / "tmp"
    if not co_tmp_path.exists():
        co_tmp_path.mkdir(parents=True, exist_ok=True)
    total_batch_count = 0
    total_rows = 0
    prefix_div = [f"PO_{i:03d}" for i in range(100, 140)]
    prefix_div += [f"PO_{i:02d}" for i in range(14, 100)]
    existing_prefix_paths = {p.name for p in co_dir.glob("PO_*")}

    # Find last file count from existing files
    max_file_count = 0
    for prefix_dir in co_dir.glob("PO_*"):
        if prefix_dir.is_dir():
            for parquet_file in prefix_dir.glob("co_*.parquet"):
                try:
                    file_num = int(parquet_file.stem.split("_")[1])
                    max_file_count = max(max_file_count, file_num)
                except (ValueError, IndexError):
                    continue

    for parquet_file in co_dir.glob("co_*.parquet"):
        try:
            file_num = int(parquet_file.stem.split("_")[1])
            max_file_count = max(max_file_count, file_num)
        except (ValueError, IndexError):
            continue

    file_count = max_file_count + 1 if max_file_count > 0 else 0
    logger.info(
        f"Starting file count at {file_count} (found max existing: {max_file_count})"
    )
    for prefix in prefix_div:
        if prefix in existing_prefix_paths:
            logger.info(f"Prefix {prefix} already processed, skipping")
            continue
        logger.info(f"Processing prefix: {prefix} for dataset: {dataset_id}")
        predicate = (_.dataset_id == dataset_id) & (_.property_id.startswith(prefix))
        batch_count, file_count, batch_rows = _export_configs(
            predicate, co_tmp_path, session, file_count
        )
        total_batch_count += batch_count
        total_rows += batch_rows
        logger.info("CO processing complete")
        logger.info(f"Prefix {prefix}: {batch_count} batches, {batch_rows} total rows")
        co_prefix_path = co_dir / prefix
        if not co_prefix_path.exists():
            co_prefix_path.mkdir(parents=True, exist_ok=True)
        for file in co_tmp_path.glob("*.parquet"):
            final_path = co_prefix_path / file.name
            file.rename(final_path)
        logger.info(f"Moved temporary CO files for {prefix} to {co_prefix_path}")

    logger.info(f"CO export took {time() - start:.2f} seconds")
    logger.info(f"Consolidating prefix directories in {co_dir}")
    for file in co_dir.glob("PO_*/*.parquet"):
        final_path = co_dir / file.name
        file.rename(final_path)
    for prefix in prefix_div:
        prefix_path = co_dir / prefix
        if prefix_path.exists() and prefix_path.is_dir():
            try:
                prefix_path.rmdir()
                logger.info(f"Removed empty directory: {prefix_path}")
            except OSError as e:
                logger.warning(f"Could not remove directory {prefix_path}: {e}")


def export_configuration_sets(dataset_id, dataset_dir, session):
    cs_dir_created = False
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
            if not cs_dir_created:
                cs_dir.mkdir(parents=True, exist_ok=True)
                cs_dir_created = True
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
    cs_co_map_dir_created = False

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
            if cs_co_map_data.num_rows == 0:
                logger.info("CS-CO mapping batch is empty, skipping write")
                continue

            file_tables.append(cs_co_map_data)
            file_rows += cs_co_map_data.num_rows

            if file_rows >= CONFIG["FILE_ROW_LIMIT"]:
                if not cs_co_map_dir_created:
                    cs_co_map_dir.mkdir(parents=True, exist_ok=True)
                    cs_co_map_dir_created = True
                output_file = cs_co_map_dir / f"cs_co_map_{file_count}.parquet"
                write_parquet_file(pa.concat_tables(file_tables), output_file)
                file_tables = []
                file_rows = 0
                file_count += 1

        if file_rows > 0:
            if not cs_co_map_dir_created:
                cs_co_map_dir.mkdir(parents=True, exist_ok=True)
                cs_co_map_dir_created = True
            output_file = cs_co_map_dir / f"cs_co_map_{file_count}.parquet"
            write_parquet_file(pa.concat_tables(file_tables), output_file)


def get_dataset_data(dataset_id, session):
    with session.transaction() as tx:
        ds_table = tx.bucket("colabfit-prod").schema("prod").table("dataset_arrays")
        ds_data = ds_table.select(predicate=ds_table["id"] == dataset_id)
        ds_data = ds_data.read_all()
        logger.info(f"Read DS rows: {ds_data.num_rows}")
    return ds_data


def check_table_exists(session, table_name):
    with session.transaction() as tx:
        exists = (
            tx.bucket("colabfit-prod")
            .schema("prod")
            .table(table_name, fail_if_missing=False)
        )
        if exists is not None:
            return True


def write_dataset_parquet(ds_data, dataset_dir):
    if ds_data.num_rows > 0:
        ds_output_path = dataset_dir / "ds.parquet"
        write_parquet_file(ds_data, ds_output_path, CONFIG["COMPRESSION_LEVEL"])
        logger.info(f"Saved DS data to: {ds_output_path}")


def process_dataset(dataset_id):
    """
    Process multiple datasets from a file containing dataset IDs

    Args:
        id_file: Path to file containing dataset IDs (one per line)
        output_dir: Directory to save the parquet files
    """
    logger.info(f"Processing dataset: {dataset_id}")
    start = time()
    output_dir = Path().cwd()

    try:
        dataset_dir = Path(output_dir) / dataset_id
        if (dataset_dir / "ds.parquet").exists():
            logger.info(f"Dataset {dataset_id} already exported, skipping")
        possible_tar_file = Path("tarfiles") / f"{dataset_id}.tar.gz"
        if possible_tar_file.exists():
            logger.info(f"Dataset {dataset_id} tar file already exists, skipping")
        dataset_dir.mkdir(parents=True, exist_ok=True)
        session = get_vastdb_session()
        ds_data = get_dataset_data(dataset_id, session)
        nconfigs = ds_data.column("nconfigurations")[0].as_py()
        if nconfigs > CONFIG["LARGE_DATASET_THRESHOLD"]:
            logger.info(
                f"Dataset {dataset_id} has {nconfigs} configurations. " "Using batches."
            )
            export_configurations_in_batches(dataset_id, dataset_dir, session)
            logger.info("Completed CO export in batches. Moving all CO files to co/")
            co_dir = dataset_dir / "co"
            co_batch_paths = sorted(list(co_dir.rglob("*.parquet")))
            for batch_path in co_batch_paths:
                final_path = co_dir / batch_path.name
                if batch_path != final_path:
                    batch_path.rename(final_path)
            for subdir in co_dir.iterdir():
                if subdir.is_dir():
                    try:
                        subdir.rmdir()
                        logger.info(f"Removed empty directory: {subdir}")
                    except OSError as e:
                        logger.warning(f"Could not remove directory {subdir}: {e}")
        else:
            logger.info(
                f"Dataset {dataset_id} has {nconfigs} configurations. "
                "Selecting all at once."
            )
            export_configuration_parquets(dataset_id, dataset_dir, session)
        cs_ids_all = []
        if check_table_exists(session, "configuration_set_arrays"):
            cs_ids_all = export_configuration_sets(dataset_id, dataset_dir, session)
        else:
            logger.info(
                f"Table configuration_set_arrays does not exist, skipping CS export"
            )

        if cs_ids_all and check_table_exists(session, "cs_co_map"):
            export_cs_co_mapping(cs_ids_all, dataset_dir, session)
        elif cs_ids_all:
            logger.info(
                f"Table cs_co_map does not exist, " "skipping CS-CO mapping export"
            )
        write_dataset_parquet(ds_data, dataset_dir)
    except Exception as e:
        logger.error(f"Error processing dataset {dataset_id}: {str(e)}")
    logger.info(
        f"Export completed for dataset {dataset_id} in {time() - start:.2f} seconds"
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python export_parquets_sdk.py <dataset_id>")
        sys.exit(1)

    ds_id = sys.argv[1]
    process_dataset(ds_id)
