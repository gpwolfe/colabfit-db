import logging
import os
import sys
from ast import literal_eval
from pathlib import Path

import boto3
import pyspark.sql.functions as sf
from botocore.exceptions import ClientError
from colabfit.tools.vast.schema import (
    config_arr_schema,
    configuration_set_arr_schema,
    dataset_arr_schema,
    property_object_arr_schema,
)
from colabfit.tools.vast.utilities import (
    str_to_arrayof_int,
    str_to_arrayof_str,
    str_to_nestedarrayof_double,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
)


with open("/scratch/gw2338/colabfit-tools/.env") as f:
    envvars = dict(x.strip().split("=") for x in f.readlines())

jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("colabfit")
    .config("spark.jars", jars)
    .config("spark.executor.memoryOverhead", "600")
    # .config("spark.sql.shuffle.partitions", 4000)
    .config("spark.driver.maxResultSize", 0)
    .config("spark.sql.adaptive.enabled", "true")
    # .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
    # .config("spark.sql.adaptive.skewJoin.enabled", "true")
    # .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    # .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()
)
# TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
access = envvars.get("SPARK_ID")
secret = envvars.get("SPARK_KEY")
endpoint = envvars.get("SPARK_ENDPOINT")


logger = logging.getLogger(f"{__name__}")
logger.setLevel("INFO")

parquet_directory = "/scratch/gw2338/vast/data-lake-main/spark/scripts/parquet_export"


class S3FileManager:
    def __init__(self, bucket_name, access_id, secret_key, endpoint_url=None):
        self.bucket_name = bucket_name
        self.access_id = access_id
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url

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
            client = self.get_client()
            response = client.get_object(Bucket=self.bucket_name, Key=file_key)
            return response["Body"].read().decode("utf-8")
        except Exception as e:
            return f"Error: {str(e)}"


def create_metadata_reader_udf(config):
    s3_mgr = S3FileManager(
        bucket_name=config["bucket_dir"],
        access_id=config["access_key"],
        secret_key=config["access_secret"],
        endpoint_url=config["endpoint"],
    )

    def read_metadata(metadata_path):
        if metadata_path is None:
            return None
        try:
            return s3_mgr.read_file(metadata_path)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return None
            else:
                logger.info(f"Error reading {metadata_path}: {str(e)}")
                return None

    return sf.udf(read_metadata, StringType())


def create_column_transformer(col_name, col_type_map):
    """Create optimized column transformations based on type mapping"""
    if col_name in col_type_map.get("nested_double", []):
        return str_to_nestedarrayof_double(sf.col(col_name)).alias(col_name)
    elif col_name in col_type_map.get("int_array", []):
        return str_to_arrayof_int(sf.col(col_name)).alias(col_name)
    elif col_name in col_type_map.get("str_array", []):
        return str_to_arrayof_str(sf.col(col_name)).alias(col_name)
    elif col_name in col_type_map.get("double_array", []):
        return str_to_arrayof_double(sf.col(col_name)).alias(col_name)
    elif col_name in col_type_map.get("bool_array", []):
        return str_to_arrayof_bool(sf.col(col_name)).alias(col_name)
    elif col_name in col_type_map.get("nested_int", []):
        return str_to_nestedarrayof_int(sf.col(col_name)).alias(col_name)
    else:
        return sf.col(col_name)


def create_column_renamer(col_name, prefix, special_cols):
    """Create optimized column renaming based on special column sets"""
    if col_name in special_cols:
        return sf.col(col_name).alias(f"{prefix}_{col_name}")
    else:
        return sf.col(col_name)


config = {
    "bucket_dir": "colabfit-data",
    "access_key": access,
    "access_secret": secret,
    "endpoint": endpoint,
    "metadata_dir": "data/MD",
}


@sf.udf(returnType=ArrayType(DoubleType()))
def str_to_arrayof_double(val):
    if val is None:
        return None
    if isinstance(val, str) and len(val) > 0 and val[0] == "[":
        return literal_eval(val)
    raise ValueError(f"Error converting {val} to list")


@sf.udf(returnType=ArrayType(BooleanType()))
def str_to_arrayof_bool(val):
    if val is None:
        return None
    if isinstance(val, str) and len(val) > 0 and val[0] == "[":
        return literal_eval(val)
    raise ValueError(f"Error converting {val} to list")


@sf.udf(returnType=ArrayType(ArrayType(IntegerType())))
def str_to_nestedarrayof_int(val):
    if val is None:
        return None
    if isinstance(val, str) and len(val) > 0 and val[0] == "[":
        return literal_eval(val)
    raise ValueError(f"Error converting {val} to list")


co_nested_arr_cols = [
    col.name
    for col in config_arr_schema
    if col.simpleString().split(":")[1] == "array<array<double>>"
]
co_double_arr_cols = [
    col.name
    for col in config_arr_schema
    if col.simpleString().split(":")[1] == "array<double>"
]
co_str_arr_cols = [
    col.name
    for col in config_arr_schema
    if col.simpleString().split(":")[1] == "array<string>"
]
co_int_arr_cols = [
    col.name
    for col in config_arr_schema
    if col.simpleString().split(":")[1] == "array<int>"
]
co_bool_arr_cols = [
    col.name
    for col in config_arr_schema
    if col.simpleString().split(":")[1] == "array<boolean>"
]
ds_double_arr_cols = [
    col.name
    for col in dataset_arr_schema
    if col.simpleString().split(":")[1] == "array<double>"
]
ds_str_arr_cols = [
    col.name
    for col in dataset_arr_schema
    if col.simpleString().split(":")[1] == "array<string>"
]
ds_int_arr_cols = [
    col.name
    for col in dataset_arr_schema
    if col.simpleString().split(":")[1] == "array<int>"
]
ds_nested_intarr_cols = [
    col.name
    for col in dataset_arr_schema
    if col.simpleString().split(":")[1] == "array<array<int>>"
]

cs_double_arr_cols = [
    col.name
    for col in configuration_set_arr_schema
    if col.simpleString().split(":")[1] == "array<double>"
]
cs_str_arr_cols = [
    col.name
    for col in configuration_set_arr_schema
    if col.simpleString().split(":")[1] == "array<string>"
]
cs_int_arr_cols = [
    col.name
    for col in configuration_set_arr_schema
    if col.simpleString().split(":")[1] == "array<int>"
]
cs_nested_intarr_cols = [
    col.name
    for col in configuration_set_arr_schema
    if col.simpleString().split(":")[1] == "array<array<int>>"
]


def get_cos(dataset_id, nconfigs):
    co_columns = spark.table("ndb.`colabfit-prod`.prod.co").columns
    logger.info(f"co_columns: {co_columns}")

    co_type_map = {
        "nested_double": co_nested_arr_cols,
        "int_array": co_int_arr_cols,
        "str_array": co_str_arr_cols,
        "double_array": co_double_arr_cols,
        "bool_array": co_bool_arr_cols,
    }
    co_dir = f"{parquet_directory}/{dataset_id}"
    transformed_cols = [
        create_column_transformer(col, co_type_map) for col in co_columns
    ]
    co_ids = spark.read.parquet(f"{co_dir}/co_ids.parquet")
    # cos = (
    #     spark.table("ndb.`colabfit-prod`.prod.co")
    #     .filter(sf.col("dataset_ids").contains(dataset_id))
    #     .select(transformed_cols)
    # )
    cos = co_ids.join(spark.table("ndb.`colabfit-prod`.prod.co"), on="id", how="inner")
    cos = cos.select(transformed_cols)
    if cos.filter("metadata_path is not null").count() > 0:
        metadata_udf = create_metadata_reader_udf(config)
        cos = cos.withColumn("metadata", metadata_udf(sf.col("metadata_path")))
    else:
        cos = cos.withColumn("metadata", sf.lit(None).cast(StringType()))
    cos_save_path = Path(f"{co_dir}/co.parquet")
    n_partitions = max(1, nconfigs // 100000)
    cos.coalesce(n_partitions).write.parquet(str(cos_save_path))
    logger.info(f"Saved COs for {dataset_id} to {cos_save_path}")
    # delete coids file
    for file in Path(f"{co_dir}/co_ids.parquet").glob("*"):
        if file.is_file():
            file.unlink()
    Path(f"{co_dir}/co_ids.parquet").rmdir(missing_ok=True)


def get_pos(dataset_id, nconfigs):
    logger.info(f"Processing property objects for dataset {dataset_id}")
    po_arr_cols = [
        field.name
        for field in property_object_arr_schema
        if field.dataType.typeName() == "array"
    ]
    po_type_map = {
        "nested_double": po_arr_cols,
    }
    pos_columns = spark.table("ndb.`colabfit-prod`.prod.po").columns
    transformed_cols = [
        create_column_transformer(col, po_type_map)
        for col in property_object_arr_schema.fieldNames()
        if (col != "metadata" and col in pos_columns)
    ]
    pos = (
        spark.table("ndb.`colabfit-prod`.prod.po")
        .filter(sf.col("dataset_id") == dataset_id)
        .select(transformed_cols)
    )
    po_dir = f"{parquet_directory}/{dataset_id}"
    metadata_udf = create_metadata_reader_udf(config)
    pos = pos.withColumn("metadata", metadata_udf(sf.col("metadata_path")))
    logger.info(f"pos columns: {pos.columns}")
    pos_save_path = Path(f"{po_dir}/po.parquet")
    n_partitions = max(1, nconfigs // 100000)
    pos.repartition(n_partitions).write.parquet(str(pos_save_path))
    logger.info(f"Saved PO for {dataset_id} to {pos_save_path}")
    pos.select("configuration_id").withColumnRenamed(
        "configuration_id", "id"
    ).write.parquet(f"{po_dir}/co_ids.parquet")


def get_dataset(dataset_id):
    ds = spark.table("ndb.`colabfit-prod`.prod.ds").filter(sf.col("id") == dataset_id)
    if ds is None:
        logger.warning(f"Dataset {dataset_id} not found in prod.ds")
        return None
    ds_type_map = {
        "int_array": ds_int_arr_cols,
        "str_array": ds_str_arr_cols,
        "double_array": ds_double_arr_cols,
        "nested_int": ds_nested_intarr_cols,
    }
    transformed_cols = [
        create_column_transformer(col, ds_type_map) for col in ds.columns
    ]
    ds_dir = f"{parquet_directory}/{dataset_id}"
    ds = ds.select(transformed_cols)
    ds.write.mode("overwrite").json(f"{ds_dir}/ds.json")
    logger.info(f"Saved DS for {dataset_id} to {ds_dir}/ds.json")
    return ds.select("nconfigurations").first()["nconfigurations"]


def get_configuration_sets(dataset_id):
    cs = spark.table("ndb.`colabfit-prod`.prod.cs").filter(
        sf.col("dataset_id") == dataset_id
    )
    if cs.count() == 0:
        logger.info(f"No configuration sets found for dataset {dataset_id}")
        return None

    cs_type_map = {
        "int_array": cs_int_arr_cols,
        "str_array": cs_str_arr_cols,
        "double_array": cs_double_arr_cols,
        "nested_int": cs_nested_intarr_cols,
    }
    transformed_cols = [
        create_column_transformer(col, cs_type_map) for col in cs.columns
    ]
    cs_dir = f"{parquet_directory}/{dataset_id}"
    cs = cs.select(transformed_cols)
    cs.write.mode("overwrite").parquet(f"{cs_dir}/cs.parquet")
    logger.info(f"Saved CS for {dataset_id} to {cs_dir}/cs.parquet")
    return cs.select("id")


def get_cs_co_mapping(dataset_id, ids):
    cs_co_map = spark.table("ndb.`colabfit-prod`.prod.cs_co_map").join(
        ids, sf.col("configuration_set_id") == sf.col("id"), "inner"
    )
    cs_co_dir = f"{parquet_directory}/{dataset_id}"
    cs_co_map.write.parquet(f"{cs_co_dir}/cs_co_map.parquet")
    logger.info(
        f"Saved CS-CO mapping for {dataset_id} to {cs_co_dir}/cs_co_map.parquet"
    )


def process_dataset(dataset_id):
    logger.info(f"Processing dataset {dataset_id}")
    Path(dataset_id).mkdir(parents=True, exist_ok=True)
    if (Path(dataset_id) / "po.parquet").exists():
        logger.info(f"PO files for {dataset_id} already exist")
        return
    nconfigs = get_dataset(dataset_id)
    get_pos(dataset_id, nconfigs)
    get_cos(dataset_id, nconfigs)
    cs_ids = get_configuration_sets(dataset_id)
    if cs_ids is not None:
        get_cs_co_mapping(dataset_id, cs_ids)


def sort_ids_by_nconfigs(id_file):
    ds_ids = [
        x["id"]
        for x in spark.table("ndb.`colabfit-prod`.prod.ds")
        .select("id", "nconfigurations")
        .sort("nconfigurations", ascending=False)
        .collect()
    ]
    with open(id_file, "r") as f:
        dataset_ids = [line.strip() for line in f.readlines()]
    return [id for id in ds_ids if id in dataset_ids]


def main(id_file):
    logger.info(f"Starting processing for dataset IDs from {id_file}")
    dataset_ids = sort_ids_by_nconfigs(id_file)
    for dataset_id in dataset_ids:
        process_dataset(dataset_id)


def main2(ds_ids):
    for dataset_id in ds_ids:
        process_dataset(dataset_id)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python recalculate_ds.py <dataset_id_file>")
        sys.exit(1)
    id_file = sys.argv[1]
    main(id_file)
