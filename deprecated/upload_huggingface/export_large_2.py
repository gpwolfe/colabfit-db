import logging
import os
import sys
from ast import literal_eval
from pathlib import Path

import boto3
import pyspark.sql.functions as sf
from botocore.exceptions import ClientError
from colabfit.tools.vast.schema import (
    dataset_arr_schema,
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
    StructField,
    StructType,
    TimestampType,
)
from vastdb.config import QueryConfig

config = QueryConfig(
    limit_rows_per_sub_split=10_000,
    rows_per_split=1_000_000,
)
# from pyspark import StorageLevel

with open("/scratch/gw2338/colabfit-tools/.env") as f:
    envvars = dict(x.strip().split("=") for x in f.readlines())

jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("colabfit")
    .config("spark.jars", jars)
    .config("spark.executor.memoryOverhead", "600")
    .config("spark.sql.shuffle.partitions", 4000)
    .config("spark.driver.maxResultSize", 0)
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
# TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
access = envvars.get("SPARK_ID")
secret = envvars.get("SPARK_KEY")
endpoint = envvars.get("SPARK_ENDPOINT")


logger = logging.getLogger(f"{__name__}")
logger.setLevel("INFO")
# PO_ID_BATCH_SIZE = 5000
# logger.info(f"PO_ID_BATCH_SIZE: {PO_ID_BATCH_SIZE}")


############################################
# Metadata Reader
############################################
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


############################################
# Optimized Column Transformation Helpers
############################################
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


############################################
# UDFs
############################################
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


po_tmp_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("hash", StringType(), True),
        StructField("last_modified", TimestampType(), True),
        StructField("configuration_id", StringType(), True),
        StructField("dataset_id", StringType(), True),
        StructField("multiplicity", IntegerType(), True),
        StructField("metadata_id", StringType(), True),
        StructField("metadata_path", StringType(), True),
        StructField("metadata_size", IntegerType(), True),
        StructField("software", StringType(), True),
        StructField("method", StringType(), True),
        StructField("chemical_formula_hill", StringType(), True),
        StructField("energy", DoubleType(), True),
        StructField("atomic_forces", ArrayType(ArrayType(DoubleType())), True),
        StructField("cauchy_stress", ArrayType(ArrayType(DoubleType())), True),
        StructField("cauchy_stress_volume_normalized", BooleanType(), True),
        StructField("electronic_band_gap", DoubleType(), True),
        StructField("electronic_band_gap_type", StringType(), True),
        StructField("energy_above_hull", DoubleType(), True),
        StructField("formation_energy", DoubleType(), True),
        StructField("adsorption_energy", DoubleType(), True),
        StructField("atomization_energy", DoubleType(), True),
        StructField("max_force_norm", DoubleType(), True),
        StructField("mean_force_norm", DoubleType(), True),
        StructField("metadata", StringType(), True),
    ]
)
po_export_schema = StructType(
    [
        field
        for field in po_tmp_schema
        if field.name not in ["metadata_size", "metadata_path", "metadata"]
    ]
)
po_export_schema.add(StructField("property_object_metadata", StringType(), True))
po_arr_cols = [
    field.name for field in po_export_schema if field.dataType.typeName() == "array"
]
po_tmp_schema2 = StructType(
    [
        StructField("id", StringType(), True),
        StructField("hash", StringType(), True),
        StructField("last_modified", TimestampType(), True),
        StructField("configuration_id", StringType(), True),
        StructField("dataset_id", StringType(), True),
        StructField("multiplicity", IntegerType(), True),
        StructField("metadata_id", StringType(), True),
        StructField("metadata_path", StringType(), True),
        StructField("metadata_size", IntegerType(), True),
        StructField("software", StringType(), True),
        StructField("method", StringType(), True),
        StructField("chemical_formula_hill", StringType(), True),
        StructField("energy", DoubleType(), True),
        StructField("atomic_forces", StringType(), True),
        StructField("cauchy_stress", StringType(), True),
        StructField("cauchy_stress_volume_normalized", BooleanType(), True),
        StructField("electronic_band_gap", DoubleType(), True),
        StructField("electronic_band_gap_type", StringType(), True),
        StructField("energy_above_hull", DoubleType(), True),
        StructField("formation_energy", DoubleType(), True),
        StructField("adsorption_energy", DoubleType(), True),
        StructField("atomization_energy", DoubleType(), True),
        StructField("max_force_norm", DoubleType(), True),
        StructField("mean_force_norm", DoubleType(), True),
        StructField("metadata", StringType(), True),
    ]
)
co_tmp_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("hash", StringType(), True),
        StructField("last_modified", TimestampType(), True),
        StructField("dataset_ids", ArrayType(StringType()), True),
        StructField("chemical_formula_hill", StringType(), True),
        StructField("chemical_formula_reduced", StringType(), True),
        StructField("chemical_formula_anonymous", StringType(), True),
        StructField("elements", ArrayType(StringType()), True),
        StructField("elements_ratios", ArrayType(DoubleType()), True),
        StructField("atomic_numbers", ArrayType(IntegerType()), True),
        StructField("nsites", IntegerType(), True),
        StructField("nelements", IntegerType(), True),
        StructField("nperiodic_dimensions", IntegerType(), True),
        StructField("cell", ArrayType(ArrayType(DoubleType())), True),
        StructField("dimension_types", ArrayType(IntegerType()), True),
        StructField("pbc", ArrayType(BooleanType()), True),
        StructField("names", ArrayType(StringType()), True),
        StructField("labels", ArrayType(StringType()), True),
        StructField("metadata_id", StringType(), True),
        StructField("metadata_path", StringType(), True),
        StructField("metadata_size", IntegerType(), True),
        StructField("structure_hash", StringType(), True),
        StructField("positions", ArrayType(ArrayType(DoubleType())), True),
        StructField("metadata", StringType(), True),
    ]
)
co_export_schema = StructType(
    [
        field
        for field in co_tmp_schema
        if field.name not in ["metadata_size", "metadata_path", "metadata"]
    ]
)
co_export_schema.add(StructField("configuration_metadata", StringType(), True))
co_nested_arr_cols = [
    col.name
    for col in co_export_schema
    if col.simpleString().split(":")[1] == "array<array<double>>"
]
co_double_arr_cols = [
    col.name
    for col in co_export_schema
    if col.simpleString().split(":")[1] == "array<double>"
]
co_str_arr_cols = [
    col.name
    for col in co_export_schema
    if col.simpleString().split(":")[1] == "array<string>"
]
co_int_arr_cols = [
    col.name
    for col in co_export_schema
    if col.simpleString().split(":")[1] == "array<int>"
]
co_bool_arr_cols = [
    col.name
    for col in co_export_schema
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

cols_to_drop_both = ["metadata_path", "metadata_size"]

cols_in_both = ["hash", "id", "last_modified", "metadata", "metadata_id"]
cols_duplicd = [
    "chemical_formula_hill",
    # "chemical_formula_reduced",
    # "chemical_formula_anonymous",
]
cols_to_prepend_config = [
    "labels",
    "names",
    "dataset_ids",
]

row_cols_in_order = [
    "chemical_formula_hill",
    "chemical_formula_reduced",
    "chemical_formula_anonymous",
    "atomic_numbers",
    "elements",
    "elements_ratios",
    "nelements",
    "nsites",
    "cell",
    "positions",
    "pbc",
    "dimension_types",
    "nperiodic_dimensions",
    "structure_hash",
    "multiplicity",
    "software",
    "method",
    "adsorption_energy",
    "atomic_forces",
    "atomization_energy",
    "cauchy_stress",
    "cauchy_stress_volume_normalized",
    "electronic_band_gap",
    "electronic_band_gap_type",
    "energy",
    "formation_energy",
    "max_force_norm",
    "mean_force_norm",
    "property_object_metadata",
    "property_object_metadata_id",
    "property_object_last_modified",
    "property_object_hash",
    "property_object_id",
    "configuration_metadata",
    "configuration_metadata_id",
    "configuration_labels",
    "configuration_names",
    "configuration_dataset_ids",
    "configuration_last_modified",
    "configuration_hash",
    "configuration_id",
]

dataset_cols_in_order = [
    "dataset_name",
    "dataset_authors",
    "dataset_description",
    "dataset_elements",
    "dataset_nelements",
    "dataset_nproperty_objects",
    "dataset_nconfigurations",
    "dataset_nsites",
    "dataset_adsorption_energy_count",
    "dataset_atomic_forces_count",
    "dataset_atomization_energy_count",
    "dataset_cauchy_stress_count",
    "dataset_electronic_band_gap_count",
    "dataset_energy_count",
    "dataset_energy_mean",
    "dataset_energy_variance",
    "dataset_formation_energy_count",
    "dataset_last_modified",
    "dataset_dimension_types",
    "dataset_nperiodic_dimensions",
    "dataset_publication_year",
    "dataset_total_elements_ratios",
    "dataset_license",
    "dataset_links",
    "dataset_doi",
    "dataset_hash",
    "dataset_id",
    "dataset_extended_id",
]
all_cols = row_cols_in_order + dataset_cols_in_order


# def get_session():
#     load_dotenv()
#     access = os.getenv("VAST_DB_ACCESS")
#     secret = os.getenv("VAST_DB_SECRET")
#     endpoint = os.getenv("VAST_DB_ENDPOINT")
#     return Session(access=access, secret=secret, endpoint=endpoint)


def get_cos(dataset_id):
    co_columns = [col for col in spark.table("ndb.`colabfit-prod`.prod.co").columns]
    logger.info(f"co_columns: {co_columns}")

    co_type_map = {
        "nested_double": co_nested_arr_cols,
        "int_array": co_int_arr_cols,
        "str_array": co_str_arr_cols,
        "double_array": co_double_arr_cols,
        "bool_array": co_bool_arr_cols,
    }

    transformed_cols = [
        create_column_transformer(col, co_type_map) for col in co_columns
    ]

    cos = (
        spark.table("ndb.`colabfit-prod`.prod.co")
        .filter(sf.col("dataset_ids").contains(dataset_id))
        .select(transformed_cols)
    )

    metadata_udf = create_metadata_reader_udf(config)
    cos = cos.withColumn("metadata", metadata_udf(sf.col("metadata_path")))

    special_cols = set(cols_in_both + cols_to_prepend_config)
    renamed_cols = [
        create_column_renamer(col, "configuration", special_cols)
        for col in cos.columns
        if col not in cols_to_drop_both
    ]

    cos = cos.select(renamed_cols)

    cos = cos.coalesce(200)

    return cos


def get_pos(dataset_id):
    logger.info(f"Processing property objects for dataset {dataset_id}")
    po_type_map = {
        "nested_double": po_arr_cols,
    }
    pos_columns = spark.table("ndb.`colabfit-prod`.prod.po").columns
    transformed_cols = [
        create_column_transformer(col, po_type_map)
        for col in po_tmp_schema2.fieldNames()
        if (col != "metadata" and col in pos_columns)
    ]
    pos = (
        spark.table("ndb.`colabfit-prod`.prod.po")
        .filter(sf.col("dataset_id") == dataset_id)
        .select(transformed_cols)
    )

    metadata_udf = create_metadata_reader_udf(config)
    pos = pos.withColumn("metadata", metadata_udf(sf.col("metadata_path")))
    logger.info(f"pos columns: {pos.columns}")
    special_cols = set(cols_in_both)
    renamed_cols = [
        create_column_renamer(col, "property_object", special_cols)
        for col in pos.columns
        if col not in cols_to_drop_both
    ]

    pos = pos.select(renamed_cols)
    logger.info(f"final pos columns: {pos.columns}")
    return pos


def merge_po_co_rows(pos, cos):
    existing_duplicated = [col for col in cols_duplicd if col in cos.columns]
    cos = cos.drop(*existing_duplicated).withColumnRenamed("id", "configuration_id")
    rows = pos.join(cos, on="configuration_id", how="left")
    return rows


def get_dataset(dataset_df):
    """Optimized dataset processing"""
    logger.info("Get dataset")
    # Create column type mapping for efficient transformations
    ds_type_map = {
        "int_array": ds_int_arr_cols,
        "str_array": ds_str_arr_cols,
        "double_array": ds_double_arr_cols,
        "nested_int": ds_nested_intarr_cols,
    }

    # Apply transformations in one pass
    transformed_cols = [
        create_column_transformer(col, ds_type_map) for col in dataset_df.columns
    ]

    dataset = dataset_df.select(transformed_cols)

    # Rename all columns with dataset prefix in one operation
    dataset = dataset.select(
        *[sf.col(col).alias(f"dataset_{col}") for col in dataset.columns]
    )
    return dataset


def generate_dataset_citation_string(item):
    logger.info(f"Generating citation for dataset {item['id']}")
    joined_names_string = None
    joined_names = []

    for author in literal_eval(item["authors"]):
        name_parts_orig = author.split(" ")
        name_parts_new = []
        family_name = name_parts_orig.pop()
        for name_part in name_parts_orig:
            # skip name parts that start as lower case
            if name_part[0].islower():
                continue
            s = name_part[0] + "."
            name_parts_new.append(s)

        formatted_name = family_name + ", " + " ".join(name_parts_new)
        joined_names.append(formatted_name)

    if len(joined_names) > 1:
        joined_names[-1] = "and " + joined_names[-1]

    joined_names_string = ", ".join(joined_names)
    item_name_converted = item["name"].replace("_", " ")
    citation_string = f"{joined_names_string} _{item_name_converted}_. ColabFit, {item['publication_year']}. https://doi.org/{item['doi']}"  # noqa: E501
    return citation_string


def process_ds_id(ds_id):
    ############################################
    # Set up directories, dataset variables
    ############################################
    dataset_df = spark.table("ndb.`colabfit-prod`.prod.ds").filter(
        sf.col("id") == ds_id
    )
    ds_row = dataset_df.first()

    if ds_row is None:
        logger.info(f"Dataset {ds_id} not found. Continuing.")
        return

    logger.info(f"Dataset: {ds_row['name']}")

    dataset_name = (
        ds_row["name"]
        .replace("@", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace("/", "_")
        .replace("+", "_")
    )
    dataset_dir = Path(f"datasets_extralarge/{dataset_name}")
    dataset_dir.mkdir(exist_ok=True)
    if (dataset_dir / dataset_name).exists():
        logger.info(f"Dataset {dataset_name} already exists. Continuing.")
        return
    citation = generate_dataset_citation_string(ds_row)

    logger.info(f"Processing dataset id: {ds_id}")

    dataset = get_dataset(dataset_df)
    dataset.cache()

    cos = get_cos(dataset_id=ds_id)

    pos = get_pos(dataset_id=ds_id)
    merged_po_co = merge_po_co_rows(pos, cos)
    logger.info("merged co po rows")
    merged_po_co_ds = merged_po_co.join(dataset, on="dataset_id", how="left")
    logger.info("joined co po ds")
    logger.info(f"merged_po_co_ds columns: {merged_po_co_ds.columns}")
    final_rows = merged_po_co_ds.select(*all_cols)
    logger.info("final rows selected")

    # Optimize partitioning based on dataset size
    nconfigs = ds_row["nconfigurations"]
    if nconfigs > 100000:
        final_rows = final_rows.repartition(max(8, nconfigs // 50000))
    elif nconfigs > 10000:
        final_rows = final_rows.repartition(4)
    else:
        final_rows = final_rows.coalesce(2)

    # Write to parquet with optimized settings
    final_rows.write.mode("append").option("compression", "snappy").parquet(
        str(dataset_dir / dataset_name / "main")
    )

    logger.info(f"Finished writing {dataset_name}, {ds_id} to parquet")
    ############################################
    # Create dataset card
    ############################################
    elements = ", ".join(literal_eval(ds_row["elements"]))
    dslicense = ds_row["license"]
    links = literal_eval(ds_row["links"])
    properties_cols = ", ".join(
        [
            col.replace("_count", "").replace("_", " ")
            for col in ds_row.asDict().keys()
            if col.endswith("count") and ds_row[col] > 0
        ]
    )

    text = f'---\nconfigs:\n- config_name: default\n  data_files: "main/*.parquet"\nlicense: {dslicense.lower().replace("-only", "")}\ntags:\n- molecular dynamics\n- mlip\n- interatomic potential\npretty_name: {ds_row["name"].replace("_", " ")}\n---\n'  # noqa: E501
    text += f"### Cite this dataset  \n{citation}\n  "
    text += f"### View on the ColabFit Exchange  \nhttps://materials.colabfit.org/id/{ds_row['id']}  \n"  # noqa: E501
    text += f"# Dataset  Name  \n{ds_row['name'].replace('_', ' ')}  \n"
    text += f"### Description  \n{ds_row['description']}  \n<br>"
    text += (
        'Additional details stored in dataset columns prepended with "dataset_".  \n'
    )
    text += f"### Dataset authors  \n{', '.join(literal_eval(ds_row['authors']))}  \n"
    if links["source-publication"] is not None:
        text += f"### Publication  \n{links['source-publication']}  \n"
    if links["source-data"] is not None:
        text += f"### Original data link \n{links['source-data']}  \n"
    text += f"### License  \n{dslicense}  \n"
    text += f"### Number of unique molecular configurations  \n{ds_row['nconfigurations']}  \n"  # noqa: E501
    text += f"### Number of atoms  \n{ds_row['nsites']}  \n"
    text += f"### Elements included  \n{elements}  \n"
    text += f"### Properties included  \n{properties_cols}  \n"

    with open(dataset_dir / "README.md", "w") as f:
        f.write(text)
    logger.info("README written")
    logger.info("Done!")


def main(ds_id_file):
    with open(ds_id_file) as f:
        ds_ids = [x.strip().split(",")[0] for x in f.readlines()][1:]
        ds_ids.reverse()

    logger.info(f"Processing {len(ds_ids)} datasets")

    for i, ds_id in enumerate(ds_ids, 1):
        logger.info(f"Processing dataset {i}/{len(ds_ids)}: {ds_id}")
        try:
            process_ds_id(ds_id)
        except Exception as e:
            logger.error(f"Error processing dataset {ds_id}: {str(e)}")
            continue

        # Force garbage collection periodically for large batches
        # if i % 10 == 0:
        spark.catalog.clearCache()
        logger.info(f"Cleared cache after processing {i} datasets")

    logger.info("All datasets processed successfully")


if __name__ == "__main__":
    ds_id_file = sys.argv[1]
    logger.info(f"File: {ds_id_file}")
    main(ds_id_file)
