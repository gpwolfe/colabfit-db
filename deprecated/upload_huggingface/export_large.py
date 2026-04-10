import logging
import os
import sys
from ast import literal_eval
from pathlib import Path

import boto3
import numpy as np
import pyspark.sql.functions as sf
from botocore.exceptions import ClientError
from colabfit.tools.vast.schema import (
    config_schema,
    dataset_arr_schema,
    property_object_schema,
)
from colabfit.tools.vast.utilities import (
    str_to_arrayof_int,
    str_to_arrayof_str,
    str_to_nestedarrayof_double,
)
import pyarrow as pa
from dotenv import load_dotenv
from pyspark.sql import Row, SparkSession
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
from vastdb.session import Session
from colabfit.tools.vast.database import batched

with open("/scratch/gw2338/colabfit-tools/.env") as f:
    envvars = dict(x.strip().split("=") for x in f.readlines())

jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("colabfit")
    .config("spark.jars", jars)
    .config("spark.executor.memoryOverhead", "600")
    .config("spark.sql.shuffle.partitions", 16)
    .config("spark.driver.maxResultSize", 0)
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
# TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
access = envvars.get("SPARK_ID")
secret = envvars.get("SPARK_KEY")
endpoint = envvars.get("SPARK_ENDPOINT")
sess = Session(access=access, secret=secret, endpoint=endpoint)


logger = logging.getLogger(f"{__name__}")
logger.setLevel("INFO")
PO_ID_BATCH_SIZE = 5000
logger.info(f"PO_ID_BATCH_SIZE: {PO_ID_BATCH_SIZE}")


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


def read_md_partition(partition, config):
    s3_mgr = S3FileManager(
        bucket_name=config["bucket_dir"],
        access_id=config["access_key"],
        secret_key=config["access_secret"],
        endpoint_url=config["endpoint"],
    )

    def process_row(row):
        rowdict = row.asDict()
        try:
            if row["metadata_path"] is None:
                rowdict["metadata"] = None
            else:
                rowdict["metadata"] = s3_mgr.read_file(row["metadata_path"])
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                rowdict["metadata"] = None
            else:
                logger.info(f"Error reading {row['metadata_path']}: {str(e)}")
                rowdict["metadata"] = None
        return Row(**rowdict)

    return map(process_row, partition)


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


@sf.udf("array<boolean>")
def pbc(cell):
    return [any([any(x) for x in cell])] * 3


@sf.udf("array<int>")
def dimension_types(pbc):
    dim_types = np.array(pbc).astype(int).tolist()
    return dim_types


@sf.udf("int")
def nperiodic_dimensions(dim_types):
    return int(sum(dim_types))


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
    "chemical_formula_reduced",
    "chemical_formula_anonymous",
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


def get_session():
    load_dotenv()
    access = os.getenv("VAST_DB_ACCESS")
    secret = os.getenv("VAST_DB_SECRET")
    endpoint = os.getenv("VAST_DB_ENDPOINT")
    return Session(access=access, secret=secret, endpoint=endpoint)


def pa_table_slicer(table: pa.Table):
    num_rows = table.num_rows()
    batch_size = PO_ID_BATCH_SIZE
    num_batches = (num_rows + batch_size - 1) // batch_size
    for i in range(num_batches):
        start = i * batch_size
        end = min(num_rows, (i + 1) * batch_size)
        yield table.slice(start, end - start)


def save_po_ids(ds_id):
    tmp_file = f"/scratch/gw2338/tmp_po_ids_{ds_id}.txt"
    if os.path.exists(tmp_file):
        os.remove(tmp_file)
    sess = get_session()
    with sess.transaction() as tx:
        table = tx.bucket("colabfit-prod").schema("prod").table("po")
        reader = table.select(
            predicate=table["dataset_id"] == ds_id,
            columns=["id"],
            internal_row_id=False,
        )
        for batch in reader:
            po_ids = batch.column("id").to_pylist()
            with open(tmp_file, "a") as f:
                for po_id in po_ids:
                    f.write(f"{po_id}\n")
    return tmp_file


def get_po_ids_from_file(file_path):
    with open(file_path, "r") as f:
        po_ids = [line.strip() for line in f.readlines()]
    yield from batched(po_ids, PO_ID_BATCH_SIZE)


def get_pos(po_id_batch):
    session = get_session()
    with session.transaction() as tx:
        table = tx.bucket("colabfit-prod").schema("prod").table("po")
        reader = table.select(
            predicate=table["id"].isin(po_id_batch),
            columns=[col.name for col in property_object_schema],
        )
        pos = reader.read_all()
    pos = pos.select([field.name for field in property_object_schema])
    return pos


def get_cos(co_ids):
    session = get_session()
    with session.transaction() as tx:
        table = tx.bucket("colabfit-prod").schema("prod").table("co")
        reader = table.select(
            predicate=table["id"].isin(co_ids),
            columns=[col.name for col in config_schema],
            internal_row_id=False,
        )
        data = reader.read_all()
    data = (
        data.select([field.name for field in config_schema])
        .to_struct_array()
        .to_pandas()
    )
    # pandas coerces int cols with nulls to float (np.nan as float)
    # not interpretable as int by pyspark
    for x in data:
        if x["metadata_size"] is not None:
            x["metadata_size"] = int(x["metadata_size"])
        else:
            x["metadata_size"] = None
    df = spark.createDataFrame(data, config_schema)
    return df


def process_configs(cos):
    cos_arr = cos.select(
        [
            (
                str_to_nestedarrayof_double(sf.col(col)).alias(col)
                if col in co_nested_arr_cols
                else (
                    str_to_arrayof_int(sf.col(col)).alias(col)
                    if col in co_int_arr_cols
                    else (
                        str_to_arrayof_str(sf.col(col)).alias(col)
                        if col in co_str_arr_cols
                        else (
                            str_to_arrayof_double(sf.col(col)).alias(col)
                            if col in co_double_arr_cols
                            else (
                                str_to_arrayof_bool(sf.col(col)).alias(col)
                                if col in co_bool_arr_cols
                                else col
                            )
                        )
                    )
                )
            )
            for col in cos.columns
        ]
    )

    cos = cos_arr.withColumn("pbc", pbc(sf.col("cell")))
    cos = cos.withColumn("dimension_types", dimension_types(sf.col("pbc")))
    cos = cos.withColumn(
        "nperiodic_dimensions", nperiodic_dimensions(sf.col("dimension_types"))
    )

    cos = cos.rdd.mapPartitions(lambda partition: read_md_partition(partition, config))
    cos = cos.toDF(co_tmp_schema)
    return cos


def get_pos_df(batch):
    pos = spark.createDataFrame(
        batch.to_struct_array().to_pandas(), property_object_schema
    )
    pos = pos.select(
        [
            (
                col
                if col not in po_arr_cols
                else str_to_nestedarrayof_double(sf.col(col)).alias(col)
            )
            for col in po_tmp_schema2.fieldNames()
            if col != "metadata"
        ]
    )
    pos = pos.rdd.mapPartitions(lambda partition: read_md_partition(partition, config))
    pos = spark.createDataFrame(
        pos,
        po_tmp_schema2,
    )
    pos = pos.drop(*cols_to_drop_both)
    return pos


def get_podf_and_codf_from_po_batch(po_batch):
    coids = list(set(po_batch.column("configuration_id").to_pylist()))
    cos = get_cos(coids)
    cos = process_configs(cos)
    pos = get_pos_df(po_batch)
    return pos, cos


def merge_po_co_rows(pos, cos):
    pos = pos.select(
        [
            sf.col(col).alias(f"property_object_{col}") if col in cols_in_both else col
            for col in pos.columns
        ]
    )
    pos = pos.drop(*cols_duplicd)
    cos = cos.drop(*cols_to_drop_both)
    cos = cos.select(
        [
            (
                sf.col(col).alias(f"configuration_{col}")
                if col in cols_in_both + cols_to_prepend_config
                else col
            )
            for col in cos.columns
        ]
    )
    dimension_agg = cos.agg(
        sf.collect_set("nperiodic_dimensions").alias("dataset_nperiodic_dimensions"),
        sf.collect_set("dimension_types").alias("dataset_dimension_types"),
    ).collect()[0]
    cos = cos.withColumn(
        "dataset_nperiodic_dimensions",
        sf.lit(dimension_agg["dataset_nperiodic_dimensions"]),
    ).withColumn(
        "dataset_dimension_types", sf.lit(dimension_agg["dataset_dimension_types"])
    )
    rows = pos.join(cos, on="configuration_id", how="left")
    return rows


def get_dataset(dataset_df):
    dataset = dataset_df.select(
        [
            (
                str_to_arrayof_int(sf.col(col)).alias(col)
                if col in ds_int_arr_cols
                else (
                    str_to_arrayof_str(sf.col(col)).alias(col)
                    if col in ds_str_arr_cols
                    else (
                        str_to_arrayof_double(sf.col(col)).alias(col)
                        if col in ds_double_arr_cols
                        else (
                            str_to_nestedarrayof_int(sf.col(col)).alias(col)
                            if col in ds_nested_intarr_cols
                            else col
                        )
                    )
                )
            )
            for col in dataset_df.columns
        ]
    )
    dataset = dataset.drop("dimension_types", "nperiodic_dimensions")
    dataset = dataset.select(
        *[sf.col(col).alias(f"dataset_{col}") for col in dataset.columns]
    )
    return dataset


def generate_dataset_citation_string(item):
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
    logger.info(f"Dataset: {ds_row['name']}")
    if ds_row is None:
        logger.info(f"Dataset {ds_id} not found. Continuing.")
        return
    dataset_name = (
        ds_row["name"]
        .replace("@", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace("/", "_")
        .replace("+", "_")
    )
    dataset_dir = Path(f"datasets/{dataset_name}")
    dataset_dir.mkdir(exist_ok=True)
    if (dataset_dir / dataset_name).exists():
        logger.info(f"Dataset {dataset_name} already exists. Continuing.")
        return
    citation = generate_dataset_citation_string(ds_row)
    # ideal_partitions = max(4, ds_row["nconfigurations"] // 50000)
    # closest_2factor = 2 ** (round(log2(ideal_partitions)))

    ############################################
    # Get Property Objects
    ############################################
    dataset = get_dataset(dataset_df)
    dataset.cache()

    logger.info(f"dataset id: {ds_id}")
    po_tmp_file = save_po_ids(ds_id)
    po_id_batches = get_po_ids_from_file(po_tmp_file)
    for i, batch in enumerate(po_id_batches):
        pos_batch = get_pos(batch)
        pos, cos = get_podf_and_codf_from_po_batch(pos_batch)
        merged_po_co = merge_po_co_rows(pos, cos)
        merged_po_co_ds = merged_po_co.join(dataset, on="dataset_id", how="left")
        final_rows = merged_po_co_ds.select(*all_cols)
        final_rows.coalesce(1).write.mode("append").parquet(
            str(dataset_dir / dataset_name)
        )
        logger.info(f"--> Batch {i} written to parquet")

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
    dataset.unpersist()


def main(ds_id_file):
    with open(ds_id_file) as f:
        ds_ids = [x.strip() for x in f.readlines()]
    for ds_id in ds_ids:
        logger.info(f"Processing dataset id: {ds_id}")
        process_ds_id(ds_id)


if __name__ == "__main__":
    ds_id_file = sys.argv[1]
    logger.info(f"File: {ds_id_file}")
    main(ds_id_file)
