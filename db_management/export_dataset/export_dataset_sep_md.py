from __future__ import print_function

import os
import sys
from datetime import datetime
from pathlib import Path
from ast import literal_eval
import pyspark.sql.functions as sf
from colabfit.tools.database import (
    # read_md_partition,
    S3FileManager,
)
from colabfit.tools.schema import (
    config_md_schema,
    configuration_set_arr_schema,
    dataset_df_schema,
    property_object_md_schema,
)
from colabfit.tools.utilities import unstring_df_val
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType
from botocore.exceptions import ClientError


def cols_to_arrays(*cols):
    arr_cols = []
    for col in cols:
        if col:
            parsed_array = literal_eval(col)
            arr_cols.append(parsed_array)
        else:
            arr_cols.append([])
    return tuple(arr_cols)


def get_unstrung_dfs(df, schema):
    array_cols = [f for f in schema if f.dataType.typeName() == "array"]
    names = [f.name for f in array_cols]
    array_cols_udf = sf.udf(cols_to_arrays, StructType([*array_cols]))
    df = df.withColumn("processed", array_cols_udf(*[df[name] for name in names]))
    df = df.select(
        [
            *[col for col in df.columns if ((col not in names) & (col != "processed"))]
            + [f"processed.{name}" for name in names]
        ]
    )
    return df


def read_md_partition(partition, config):
    s3_mgr = S3FileManager(
        bucket_name=config["bucket_dir"],
        access_id=config["access_key"],
        secret_key=config["access_secret"],
        endpoint_url=config["endpoint"],
    )

    def process_row(row):
        rowdict = row.asDict()
        rowdict = {key: rowdict[key] for key in ["metadata_id", "metadata_path"]}
        try:
            rowdict["metadata"] = s3_mgr.read_file(rowdict.pop("metadata_path"))
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                rowdict["metadata"] = None
            else:
                print(f"Error reading {row['metadata_path']}: {str(e)}")
                rowdict["metadata"] = None
        return Row(**rowdict)

    return map(process_row, partition)


def main(DATASET_ID):
    start = datetime.now()
    TABLE_PREFIX = "ndb.`colabfit-prod`.prod"
    BUCKET_DIR = "colabfit-data"
    load_dotenv()
    config = {
        "bucket_dir": BUCKET_DIR,
        "access_key": os.getenv("VAST_DB_ACCESS"),
        "access_secret": os.getenv("VAST_DB_SECRET"),
        "endpoint": os.getenv("VAST_DB_ENDPOINT"),
    }

    spark = SparkSession.builder.appName("ColabfitExportDataset").getOrCreate()
    print("dataset")
    ds_df = spark.read.table(f"{TABLE_PREFIX}.ds").filter(sf.col("id") == DATASET_ID)
    dataset_name = ds_df.select("name").first()["name"]
    OUTPUT_DIR = Path(
        f"/scratch/work/martiniani/for_gregory/colabfit-export/{DATASET_ID}_{dataset_name}_{datetime.now().strftime('%Y-%m-%d-%H%M')}"  # noqa
    )
    ds_df = get_unstrung_dfs(ds_df, dataset_df_schema)
    ds_df.write.parquet(str(Path(OUTPUT_DIR / "ds.parquet")))
    print("cs")
    cs_df = spark.read.table(f"{TABLE_PREFIX}.cs").filter(
        sf.col("dataset_id") == DATASET_ID
    )
    css_exist = cs_df.count() > 0
    if css_exist:
        cs_df = get_unstrung_dfs(cs_df, configuration_set_arr_schema)
        cs_df.write.parquet(str(Path(OUTPUT_DIR / "cs.parquet")))

    print("po")
    po_df = spark.read.table(f"{TABLE_PREFIX}.po").filter(
        sf.col("dataset_id") == DATASET_ID
    )
    po_df.write.parquet(str(Path(OUTPUT_DIR / "po.parquet")))
    po_md_df = (
        po_df.select(["metadata_path", "metadata_id"])
        .dropDuplicates()
        .rdd.mapPartitions(lambda partition: read_md_partition(partition, config))
        .toDF()
    )
    # schema_type_dict = {f.name: f.dataType for f in property_object_md_schema}
    # array_cols = [
    #     f.name for f in property_object_md_schema if f.dataType.typeName() == "array"
    # ]
    po_df = get_unstrung_dfs(po_df, property_object_md_schema)
    # for col in array_cols:
    #     array_col_udf = sf.udf(unstring_df_val, schema_type_dict[col])
    #     po_df = po_df.withColumn(col, array_col_udf(sf.col(col)))
    if po_md_df.count() > 0:
        po_df = po_df.join(po_md_df, on="metadata_id", how="left")
    # po_md_df.write.parquet(str(Path(OUTPUT_DIR / "po_md.parquet")))

    print("co")
    co_df = spark.read.table(f"{TABLE_PREFIX}.co").filter(
        sf.col("dataset_ids").contains(DATASET_ID)
    )

    co_md_df = (
        co_df.select(["metadata_path", "metadata_id"])
        .dropDuplicates()
        .rdd.mapPartitions(lambda partition: read_md_partition(partition, config))
        .toDF()
    )
    # co_md_df.write.parquet(str(Path(OUTPUT_DIR / "co_md.parquet")))
    # schema_type_dict = {f.name: f.dataType for f in config_md_schema}
    # array_cols = [f.name for f in config_md_schema if f.dataType.typeName() == "array"]
    # for col in array_cols:
    #     array_col_udf = sf.udf(unstring_df_val, schema_type_dict[col])
    #     co_df = co_df.withColumn(col, array_col_udf(sf.col(col)))
    co_df = get_unstrung_dfs(co_df, config_md_schema)
    if co_md_df.count() > 0:
        co_df = co_df.join(co_md_df, on="metadata_id", how="left")
    co_df.write.parquet(str(Path(OUTPUT_DIR / "co.parquet")))

    end = datetime.now()
    print(f"time taken: {str(end - start)}")
    print(OUTPUT_DIR.absolute())


if __name__ == "__main__":
    ds_id = sys.argv[1]
    main(ds_id)
