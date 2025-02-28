from __future__ import print_function

import os
import sys
from datetime import datetime
from pathlib import Path

import pyspark.sql.functions as sf
from colabfit.tools.database import read_md_partition
from colabfit.tools.schema import (
    config_md_schema,
    configuration_set_df_schema,
    dataset_df_schema,
    property_object_md_schema,
)
from colabfit.tools.utilities import unstring_df_val
from dotenv import load_dotenv
from pyspark.sql import SparkSession


def get_unstrung_dfs(df, schema):
    schema_type_dict = {f.name: f.dataType for f in schema}
    array_cols = [f.name for f in schema if f.dataType.typeName() == "array"]
    for col in array_cols:
        array_col_udf = sf.udf(unstring_df_val, schema_type_dict[col])
        df = df.withColumn(col, array_col_udf(sf.col(col)))
    return df


def main(DATASET_ID):
    start = datetime.now()

    # TABLE_PREFIX = sys.argv[1]  # Ex: ndb.colabfit.dev
    TABLE_PREFIX = "ndb.`colabfit-prod`.prod"
    # BUCKET_DIR = sys.argv[2]  # Ex: /vdev/colabfit-data/data
    BUCKET_DIR = "colabfit-data"
    # DATASET_ID = sys.argv[1]  # Ex: DS_1109229269864121652581370

    load_dotenv()
    config = {
        "bucket_dir": BUCKET_DIR,
        "access_key": os.getenv("VAST_DB_ACCESS"),
        "access_secret": os.getenv("VAST_DB_SECRET"),
        "endpoint": os.getenv("VAST_DB_ENDPOINT"),
    }

    spark = SparkSession.builder.appName("ColabfitExportDataset").getOrCreate()
    # spark.sparkContext.setLogLevel("WARN")
    print("dataset")
    ds_df = spark.read.table(f"{TABLE_PREFIX}.ds").filter(sf.col("id") == DATASET_ID)
    dataset_name = ds_df.select("name").first()["name"]
    OUTPUT_DIR = Path(
        f"/scratch/work/martiniani/for_gregory/colabfit-export/{DATASET_ID}_{dataset_name}_{datetime.now().strftime('%Y-%m-%d-%H%M')}"  # noqa
    )
    (OUTPUT_DIR / "data").mkdir(parents=True, exist_ok=True)
    ds_df = get_unstrung_dfs(ds_df, dataset_df_schema)
    ds_df.write.parquet(str(Path(OUTPUT_DIR / "ds.parquet")))
    print("cs")
    cs_df = spark.read.table(f"{TABLE_PREFIX}.cs").filter(
        sf.col("dataset_id") == DATASET_ID
    )
    css_exist = cs_df.count() > 0
    if css_exist:
        cs_df = get_unstrung_dfs(cs_df, configuration_set_df_schema)
        cs_df.write.parquet(str(Path(OUTPUT_DIR / "cs.parquet")))

    print("co")
    co_df = spark.read.table(f"{TABLE_PREFIX}.co").filter(
        sf.col("dataset_ids").contains(DATASET_ID)
    )
    co_df = get_unstrung_dfs(co_df, config_md_schema)
    co_df = co_df.rdd.mapPartitions(
        lambda partition: read_md_partition(partition, config)
    ).toDF(config_md_schema)
    co_df.write.parquet(str(Path(OUTPUT_DIR / "co.parquet")))

    print("po")
    po_df = spark.read.table(f"{TABLE_PREFIX}.po").filter(
        sf.col("dataset_id") == DATASET_ID
    )
    po_df = get_unstrung_dfs(po_df, property_object_md_schema)
    po_df = po_df.rdd.mapPartitions(
        lambda partition: read_md_partition(partition, config)
    ).toDF(property_object_md_schema)
    po_df.write.parquet(str(Path(OUTPUT_DIR / "po.parquet")))

    end = datetime.now()
    print(f"time taken: {str(end - start)}")
    print(OUTPUT_DIR.absolute())


if __name__ == "__main__":
    ds_id = sys.argv[1]
    main(ds_id)
