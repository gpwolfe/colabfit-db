from __future__ import print_function

from datetime import datetime
import shutil
import sys

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

if __name__ == "__main__":

    start = datetime.now()

    if len(sys.argv) != 2:
        print(
            "Usage: export_from_query <dataset_id>",
            file=sys.stderr,
        )
        exit(-1)

    # TABLE_PREFIX = sys.argv[1]  # Ex: ndb.colabfit.dev
    TABLE_PREFIX = "ndb.`colabfit-prod`.prod"
    # BUCKET_DIR = sys.argv[2]  # Ex: /vdev/colabfit-data/data
    BUCKET_DIR = "/vdev/colabfit-data/data"
    DATASET_ID = sys.argv[1]  # Ex: DS_1109229269864121652581370
    OUTPUT_DIR = Path(
        f"/scratch/work/martiniani/for_gregory/colabfit-export/{DATASET_ID}_{datetime.now().strftime('%Y-%m-%d-%H%M')}"
    )

    spark = SparkSession.builder.appName("ColabfitExportDataset").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Query tables
    # TODO: This can take a more flexible query based on user input
    ds_df = spark.read.table(f"{TABLE_PREFIX}.ds").filter(col("id") == DATASET_ID)
    cs_df = spark.read.table(f"{TABLE_PREFIX}.cs").filter(
        col("dataset_id") == DATASET_ID
    )
    co_df = spark.read.table(f"{TABLE_PREFIX}.co").filter(
        col("dataset_ids").contains(DATASET_ID)
    )
    po_df = spark.read.table(f"{TABLE_PREFIX}.po").filter(
        col("dataset_ids").contains(DATASET_ID)
    )

    # Copy data files and update paths in the tables
    # os.makedirs(os.path.join(OUTPUT_DIR, "data"))
    (OUTPUT_DIR / "data").mkdir(parents=True, exist_ok=True)

    @udf(returnType=StringType())
    def copy_raw_data_file(file_path):
        file_path = Path(file_path)
        # if not os.path.isfile(file_path):
        if not file_path.is_file():
            raise FileNotFoundError(f"Data file not found: {file_path}")
        rel_path = Path(str(file_path).replace(BUCKET_DIR, ""))
        # dest = os.path.join(OUTPUT_DIR, "data", rel_path)
        dest = OUTPUT_DIR / "data" / rel_path
        # os.makedirs(os.path.dirname(dest), exist_ok=True)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(file_path, dest)
        # return os.path.join("data", rel_path)
        return str(dest)

    # co_df = co_df.withColumn("positions", copy_raw_data_file(co_df.positions))
    co_df = co_df.withColumn("metadata", copy_raw_data_file(co_df.metadata))
    # po_df = po_df.withColumn("atomic_forces", copy_raw_data_file(co_df.atomic_forces))

    # Zip raw data files
    shutil.make_archive(
        OUTPUT_DIR / "data",
        "zip",
        OUTPUT_DIR / "data",
    )

    # Write parquet files
    ds_df.write.parquet(Path(OUTPUT_DIR / "ds.parquet"))
    cs_df.write.parquet(Path(OUTPUT_DIR / "cs.parquet"))
    co_df.write.parquet(Path(OUTPUT_DIR / "co.parquet"))
    po_df.write.parquet(Path(OUTPUT_DIR / "po.parquet"))

    end = datetime.now()
    print(f"time taken: {str(end - start)}")
    print(OUTPUT_DIR.absolute())
