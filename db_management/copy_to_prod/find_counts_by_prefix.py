import datetime
import os
from pathlib import Path
from tqdm import tqdm
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from vastdb.session import Session
import sys

import logging

logger = logging.getLogger(__name__)

with open("/scratch/gw2338/vast/data-lake-main/spark/scripts/.env") as f:
    envvars = dict(x.strip().split("=") for x in f.readlines())

jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("Test")
    .config("spark.jars", os.environ["VASTDB_CONNECTOR_JARS"])
    .config(
        "spark.sql.files.maxPartitionBytes", 134217728
    )  # 128MB per partition (optional tuning)
    .config("spark.sql.shuffle.partitions", 800)
    .getOrCreate()
)

access_key = envvars.get("SPARK_ID")
access_secret = envvars.get("SPARK_KEY")
endpoint = envvars.get("SPARK_ENDPOINT")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)


def get_fp_with_affix(fp: Path):
    base = fp.stem
    suffix = fp.suffix
    parent = fp.parent
    counter = 1

    candidate = fp
    while candidate.exists():
        if "_" in base and base.split("_")[-1].isdigit():
            base_no_num = "_".join(base.split("_")[:-1])
            candidate = parent / f"{base_no_num}_{counter}{suffix}"
        else:
            candidate = parent / f"{base}_{counter}{suffix}"
        counter += 1
    return candidate


def main(table):
    copytoprod_dir = Path(
        "/scratch/gw2338/vast/data-lake-main/spark/scripts/colabfit_db/copy_to_prod/"
    )
    copytoprod_dir.exists()
    letters = table[:2].upper()
    if letters not in ["PO", "CO"]:
        raise ValueError(f"Invalid table prefix: {letters}. Use 'PO' or 'CO'.")
    # prefixes = list(range(1000, 1350))
    # prefixes.extend(list(range(135, 1000)))
    # prefixes = [f"{letters}_{prefix}" for prefix in prefixes][start:]
    all_prefixes = [f"{letters}_{i:03d}" for i in range(100, 1000)]
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    fp = copytoprod_dir / f"count_by_id_prefix_{table}_{today}.csv"
    fp = get_fp_with_affix(fp)
    with open(fp, "a") as f:
        f.write("id,count\n")

    table = spark.table(f"ndb.`colabfit-prod`.prod.{table}")
    table = table.withColumn("prefix", sf.col("id").substr(1, 6)).drop("id")
    agg_by_first_4 = (
        table.groupBy(sf.col("prefix"))
        .agg(sf.count("*").alias("count"))
        .orderBy("prefix")
    )
    agg_dict = {row["prefix"]: row["count"] for row in agg_by_first_4.collect()}
    for prefix in all_prefixes:
        logger.info(f"Processing prefix: {prefix}")
        if agg_dict.get(prefix) is None:
            row = {"prefix": prefix, "count": 0}
        else:
            row = {"prefix": prefix, "count": agg_dict[prefix]}
        with open(fp, "a") as f:
            f.write(f"{row['prefix']},{row['count']}\n")


if __name__ == "__main__":

    if len(sys.argv) not in [2, 3]:
        print("Usage: python find_counts_by_prefix.py <table> [start]")
        sys.exit(1)
    table = sys.argv[1]
    table = table.lower()
    # start = int(sys.argv[2]) if len(sys.argv) == 3 else 0
    if table[:2] not in ["po", "co"]:
        print(f"Invalid table name: {table}. Please use 'po' or 'co' table.")
        sys.exit(1)
    main(table)
    spark.stop()
