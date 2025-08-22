import os
from time import time, sleep

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from vastdb.session import Session
from tqdm import tqdm

from colabfit.tools.vast.schema import property_object_schema
from colabfit.tools.vast.utilities import spark_schema_to_arrow_schema

load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
session = Session(access=access, secret=secret, endpoint=endpoint)

jars = os.getenv("VASTDB_CONNECTOR_JARS")

spark = (
    SparkSession.builder.appName("copy_to_dev").config("spark.jars", jars).getOrCreate()
)

ACTUAL_INDEX = int(os.getenv("SLURM_ARRAY_TASK_ID"))

if __name__ == "__main__":

    SRC = "colabfit.dev.po_wip"
    DEST = "colabfit-prod.prod.po_tmp1"
    print(f"Copying {SRC} to {DEST}")
    sleep(max(ACTUAL_INDEX % 50, 10))
    start = time()

    prefix = f"PO_{ACTUAL_INDEX:02}"
    arrow_schema = spark_schema_to_arrow_schema(property_object_schema)
    if ACTUAL_INDEX == 10:
        if not spark.catalog.tableExists("ndb.`colabfit-prod`.prod.po_tmp1"):
            with session.transaction() as tx:
                print(f"Creating table {DEST}")
                dest_schema = tx.bucket("colabfit-prod").schema("prod")
                dest_schema.create_table("po_tmp1", arrow_schema)
    else:
        while not spark.catalog.tableExists("ndb.`colabfit-prod`.prod.po_tmp1"):
            sleep(5)
            print("Waiting for table to be created")
        print("Table has been created")
    with session.transaction() as tx:
        dest_path = DEST.split(".")
        dest_table = tx.bucket(dest_path[0]).schema(dest_path[1]).table(dest_path[2])
        src_path = SRC.split(".")
        src_table = tx.bucket(src_path[0]).schema(src_path[1]).table(src_path[2])
        src_reader = src_table.select(predicate=src_table["id"].startswith(prefix))
        # Copy
        num_batches = 0
        for batch in tqdm(iter(src_reader.read_next_batch, None)):
            dest_table.insert(batch)
            num_batches += 1

        print(f"Copied {SRC} to {DEST}")
        print(f"Total batches: {num_batches}")
        print("complete!")
        print(f"Total runtime: {time() - start}")
    if ACTUAL_INDEX == 99:
        sorted_columns = ["dataset_id"]
        unsorted_columns = ["id"]
        with session.transaction() as tx:
            table = tx.bucket("colabfit-prod").schema("prod").table("po_tmp1")
            table.create_projection(
                projection_name="po-dataset_id",
                sorted_columns=sorted_columns,
                unsorted_columns=unsorted_columns,
            )

        sorted_columns = ["configuration_id"]
        unsorted_columns = ["id"]
        with session.transaction() as tx:
            table = tx.bucket("colabfit-prod").schema("prod").table("po_tmp1")
            table.create_projection(
                projection_name="po-configuration_id",
                sorted_columns=sorted_columns,
                unsorted_columns=unsorted_columns,
            )

        sorted_columns = ["id"]
        unsorted_columns = [
            col
            for col in property_object_schema.fieldNames()
            if col not in sorted_columns
        ]
        with session.transaction() as tx:
            table = tx.bucket("colabfit-prod").schema("prod").table("po_tmp1")
            table.create_projection(
                projection_name="po-id-all",
                sorted_columns=sorted_columns,
                unsorted_columns=unsorted_columns,
            )
            print(table.projections())

    spark.stop()

# load_dotenv()
# endpoint = os.getenv("VAST_DB_ENDPOINT")
# access = os.getenv("VAST_DB_ACCESS")
# secret = os.getenv("VAST_DB_SECRET")
# sess = Session(access=access, secret=secret, endpoint=endpoint)

# spark = SparkSession.builder.appName("copy_to_dev").getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")

# begin = time()

# po_table = "ndb.colabfit.dev.po_wip"
# to_po_table = "ndb.`colabfit-prod`.prod.po_tmp1"

# print("loading pos")
# pos = spark.table(po_table)
# print("writing pos to table")
# pos.printSchema()
# pos.write.mode("errorifexists").saveAsTable(to_po_table,
# schema=property_object_schema)

# Set up projections
