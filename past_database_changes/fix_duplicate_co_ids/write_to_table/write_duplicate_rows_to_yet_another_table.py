import os
from time import time

from pathlib import Path
from colabfit.tools.schema import config_schema
from colabfit.tools.utilities import spark_schema_to_arrow_schema
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from vastdb.session import Session

load_dotenv()
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("VAST_DB_ENDPOINT")
# n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
# if not n_cpus:
#     n_cpus = 1
# spark_ui_port = os.getenv("__SPARK_UI_PORT")
# print(spark_ui_port)
# jars = os.getenv("VASTDB_CONNECTOR_JARS")
# print(jars)
# SLURM_ARRAY_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
# SLURM_JOB_ID = int(os.getenv("SLURM_JOB_ID"))
spark = (
    SparkSession.builder.appName("colabfit_yet_another")
    # .master(f"local[{n_cpus}]")
    # .config("spark.ui.port", f"{spark_ui_port}")
    .getOrCreate()
)
session = Session(access=access, secret=secret, endpoint=endpoint)


bucket_name, schema_name, table_name = "colabfit.dev.co_wip".split(".")
new_table_name = "co_wip_duplicate_rows_yet_again"

start = time()
id_files = sorted(
    list(
        Path(
            "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/fix_duplicate_co_ids/write_to_table"  # noqa
        ).glob("*.csv")
    )
)
# id_file = id_files[SLURM_ARRAY_TASK_ID]
for id_file in id_files[1:]:
    with session.transaction() as tx:
        print(f"processing file {id_file}")
        ids = spark.read.csv(str(id_file), header=True)
        ids = [x["id"] for x in ids.collect()]
        table = tx.bucket(bucket_name).schema(schema_name).table(table_name)
        reader = table.select(
            predicate=table["id"].isin(ids),
            internal_row_id=False,
        )
        rec_batch = reader.read_all()
        if not spark.catalog.tableExists(f"ndb.colabfit.dev.{new_table_name}"):
            arrow_schema = spark_schema_to_arrow_schema(config_schema)
            sch = tx.bucket(bucket_name).schema(schema_name)
            sch.create_table(
                table_name=new_table_name,
                columns=arrow_schema,
            )
        new_table = tx.bucket(bucket_name).schema(schema_name).table(new_table_name)
        new_table.insert(rec_batch)

print(f"Time taken: {time() - start}")

print("Finished!")
