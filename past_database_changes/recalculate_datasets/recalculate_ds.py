import logging
import os
from ast import literal_eval
import sys

import pyspark
from colabfit.tools.vast.database import DataManager, VastDataLoader
from dotenv import load_dotenv
from pyspark.sql import SparkSession

logger = logging.getLogger(f"{__name__}.hasher")
logger.setLevel("INFO")

logging.info(f"pyspark version: {pyspark.__version__}")
load_dotenv()
SLURM_JOB_ID = os.getenv("SLURM_JOB_ID")


n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1
# spark_ui_port = os.getenv("__SPARK_UI_PORT")
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName(f"colabfit_{SLURM_JOB_ID}")
    .master(f"local[{n_cpus}]")
    .config("spark.executor.memoryOverhead", "600")
    .config("spark.jars", jars)
    .config("spark.ui.showConsoleProgress", "true")
    .config("spark.driver.maxResultSize", 0)
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
loader = VastDataLoader(
    spark_session=spark,
    access_key=os.getenv("SPARK_ID"),
    access_secret=os.getenv("SPARK_KEY"),
    endpoint=os.getenv("SPARK_ENDPOINT"),
)


loader.config_table = "ndb.`colabfit-prod`.prod.co"
loader.prop_object_table = "ndb.`colabfit-prod`.prod.po"
loader.config_set_table = "ndb.`colabfit-prod`.prod.cs"
loader.dataset_table = "ndb.colabfit.dev.ds_recalculated"
loader.co_cs_map_table = "ndb.`colabfit-prod`.prod.cs_co_map"


# def main(begin, end):
def main(id):
    dss = spark.table("ndb.`colabfit-prod`.prod.ds").sort("nconfigurations")
    # existing = spark.table("ndb.colabfit.dev.ds_recalculated").select("id")
    # dss = dss.join(existing, on="id", how="left_anti").select(
    #    "id",
    #    "name",
    #    "links",
    #    "authors",
    #    "description",
    #    "publication_year",
    #    "doi",
    #    "license",
    # )
    logger.info(f"dss count: {dss.count()}")

    # for ds in dss.collect()[begin:end]:
    # for ds in ["DS_jyuwhl30jklq_0"]:
    ds = dss.filter(f'id == "{id}"').collect()[0]
    logger.info(f"Processing dataset {ds['id']} - {ds['name']}")
    dsid = ds["id"]
    links = literal_eval(ds["links"])
    authors = literal_eval(ds["authors"])
    dm = DataManager(
        dataset_id=dsid,
    )

    dm.create_dataset(
        loader=loader,
        name=ds["name"],
        authors=authors,
        description=ds["description"],
        publication_link=links["source-publication"],
        data_link=links["source-data"],
        other_links=links.get("other", None),
        publication_year=ds["publication_year"],
        doi=ds["doi"],
        data_license=ds["license"],
    )
    logger.info(f"Dataset {ds['id']} - {ds['name']} complete")


if __name__ == "__main__":
    # if len(sys.argv) != 3:
    #     print("Usage: python recalculate_ds.py <beginning index> <ending index>")
    #     sys.exit(1)
    # begin = int(sys.argv[1])
    # end = int(sys.argv[2])
    # main(begin, end)
    id = sys.argv[1]
    main(id)
