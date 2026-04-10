import logging
import os
import sys
from multiprocessing import Pool
from time import time

import pyarrow as pa
from colabfit.tools.vast.schema import config_prop_schema
from colabfit.tools.vast.utilities import spark_schema_to_arrow_schema
from dotenv import load_dotenv
from vastdb.session import Session

MISSING = [
    433,
    434,
    446,
    447,
    459,
    460,
    472,
    473,
    485,
    486,
    498,
    499,
    511,
    512,
    524,
    525,
    537,
    538,
    550,
    551,
    563,
    564,
    576,
    577,
    589,
    590,
    602,
    603,
    615,
    616,
    628,
    629,
    641,
    642,
    654,
    655,
    667,
    668,
    679,
    680,
    681,
    692,
    693,
    694,
    705,
    706,
    707,
    718,
    719,
    720,
    725,
    726,
    727,
    728,
    729,
    730,
    731,
    732,
    733,
    734,
    735,
    736,
    737,
    738,
    739,
    740,
    741,
    742,
    743,
    744,
    745,
    746,
    747,
    748,
    749,
    750,
    751,
    752,
    753,
    754,
    755,
    756,
    757,
    758,
    759,
    760,
    761,
    762,
    763,
    764,
    765,
    766,
    767,
    768,
    769,
    770,
    771,
    772,
    773,
    774,
    775,
    776,
    777,
    778,
    779,
    780,
    781,
    782,
    783,
    784,
    785,
    786,
    787,
    788,
    789,
    790,
    791,
    792,
    793,
    794,
    795,
    796,
    797,
    798,
    799,
    800,
    801,
    802,
    803,
    804,
    805,
    806,
    807,
    808,
    809,
    810,
    811,
    812,
    813,
    814,
    815,
    816,
    817,
    818,
    819,
    820,
    821,
    822,
    823,
    824,
    825,
    826,
    827,
    828,
    829,
    830,
    831,
    832,
    833,
    834,
    835,
    836,
    837,
    838,
    839,
    840,
    841,
    842,
    843,
    844,
    845,
    846,
    847,
    848,
    849,
    850,
    851,
    852,
    853,
    854,
    855,
    856,
    857,
    858,
    859,
    860,
    861,
    862,
    863,
    864,
    865,
    866,
    867,
    868,
    869,
    870,
    871,
    872,
    873,
    874,
    875,
    876,
    877,
    878,
    879,
    880,
    881,
    882,
    883,
    884,
    885,
    886,
    887,
    888,
    889,
    890,
    891,
    892,
    893,
    894,
    895,
    896,
    897,
    898,
    899,
    900,
    901,
    902,
    903,
    904,
    905,
    906,
    907,
    908,
    909,
    910,
    911,
    912,
    913,
    914,
    915,
    916,
    917,
    918,
    919,
    920,
    921,
    922,
    923,
    924,
    925,
    926,
    927,
    928,
    929,
    930,
    931,
    932,
    933,
    934,
    935,
    936,
    937,
    938,
    939,
    940,
    941,
    942,
    943,
    944,
    945,
    946,
    947,
    948,
    949,
    950,
    951,
    952,
    953,
    954,
    955,
    956,
    957,
    958,
    959,
    960,
    961,
    962,
    963,
    964,
    965,
    966,
    967,
    968,
    969,
    970,
    971,
    972,
    973,
    974,
    975,
    976,
    977,
    978,
    979,
    980,
    981,
    982,
    983,
    984,
    985,
    986,
    987,
    988,
    989,
    990,
    991,
    992,
    993,
    994,
    995,
    996,
    997,
    998,
    999,
]
load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
session = Session(access=access, secret=secret, endpoint=endpoint)

logger = logging.getLogger(f"{__name__}")
logger.setLevel(logging.INFO)
logger.info("copy_to_prod_cos_mp.py")
SRC = "colabfit.dev.co_wip"
DEST = "colabfit-prod.prod.co_tmp"
arrow_schema = spark_schema_to_arrow_schema(config_prop_schema)
arrow_inter_schema = pa.schema(
    [
        (
            field
            if field.name != "last_modified"
            else pa.field("last_modified", pa.timestamp("ns"))
        )
        for field in arrow_schema
    ],
)


def convert_table_to_timestamp_us(table):
    """
    Convert the timestamp column in the table to microseconds.
    """
    # Assuming the timestamp column is named 'last_modified'
    # and is of type pa.timestamp('ns')
    # You can adjust the column name as per your schema
    if "last_modified" not in table.schema.names:
        raise ValueError("Column 'last_modified' not found in the table schema.")

    # Convert the timestamp column to microseconds
    table.set_column(
        table.schema.get_field_index("last_modified"),
        "last_modified",
        table.column("last_modified").cast(pa.timestamp("ns")),
    )
    return table


def get_session():
    load_dotenv()
    access = os.getenv("VAST_DB_ACCESS")
    secret = os.getenv("VAST_DB_SECRET")
    endpoint = os.getenv("VAST_DB_ENDPOINT")
    return Session(access=access, secret=secret, endpoint=endpoint)


def get_cos(prefix):
    logger.info(f"Processing {prefix}")
    session = get_session()
    b1, s1, t1 = SRC.split(".")
    b2, s2, t2 = DEST.split(".")
    with session.transaction() as tx:
        table = tx.bucket(b1).schema(s1).table(t1)
        table2 = tx.bucket(b2).schema(s2).table(t2)
        reader = table.select(
            predicate=table["id"].startswith(prefix),
        )
        batch_rows = 0
        table_batch = []
        for batch in reader:
            batch_rows += batch.num_rows
            table_batch.append(batch)
            if batch_rows >= 25000:
                pos = pa.Table.from_batches(table_batch, schema=arrow_inter_schema)
                pos = convert_table_to_timestamp_us(pos)
                table2.insert(pos)
                logger.info(f"{prefix}: Inserted {batch_rows} rows")
                batch_rows = 0
                table_batch = []
        if batch_rows > 0:
            pos = pa.Table.from_batches(table_batch, schema=arrow_inter_schema)
            pos = convert_table_to_timestamp_us(pos)
            table2.insert(pos)
            logger.info(f"Inserted {batch_rows} rows to finish {prefix}")
        logger.info(f"Finished {prefix}")


DEST = "colabfit-prod.prod.co_po_merged_innerjoin"


def create_projections():
    session = get_session()
    b2, s2, t2 = DEST.split(".")


with session.transaction() as tx:
    logger.info(f"Creating table {DEST}")
    table = tx.bucket(b2).schema(s2).table(t2)
    sorted_columns = ["id"]
    unsorted_columns = [
        col
        for col in config_prop_schema.fieldNames()
        if col not in sorted_columns + ["id"]
    ]
    table.create_projection(
        projection_name="co-id-all",
        sorted_columns=sorted_columns,
        unsorted_columns=unsorted_columns,
    )
    logger.info(table.projections())


if __name__ == "__main__":
    logger.info(f"Copying {SRC} to {DEST}")
    start = time()
    p = Pool(24)
    co_prefixes = MISSING
    co_prefixes = [f"CO_{prefix}" for prefix in co_prefixes]
    p.map(get_cos, co_prefixes)
    logger.info("Copy complete!")
    logger.info(f"Copy runtime: {time() - start}")
    logger.info(f"Copied {SRC} to {DEST}")
    logger.info("Creating projection")
    create_projections()
    logger.info("Done!")
