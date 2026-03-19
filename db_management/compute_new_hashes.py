import logging
import os
from hashlib import sha512

import numpy as np
import pyarrow as pa
from dotenv import load_dotenv
from vastdb.config import QueryConfig
from vastdb.session import Session

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

access_key = os.getenv("VAST_DB_ACCESS")
access_secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("SPARK_ENDPOINT")

if not access_key or not access_secret or not endpoint:
    logger.error(
        f"Missing environment variables: access_key={bool(access_key)}, "
        f"access_secret={bool(access_secret)}, endpoint={bool(endpoint)}"
    )
    raise ValueError("Missing required environment variables")

logger.info(f"Connecting to endpoint: {endpoint}")

task_id_str = os.getenv("SLURM_ARRAY_TASK_ID")
if not task_id_str:
    logger.error("SLURM_ARRAY_TASK_ID environment variable not set")
    raise ValueError("SLURM_ARRAY_TASK_ID must be set")

TASK_ID = int(task_id_str)
logger.info(f"Starting with TASK_ID: {TASK_ID}")

qconfig = QueryConfig(
    limit_rows_per_sub_split=10_000, use_semi_sorted_projections=False
)


# Hashing functions inlined from colabfit-tools/colabfit/tools/vast/utils/hashing.py
# (metadata_column branch, commit a68b348)
def _format_for_hash(v):
    if isinstance(v, (list, tuple)):
        v = np.array(v)
    if isinstance(v, np.ndarray):
        if np.issubdtype(v.dtype, np.floating):
            return np.round(v.astype(np.float64), decimals=16)
        elif np.issubdtype(v.dtype, np.integer):
            return v.astype(np.int64)
        elif np.issubdtype(v.dtype, np.bool_):
            return v.astype(np.int64)
        else:
            return v
    elif isinstance(v, dict):
        return str(v).encode("utf-8")
    elif isinstance(v, str):
        return v.encode("utf-8")
    elif isinstance(v, (int, float)):
        return np.array(v).data.tobytes()
    else:
        return v


def _new_hash(row: dict, identifying_key_list: list) -> str:
    identifying_key_list = sorted(identifying_key_list)
    identifiers = [row[k] for k in identifying_key_list]
    h = sha512()
    for _, v in zip(identifying_key_list, identifiers):
        if v is None or v == "[]":
            continue
        h.update(_format_for_hash(v))
    return h.hexdigest()


def new_config_struct_hash(
    atomic_numbers: list,
    cell: list,
    pbc: list,
    positions: list,
) -> str:
    """Sort atoms by (x, y, z) before hashing so result is ordering-independent."""
    h = sha512()
    positions_arr = np.array(positions)
    sort_ixs = np.lexsort(
        (positions_arr[:, 2], positions_arr[:, 1], positions_arr[:, 0])
    )
    sorted_positions = positions_arr[sort_ixs]
    sorted_atomic_numbers = np.array(atomic_numbers)[sort_ixs]
    h.update(bytes(_format_for_hash(sorted_atomic_numbers)))
    h.update(bytes(_format_for_hash(cell)))
    h.update(bytes(_format_for_hash(pbc)))
    h.update(bytes(_format_for_hash(sorted_positions)))
    return h.hexdigest()


# new_property_hash identifier keys: all fields in property_object_schema minus
# _hash_ignored_fields = [id, hash, new_hash, last_modified, multiplicity,
#                          mean_force_norm, max_force_norm, configuration_id]
# configuration_id (old) is excluded; new_configuration_id (computed below) is included.
PROPERTY_HASH_KEYS = [
    "adsorption_energy",
    "atomic_forces",
    "atomization_energy",
    "cauchy_stress",
    "cauchy_stress_volume_normalized",
    "chemical_formula_hill",
    "dataset_id",
    "electronic_band_gap",
    "electronic_band_gap_type",
    "energy",
    "energy_above_hull",
    "formation_energy",
    "metadata",
    "method",
    "new_configuration_id",
    "software",
]

# new_configuration_hash identifier keys: AtomicConfiguration.unique_identifier_kw
CONFIG_HASH_KEYS = ["atomic_numbers", "cell", "pbc", "positions"]


def compute_hashes_for_row(row: dict) -> tuple:
    """
    Returns (new_prop_hash, new_prop_id, new_config_hash, new_config_id,
             new_struct_hash).
    Config hash is computed first so new_configuration_id can be injected
    into the row before computing the property hash.
    """
    new_config_hash = _new_hash(row, CONFIG_HASH_KEYS)
    new_config_id = f"CO_{new_config_hash}"[:28]
    new_struct_hash = new_config_struct_hash(
        row["atomic_numbers"], row["cell"], row["pbc"], row["positions"]
    )
    # Inject computed new_configuration_id before hashing property; the source
    # column may not exist or may be null in the database.
    row_for_prop = {**row, "new_configuration_id": new_config_id}
    new_prop_hash = _new_hash(row_for_prop, PROPERTY_HASH_KEYS)
    new_prop_id = f"PO_{new_prop_hash}"[:28]
    return new_prop_hash, new_prop_id, new_config_hash, new_config_id, new_struct_hash


def add_new_hash_columns(table: pa.Table) -> pa.Table:
    """Compute and set the five new hash/id columns on a PyArrow Table."""
    rows = table.to_pydict()
    n = table.num_rows
    new_prop_hashes, new_prop_ids = [], []
    new_config_hashes, new_config_ids = [], []
    new_struct_hashes = []

    for i in range(n):
        row = {col: rows[col][i] for col in rows}
        ph, pi, ch, ci, sh = compute_hashes_for_row(row)
        new_prop_hashes.append(ph)
        new_prop_ids.append(pi)
        new_config_hashes.append(ch)
        new_config_ids.append(ci)
        new_struct_hashes.append(sh)

    new_cols = [
        ("new_property_hash", new_prop_hashes),
        ("new_property_id", new_prop_ids),
        ("new_configuration_hash", new_config_hashes),
        ("new_configuration_id", new_config_ids),
        ("new_structure_hash", new_struct_hashes),
    ]
    for col_name, values in new_cols:
        arr = pa.array(values, type=pa.string())
        if col_name in table.schema.names:
            idx = table.schema.get_field_index(col_name)
            table = table.set_column(idx, pa.field(col_name, pa.string()), arr)
        else:
            table = table.append_column(pa.field(col_name, pa.string()), arr)
    return table


def get_session():
    load_dotenv()
    return Session(
        access=os.getenv("VAST_DB_ACCESS"),
        secret=os.getenv("VAST_DB_SECRET"),
        endpoint=os.getenv("SPARK_ENDPOINT"),
    )


def update_cs_co_map(
    tx,
    table_with_hashes: pa.Table,
    cs_co_map_table_name: str,
    new_cs_co_map_table_name: str,
):
    """
    Updates the cs_co_map table with new configuration IDs.
    It reads the existing cs_co_map, creates a new table with an additional
    'new_configuration_id' column, and populates it with mappings from old to new
    configuration IDs.
    """
    logger.info(f"Updating {new_cs_co_map_table_name}...")
    cs_co_map_table = (
        tx.bucket("colabfit-prod").schema("prod").table(cs_co_map_table_name)
    )

    # Ensure the new table exists with the correct schema
    try:
        new_schema = cs_co_map_table.schema.append(
            pa.field("new_configuration_id", pa.string())
        )
        tx.bucket("colabfit-prod").schema("prod").create_table(
            new_cs_co_map_table_name, columns=new_schema, fail_if_exists=False
        )
        logger.info(f"Table {new_cs_co_map_table_name} ensured to exist.")
    except Exception as e:
        logger.info(f"Skipping creation of {new_cs_co_map_table_name}: {e}")

    new_cs_co_map_table = (
        tx.bucket("colabfit-prod").schema("prod").table(new_cs_co_map_table_name)
    )

    # Create a dictionary for quick lookup of new_configuration_id
    processed_configs = {
        row["configuration_id"]: row["new_configuration_id"]
        for row in table_with_hashes.to_pydict()
    }

    config_ids_to_lookup = list(processed_configs.keys())

    # Query cs_co_map for all relevant configuration_ids in one go
    if config_ids_to_lookup:
        map_reader = cs_co_map_table.select(
            predicate=cs_co_map_table["configuration_id"].isin(config_ids_to_lookup)
        )

        rows_to_insert = []
        for batch in map_reader:
            for row in batch.to_pylist():
                old_config_id = row["configuration_id"]
                new_config_id = processed_configs.get(old_config_id)
                if new_config_id:
                    rows_to_insert.append(
                        {
                            "configuration_set_id": row["configuration_set_id"],
                            "configuration_id": old_config_id,
                            "new_configuration_id": new_config_id,
                        }
                    )

        if rows_to_insert:
            new_cs_co_map_table.insert(rows_to_insert)
            logger.info(
                f"Inserted {len(rows_to_insert)} rows into {new_cs_co_map_table_name}"
            )


def append_wip_to_prod(from_table: str, to_table: str):
    """Copy all rows from from_table into to_table using a fresh session."""
    total_rows = 0
    _, from_bucket, from_schema, from_tbl = from_table.split(".")
    _, to_bucket, to_schema, to_tbl = to_table.split(".")
    with get_session().transaction() as tx:
        src = tx.bucket(from_bucket).schema(from_schema).table(from_tbl)
        dst = tx.bucket(to_bucket).schema(to_schema).table(to_tbl)
        for batch in src.select():
            dst.insert(batch)
            total_rows += batch.num_rows
    logger.info(f"Appended {total_rows} rows to {to_table}")


prefixes = [f"PO_{n}" for n in range(1000, 1350)]
prefixes += [f"PO_{n}" for n in range(135, 1000)]

ZERO_BATCH = os.getenv("ZERO_BATCH", "false").lower() == "true"

if ZERO_BATCH:
    if TASK_ID != 0:
        logger.info("ZERO_BATCH is true, but TASK_ID is not 0. Exiting.")
        exit()
    else:
        logger.info("Running ZERO_BATCH for initial setup.")
        prefixes = [prefixes[0]]
elif TASK_ID >= len(prefixes):
    logger.error(f"TASK_ID {TASK_ID} exceeds number of prefixes {len(prefixes)}")
    raise ValueError(f"TASK_ID {TASK_ID} is out of range")

prefix = prefixes[TASK_ID]
logger.info(f"Processing prefix {prefix} (index {TASK_ID} of {len(prefixes)})")

target_table_name = f"co_tmp_hashes_{prefix}"
PROD_TABLE = "ndb.colabfit-prod.prod.co_with_hashes"
CS_CO_MAP_TABLE = "ndb.colabfit-prod.prod.cs_co_map"
NEW_CS_CO_MAP_TABLE = "ndb.colabfit-prod.prod.new_cs_co_map"

try:
    # Drop the temporary table if it exists
    with get_session().transaction() as tx:
        tx.bucket("colabfit-prod").schema("prod").table(target_table_name).drop(
            must_exist=False
        )
    logger.info(f"Dropped existing temporary table {target_table_name} if it existed.")

    with get_session().transaction() as tx:
        logger.info("Transaction started")
        co_table = tx.bucket("colabfit-prod").schema("prod").table("co_with_metadata")
        logger.info(f"Querying for property_id starting with {prefix}")
        reader = co_table.select(
            config=qconfig,
            predicate=co_table["property_id"].startswith(prefix),
        )

        batch_count = 0
        total_rows = 0
        table_with_hashes = None
        for batch in reader:
            if batch.num_rows == 0:
                logger.info("Skipping empty batch")
                continue

            batch_count += 1
            total_rows += batch.num_rows
            logger.info(f"Processing batch {batch_count} with {batch.num_rows} rows")

            table_with_hashes = add_new_hash_columns(pa.Table.from_batches([batch]))

            if batch_count == 1:
                sch = tx.bucket("colabfit-prod").schema("prod")
                sch.create_table(
                    target_table_name,
                    columns=table_with_hashes.schema,
                    fail_if_exists=False,
                )

            target_table = (
                tx.bucket("colabfit-prod").schema("prod").table(target_table_name)
            )
            target_table.insert(table_with_hashes)
            logger.info(
                f"Wrote {table_with_hashes.num_rows} rows to {target_table_name}"
            )

            # Update the cs_co_map table
            update_cs_co_map(
                tx,
                table_with_hashes,
                CS_CO_MAP_TABLE.split(".")[-1],
                NEW_CS_CO_MAP_TABLE.split(".")[-1],
            )

        if batch_count == 0:
            logger.warning(f"No batches found for prefix {prefix}")
        else:
            logger.info(f"Processed {batch_count} batches, {total_rows} total rows")

    if batch_count == 0:
        logger.info("Nothing to append; exiting.")
    else:
        logger.info(f"Appending {prefix} to production table")
        try:
            with get_session().transaction() as tx:
                tx.bucket("colabfit-prod").schema("prod").create_table(
                    PROD_TABLE.split(".")[-1],
                    columns=table_with_hashes.schema,
                    fail_if_exists=False,
                )
        except Exception as e:
            logger.info(f"Production table creation skipped: {e}")

        append_wip_to_prod(f"ndb.colabfit-prod.prod.{target_table_name}", PROD_TABLE)

        with get_session().transaction() as tx:
            tx.bucket("colabfit-prod").schema("prod").table(target_table_name).drop()
        logger.info(f"Dropped temporary table {target_table_name}")
        logger.info("Complete!")

except Exception as e:
    logger.error(f"Error processing prefix {prefix}: {str(e)}", exc_info=True)
    raise
