"""
895 879 867 594 529 (rerun already) 206
"""

import json
import logging
import os
from collections import OrderedDict

import boto3
import pyarrow as pa
import pyarrow.compute as pc
from botocore.exceptions import ClientError
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
qconfig = QueryConfig(
    limit_rows_per_sub_split=10_000, use_semi_sorted_projections=False
)
if not access_key or not access_secret or not endpoint:
    logger.error(
        f"Missing environment variables: access_key={bool(access_key)}, "
        f"access_secret={bool(access_secret)}, endpoint={bool(endpoint)}"
    )
    raise ValueError("Missing required environment variables")

logger.info(f"Connecting to endpoint: {endpoint}")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)

task_id_str = os.getenv("SLURM_ARRAY_TASK_ID")
if not task_id_str:
    logger.error("SLURM_ARRAY_TASK_ID environment variable not set")
    raise ValueError("SLURM_ARRAY_TASK_ID must be set")

TASK_ID = int(task_id_str)
logger.info(f"Starting with TASK_ID: {TASK_ID}")


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

    def read_file(self, file_key: str):
        try:
            client = self.get_client()
            response = client.get_object(Bucket=self.bucket_name, Key=file_key)
            return response["Body"].read().decode("utf-8")
        except Exception as e:
            return f"Error: {str(e)}"


KEYS_TO_REMOVE = [
    "a2F",
    "a2F_original_x",
    "a2F_original_y",
    "alpha",
    "aniso_di_pol",
    "atomic-charges",
    "avg_elec_mass",
    "avg_hole_mass",
    "bulk_modulus_kv",
    "case",
    "charge",
    "chemical_system",
    "cm5-atomic-charges",
    "composition",
    "crys",
    "CSD-code",
    "cumul_frames",
    "Cv",
    "cv",
    "data_id",
    "density",
    "desc",
    "dft_bulk_modulus",
    "dft_mag_density",
    "dft_total_gradient",
    "di_moment_1",
    "di_moment_2",
    "di_moment_3",
    "di_pol_1",
    "di_pol_2",
    "di_pol_3",
    "di_pol_4",
    "di_pol_5",
    "di_pol_6",
    "diel_elec",
    "diel_ion",
    "diel_tot",
    "dihedrals",
    "dimensionality",
    "dimer-charge",
    "dimer-multiplicity",
    "dipole",
    "dipole-moment",
    "dipole_moment",
    "dipoles",
    "dispersion",
    "distances-to-migrating-atoms",
    "dmin",
    "dos_info",
    "e_above_hull",
    "efermi",
    "eff._alc",
    "effective_masses_300K",
    "elastic_tensor",
    "electronic-spatial-extent",
    "energy-sigma",
    "energy-temperature",
    "enthalpy",
    "epsx",
    "epsy",
    "epsz",
    "fermi",
    "first_frame",
    "formula",
    "full_formula",
    "fundamental_or_optical",
    "gap",
    "gibbs-energy",
    "H",
    "hash",
    "heat-capacity",
    "hhi_score",
    "HOMO",
    "homo",
    "homo-energy",
    "homo-lumo-gap",
    "homo_energy",
    "homo_lumo_gap",
    "hubbards",
    "id",
    "integrated_densities",
    "interlayer_distance",
    "internal-energy-298K",
    "is_compatible",
    "is_hubbard",
    "iso_di_pol",
    "isotropic-polarizability",
    "kinetic_energy",
    "kinetic_units",
    "lamb",
    "lcd",
    "lowdin_charges",
    "lowdin_spins",
    "LUMO",
    "lumo",
    "lumo-energy",
    "lumo_energy",
    "magmom_oszicar",
    "magmom_outcar",
    "magnetic-moment",
    "magnetic_moments",
    "Magnetic_ordering",
    "masses",
    "max_efg",
    "mayer_indices",
    "mbis_atomic_charges",
    "mbis_atomic_dipole_magnitudes",
    "mbis_atomic_octupole_magnitudes",
    "mbis_atomic_quadrupole_magnitudes",
    "mbis_atomic_volumes",
    "mbis_charges",
    "mbis_dipoles",
    "mbis_octupoles",
    "mbis_quadrupoles",
    "mbj_bandgap",
    "mepsx",
    "mepsy",
    "mepsz",
    "miller_index",
    "ml_bulk_modulus",
    "modes",
    "momenta",
    "monomer_a_multiplicity",
    "monomer_b_multiplicity",
    "mu",
    "mulliken-charges",
    "mulliken_charges",
    "mulliken_spins",
    "n-powerfact",
    "n-Seebeck",
    "n_basis",
    "n_scf_steps",
    "nat",
    "nbo_charges",
    "nbo_spins",
    "ncond",
    "net_magmom",
    "nkappa",
    "nl_energy",
    "ntiling",
    "nuclear-gradients",
    "num_atoms",
    "num_atoms_monomer_a",
    "num_atoms_monomer_b",
    "num_ecp_electrons",
    "num_electrons",
    "omega1",
    "outcar",
    "p-powerfact",
    "p-Seebeck",
    "partial-charge",
    "path",
    "pcond",
    "pkappa",
    "pld",
    "poisson",
    "POTCAR",
    "press",
    "pretty_formula",
    "process",
    "quad_moment_1",
    "quad_moment_2",
    "quad_moment_3",
    "quad_moment_4",
    "quad_moment_5",
    "quad_moment_6",
    "quadrupole-moment",
    "r2",
    "R2",
    "raw_files",
    "real_alc",
    "reference",
    "reference_source",
    "registry",
    "relaxed_positions",
    "rho_(gcc)",
    "rotational-constants",
    "s_squared",
    "s_squared_dev",
    "SAPT2+/aDZ-dispersion",
    "SAPT2+/aDZ-electrostatics",
    "SAPT2+/aDZ-exchange",
    "SAPT2+/aDZ-induction",
    "scf_dipole",
    "scf_quadrupole",
    "search",
    "shear_modulus_gv",
    "shift",
    "SMILES",
    "SMILES_relaxed",
    "source",
    "space_group",
    "spg_number",
    "spin",
    "spin-orbit-coupling",
    "spins",
    "stability",
    "Structure_rlx",
    "t_(k)",
    "Tc",
    "Tc_supercon",
    "temperature",
    "thermostat",
    "top",
    "total-charge",
    "total_frames",
    "total_magnetization",
    "U",
    "U0",
    "unit_cell_formula",
    "unrestricted",
    "velocities",
    "velocity-units",
    "virial-sigma",
    "vol",
    "volume",
    "warnings",
    "wb97x_dz",
    "wf",
    "wiberg_lowdin_indices",
    "wlog",
    "xml_data_link",
    "ZPVE",
    "zpve",
]


def process_metadata_file(file_path, s3_mgr):
    """Process a single metadata file and return cleaned metadata dict"""
    if file_path is None:
        return {}

    try:
        metadata = json.loads(s3_mgr.read_file(file_path))
        metadata = {k: v for k, v in metadata.items() if k not in KEYS_TO_REMOVE}
        if metadata.get("input"):
            if (
                isinstance(metadata["input"], dict)
                and "temperature" in metadata["input"].keys()
            ):
                metadata["input"].pop("temperature")
        return metadata
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return {}
        else:
            logger.error(f"Error reading {file_path}: {str(e)}")
            return {}
    except Exception as e:
        logger.error(f"Error processing {file_path}: {str(e)}")
        return {}


def add_metadata_column(table: pa.Table, s3_mgr) -> pa.Table:
    """
    Add metadata column to PyArrow table by:
    1. Finding unique metadata paths
    2. Processing each unique file once
    3. Mapping back to original rows with correct multiplicity
    """
    # Extract metadata path columns
    prop_paths = table.column("property_metadata_path").to_pylist()
    config_paths = table.column("configuration_metadata_path").to_pylist()

    # Get unique paths to minimize S3 reads
    unique_prop_paths = list(
        OrderedDict.fromkeys(p for p in prop_paths if p is not None)
    )
    unique_config_paths = list(
        OrderedDict.fromkeys(c for c in config_paths if c is not None)
    )

    logger.info(f"Processing {len(unique_prop_paths)} unique property metadata files")
    logger.info(
        f"Processing {len(unique_config_paths)} unique configuration metadata files"
    )

    # Process unique files and cache results
    prop_cache = {
        path: process_metadata_file(path, s3_mgr) for path in unique_prop_paths
    }
    config_cache = {
        path: process_metadata_file(path, s3_mgr) for path in unique_config_paths
    }

    # Build metadata for each row by looking up cached values
    metadata_list = []
    for i in range(table.num_rows):
        po_metadata = prop_cache.get(prop_paths[i], {})
        co_metadata = config_cache.get(config_paths[i], {})

        # Merge metadata
        merged_metadata = {**po_metadata, **co_metadata}
        metadata_list.append(json.dumps(merged_metadata))

    # Add new column to table
    metadata_array = pa.array(metadata_list, type=pa.string())
    return table.append_column(pa.field("metadata", pa.string()), metadata_array)


def get_session():
    from dotenv import load_dotenv
    from vastdb.session import Session
    import os

    load_dotenv()
    access = os.getenv("VAST_DB_ACCESS")
    secret = os.getenv("VAST_DB_SECRET")
    endpoint = os.getenv("VAST_DB_ENDPOINT")
    return Session(access=access, secret=secret, endpoint=endpoint)


def append_wip_table_to_prod(from_table, to_table, dry_run=False):
    session = get_session()
    total_rows = 0
    _, from_bucket, from_schema, from_tbl = from_table.split(".")
    _, to_bucket, to_schema, to_tbl = to_table.split(".")
    with session.transaction() as tx:
        from_table = tx.bucket(from_bucket).schema(from_schema).table(from_tbl)
        to_table = tx.bucket(to_bucket).schema(to_schema).table(to_tbl)
        rdr = from_table.select()
        logger.info(f"Reading from {from_table}")
        logger.info(f"Writing to {to_table}")
        if dry_run:
            for batch in rdr:
                logger.info(batch.num_rows)
                total_rows += batch.num_rows
        else:
            for batch in rdr:
                logger.info(batch.num_rows)
                total_rows += batch.num_rows
                to_table.insert(batch)
    logger.info(f"total_rows: {total_rows}")


# Initialize S3 manager
s3_mgr = S3FileManager(
    bucket_name="colabfit-data",
    access_id=access_key,
    secret_key=access_secret,
    endpoint_url=endpoint,
)

# Method by prefix
prefixes = [f"PO_{prefix_num}" for prefix_num in range(1000, 1350)]
prefixes += [f"PO_{prefix_num}" for prefix_num in range(135, 1000)]

if TASK_ID >= len(prefixes):
    logger.error(f"TASK_ID {TASK_ID} exceeds number of prefixes {len(prefixes)}")
    raise ValueError(f"TASK_ID {TASK_ID} is out of range")

prefix = prefixes[TASK_ID]
logger.info(f"Processing prefix {prefix} (index {TASK_ID} of {len(prefixes)})")

try:
    with session.transaction() as tx:
        target_table_name = f"co_tmp_metadata_{prefix}"
        logger.info("Transaction started")
        sch = tx.bucket("colabfit-prod").schema("prod")
        if sch.table(target_table_name, fail_if_missing=False):
            logger.info("dropping pre-existing temp table")
            sch.table(target_table_name).drop()
        co_table = sch.table("co_arrays")
        logger.info("Table reference obtained")
        # Query data from VastDB
        logger.info(f"Querying for property_id starting with {prefix}")
        reader = co_table.select(
            config=qconfig,
            predicate=co_table["property_id"].startswith(prefix),
        )
        logger.info("Query executed, reading batches...")

        batch_count = 0
        total_rows = 0
        # Process batches
        for batch in reader:
            if batch.num_rows == 0:
                logger.info("Skipping empty batch")
                continue

            batch_count += 1
            total_rows += batch.num_rows
            logger.info(f"Processing batch {batch_count} with {batch.num_rows} rows")

            # Convert to table and add metadata column
            table = pa.Table.from_batches([batch])
            table_with_metadata = add_metadata_column(table, s3_mgr)
            max_len = pc.max(pc.utf8_length(table_with_metadata["metadata"])).as_py()
            logger.info(f"Max length of metadata: {max_len}")
            if max_len > 50_000:
                df = table_with_metadata.to_pandas()
                max_length_index_pd = df["metadata"].apply(len).idxmax()
                max_metadata = df.iloc[max_length_index_pd]
                logger.warning(
                    f"Found metadata exceeding 50k characters at index {max_length_index_pd} with length {max_len}"
                )
                logger.warning(f"Metadata content: {json.loads(max_metadata)}")
                break
            if batch_count == 1:
                sch.create_table(target_table_name, columns=table_with_metadata.schema)

            # Write to target table
            target_table = sch.table(target_table_name)
            target_table.insert(table_with_metadata)

            logger.info(f"Wrote {table_with_metadata.num_rows} rows to target table")
        if batch_count == 0:
            logger.warning(f"No batches found for prefix {prefix}")
        else:
            logger.info(f"Processed {batch_count} batches with {total_rows} total rows")

    logger.info(f"Finished prefix {prefix}")
    try:
        logger.info("Starting to append WIP table to production table")
        from_table = f"ndb.colabfit-prod.prod.{target_table_name}"
        to_table = "ndb.colabfit-prod.prod.co_with_metadata2"
        if TASK_ID == 0:
            with get_session().transaction() as tx:
                sch = tx.bucket("colabfit-prod").schema("prod")
                sch.create_table(
                    to_table.split(".")[-1], columns=table_with_metadata.schema
                )
        append_wip_table_to_prod(from_table, to_table, dry_run=False)
        logger.info("Finished appending WIP table to production table")
        with get_session().transaction() as tx:
            tx.bucket("colabfit-prod").schema("prod").table(target_table_name).drop()
        logger.info(f"Dropped temporary table {target_table_name}")
        logger.info("Complete!")
    except Exception as e:
        logger.error(
            f"Error appending WIP table to production table: {str(e)}", exc_info=True
        )
        raise
except Exception as e:
    logger.error(f"Error processing prefix {prefix}: {str(e)}", exc_info=True)
    raise
