import json
import logging
import os
from collections import OrderedDict

import boto3
import pyarrow as pa
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from vastdb.session import Session

load_dotenv()
logger = logging.getLogger(__name__)

access_key = os.getenv("VAST_DB_ACCESS")
access_secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("SPARK_ENDPOINT")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)

TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))


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
    "wlog",
    "xml_data_link",
    "ZPVE",
    "zpve",
]


def remove_too_long_keys_vals(metadata: dict, max_length: int = 1000) -> dict:
    """Remove keys and values from metadata dict that exceed max_length"""
    cleaned_metadata = {}

    for k, v in metadata.items():
        if isinstance(v, dict):
            v = remove_too_long_keys_vals(v, max_length)
        if len(str(v)) <= max_length:
            cleaned_metadata[k] = v

    return cleaned_metadata


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
        metadata = remove_too_long_keys_vals(metadata, max_length=8_000)
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
prefix = prefixes[TASK_ID]
logger.info(f"Processing prefix {prefix}")


def batcher(iterable: pa.RecordBatch, return_size=5000):
    """Yield successive n-sized chunks from iterable."""
    for i in range(0, iterable.num_rows, return_size):
        yield iterable.slice(i, min(return_size, iterable.num_rows - i))


with session.transaction() as tx:
    co_table = tx.bucket("colabfit-prod").schema("prod").table("co_arrays")

    # Query data from VastDB
    reader = co_table.select(predicate=co_table["property_id"].startswith(prefix))

    # Process batches
    for r_batch in reader:
        if r_batch.num_rows == 0:
            continue
        for slice_batch in batcher(r_batch, return_size=5000):
            logger.info(f"Processing batch with {slice_batch.num_rows} rows")

            # Convert to table and add metadata column
            table = pa.Table.from_batches([slice_batch])
            table_with_metadata = add_metadata_column(table, s3_mgr)

            # Write to target table
            target_table = (
                tx.bucket("colabfit-prod")
                .schema("prod")
                .table("co_new_hashes_by_prefix2")
            )
            target_table.insert(table_with_metadata)

            logger.info(f"Wrote {table_with_metadata.num_rows} rows to target table")

logger.info(f"Finished prefix {prefix}")
