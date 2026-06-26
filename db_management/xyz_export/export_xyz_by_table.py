import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from time import time

import numpy as np
import pyarrow as pa
import vastdb
from ase import Atoms
from ase.io.extxyz import write_extxyz
from dotenv import load_dotenv
from ibis import _
from vastdb.config import QueryConfig

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

LARGE_DATASET_THRESHOLD = 5_000_000
BATCH_SIZE = 100_000
FILE_ROW_LIMIT = 500_000

query_config = QueryConfig(
    limit_rows_per_sub_split=150_000,
    use_semi_sorted_projections=False,
)


def json_serialize(col):
    if isinstance(col, datetime):
        return col.strftime("%Y-%m-%dT%H:%M:%SZ")
    raise TypeError("Type %s not serializable" % type(col))


def get_vastdb_session():
    secret_key = os.getenv("VAST_DB_SECRET")
    endpoint = os.getenv("VAST_DB_ENDPOINT")
    access_key = os.getenv("VAST_DB_KEY")
    return vastdb.connect(endpoint=endpoint, access=access_key, secret=secret_key)


class ColabfitXYZExporter:
    def __init__(self, table_suffix, output_dir=None):
        self.table_suffix = table_suffix
        self.bucket = "colabfit"
        self.schema = "dev"
        self.ds_table_name = f"ds_{table_suffix}"
        self.co_table_name = f"co_{table_suffix}"
        self.export_root_dir = Path(output_dir) if output_dir else Path.cwd()
        self.info_cols = [
            "dataset_id",
            "multiplicity",
            "software",
            "method",
            "energy",
            "cauchy_stress",
            "cauchy_stress_volume_normalized",
            "electronic_band_gap",
            "electronic_band_gap_type",
            "formation_energy",
            "adsorption_energy",
            "atomization_energy",
            "max_force_norm",
            "mean_force_norm",
            "energy_above_hull",
            "property_id",
            "configuration_id",
            "names",
            "labels",
        ]
        self.co_columns = self.info_cols + [
            "atomic_numbers",
            "positions",
            "cell",
            "pbc",
            "atomic_forces",
        ]

    def get_ds(self, dataset_id):
        with get_vastdb_session().transaction() as tx:
            table = tx.bucket(self.bucket).schema(self.schema).table(self.ds_table_name)
            ds_batch_rdr = table.select(
                predicate=(table["id"] == dataset_id), config=query_config
            )
            return ds_batch_rdr.read_all().to_pylist()[0]

    def get_cos(self, predicate):
        with get_vastdb_session().transaction() as tx:
            table = tx.bucket(self.bucket).schema(self.schema).table(self.co_table_name)
            co_batch_rdr = table.select(
                predicate=predicate, columns=self.co_columns, config=query_config
            )
            yield from self.batch_manager(co_batch_rdr)

    def batch_manager(self, data_iterator, target_batch_size=BATCH_SIZE):
        leftover_table = None
        for raw_batch in data_iterator:
            if raw_batch.num_rows == 0:
                continue
            raw_table = pa.Table.from_batches([raw_batch])
            if leftover_table is not None:
                combined_table = pa.concat_tables([leftover_table, raw_table])
                leftover_table = None
            else:
                combined_table = raw_table
            current_offset = 0
            while current_offset + target_batch_size <= combined_table.num_rows:
                yield combined_table.slice(current_offset, target_batch_size)
                current_offset += target_batch_size
            remaining_rows = combined_table.num_rows - current_offset
            if remaining_rows > 0:
                leftover_table = combined_table.slice(current_offset, remaining_rows)
        if leftover_table is not None and leftover_table.num_rows > 0:
            yield leftover_table

    def create_atoms_from_table(self, pa_table):
        atomic_numbers = pa_table.column("atomic_numbers").to_pylist()
        positions = pa_table.column("positions").to_pylist()
        cells = pa_table.column("cell").to_pylist()
        pbcs = pa_table.column("pbc").to_pylist()
        forces = pa_table.column("atomic_forces").to_pylist()

        info_data = {}
        for col in self.info_cols:
            if col in pa_table.column_names:
                info_data[col] = pa_table.column(col).to_pylist()

        for i in range(pa_table.num_rows):
            info = {}
            for col, values in info_data.items():
                val = values[i]
                if val is not None and val != []:
                    info[col] = val

            force_val = forces[i]
            has_forces = force_val is not None and force_val != []
            has_stress = "cauchy_stress" in info and info["cauchy_stress"] is not None
            if has_stress:
                info["cauchy_stress"] = (
                    np.array(info["cauchy_stress"]).flatten().tolist()
                )

            atoms = Atoms(
                numbers=atomic_numbers[i],
                positions=positions[i],
                cell=cells[i],
                pbc=pbcs[i],
                info=info,
            )
            if has_forces:
                atoms.arrays["forces"] = np.array(force_val)
            yield atoms

    def _write_configs_to_xyz(self, predicate, output_dir, initial_file_count):
        """Fetch configs and write to extxyz, interleaving DB reads and file writes."""
        file_count = initial_file_count
        file_rows = 0
        total_configs = 0
        current_file_path = None
        fh = None
        try:
            for batch in self.get_cos(predicate):
                for atoms in self.create_atoms_from_table(batch):
                    if fh is None:
                        current_file_path = output_dir / f"co_{file_count}.extxyz"
                        fh = open(current_file_path, "w")
                        logger.info(f"Opened {current_file_path}")
                    write_extxyz(fh, [atoms])
                    file_rows += 1
                    total_configs += 1
                    if file_rows >= FILE_ROW_LIMIT:
                        fh.close()
                        logger.info(f"Saved {current_file_path} ({file_rows} configs)")
                        fh = None
                        file_count += 1
                        file_rows = 0
                if total_configs % 100_000 == 0 and total_configs > 0:
                    logger.info(f"Written {total_configs} configs so far")
            if fh is not None:
                fh.close()
                logger.info(f"Saved {current_file_path} ({file_rows} configs)")
                file_count += 1
        except Exception:
            if fh is not None:
                fh.close()
            raise
        return file_count, total_configs

    def export_dataset_prefix(self, predicate, prefix, co_tmp_dir, file_offset):
        """Export configurations for a single prefix into co_tmp_dir."""
        logger.info(f"Exporting prefix {prefix}")
        file_count, total_configs = self._write_configs_to_xyz(
            predicate, co_tmp_dir, file_offset
        )
        logger.info(f"Prefix {prefix}: {total_configs} configurations exported")
        return file_count, total_configs

    def export_dataset_in_batches(self, dataset_id, export_dir):
        """Export dataset using hex-prefix batching for large datasets and restartability."""
        logger.info(f"Exporting dataset in batches: {dataset_id}")
        start_time = time()

        co_dir = export_dir / "co"
        co_dir.mkdir(parents=True, exist_ok=True)
        co_tmp_dir = co_dir / "tmp"
        co_tmp_dir.mkdir(parents=True, exist_ok=True)

        existing_tmp_files = list(co_tmp_dir.glob("*.extxyz"))
        if existing_tmp_files:
            logger.info(
                f"Found {len(existing_tmp_files)} temporary files, removing them"
            )
            for tmp_file in existing_tmp_files:
                tmp_file.unlink()

        prefix_div = [f"PO_{c}" for c in "0123456789abcdef"]
        existing_prefix_paths = {p.name for p in co_dir.glob("PO_*")}

        max_file_count = 0
        for prefix_dir in co_dir.glob("PO_*"):
            if prefix_dir.is_dir():
                for xyz_file in prefix_dir.glob("co_*.extxyz"):
                    try:
                        file_num = int(xyz_file.stem.split("_")[1])
                        max_file_count = max(max_file_count, file_num)
                    except (ValueError, IndexError):
                        continue
        for xyz_file in co_dir.glob("co_*.extxyz"):
            try:
                file_num = int(xyz_file.stem.split("_")[1])
                max_file_count = max(max_file_count, file_num)
            except (ValueError, IndexError):
                continue

        file_count = max_file_count + 1 if max_file_count > 0 else 0
        logger.info(
            f"Starting file count at {file_count} (found max existing: {max_file_count})"
        )

        total_configs = 0
        for prefix in prefix_div:
            if prefix in existing_prefix_paths:
                logger.info(f"Prefix {prefix} already processed, skipping")
                continue

            logger.info(f"Processing prefix: {prefix} for dataset: {dataset_id}")
            predicate = (_.dataset_id == dataset_id) & (
                _.property_id.startswith(prefix)
            )
            file_count, prefix_configs = self.export_dataset_prefix(
                predicate, prefix, co_tmp_dir, file_count
            )
            total_configs += prefix_configs

            co_prefix_path = co_dir / prefix
            co_prefix_path.mkdir(parents=True, exist_ok=True)
            for file in co_tmp_dir.glob("*.extxyz"):
                file.rename(co_prefix_path / file.name)
            logger.info(f"Moved temporary files for {prefix} to {co_prefix_path}")

        logger.info(f"Batch export took {time() - start_time:.2f} seconds")
        logger.info(f"Consolidating prefix directories in {co_dir}")

        for file in co_dir.glob("PO_*/*.extxyz"):
            file.rename(co_dir / file.name)

        for prefix in prefix_div:
            prefix_path = co_dir / prefix
            if prefix_path.exists() and prefix_path.is_dir():
                try:
                    prefix_path.rmdir()
                    logger.info(f"Removed empty directory: {prefix_path}")
                except OSError as e:
                    logger.warning(f"Could not remove directory {prefix_path}: {e}")

        try:
            co_tmp_dir.rmdir()
            logger.info(f"Removed temporary directory: {co_tmp_dir}")
        except OSError as e:
            logger.warning(f"Could not remove tmp directory {co_tmp_dir}: {e}")

        return total_configs

    def export_dataset(self, dataset_id):
        logger.info(f"Exporting dataset: {dataset_id}")
        start_time = time()
        export_dir = self.export_root_dir / dataset_id

        if (export_dir / "dataset.json").exists():
            logger.info(f"Dataset {dataset_id} already exported. Skipping.")
            return

        possible_tar_file = self.export_root_dir / "tarfiles" / f"{dataset_id}.tar.gz"
        if possible_tar_file.exists():
            logger.info(f"Dataset {dataset_id} tar file already exists. Skipping.")
            return

        export_dir.mkdir(parents=True, exist_ok=True)

        ds = self.get_ds(dataset_id)
        nconfigs = ds.get("nconfigurations", 0)

        if nconfigs > LARGE_DATASET_THRESHOLD:
            logger.info(
                f"Dataset {dataset_id} has {nconfigs} configurations. "
                "Using prefix-based batches."
            )
            total_configs = self.export_dataset_in_batches(dataset_id, export_dir)
        else:
            logger.info(
                f"Dataset {dataset_id} has {nconfigs} configurations. "
                "Selecting all at once."
            )
            co_dir = export_dir / "co"
            co_dir.mkdir(parents=True, exist_ok=True)
            predicate = _.dataset_id == dataset_id
            _, total_configs = self._write_configs_to_xyz(predicate, co_dir, 0)

        logger.info(
            f"Finished exporting {total_configs} configurations for dataset {dataset_id}"
        )
        with open(export_dir / "dataset.json", "w") as f:
            json.dump(ds, f, indent=4, default=json_serialize)
        logger.info(f"Wrote dataset.json for dataset {dataset_id}")
        logger.info(f"Finished in {time() - start_time:.2f} seconds")


def process_dataset(dataset_id, table_suffix, output_dir=None):
    exporter = ColabfitXYZExporter(table_suffix, output_dir=output_dir)
    exporter.export_dataset(dataset_id)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(
            "Usage: python export_xyz_by_table.py <dataset_id> <table_suffix> "
            "[output_dir]"
        )
        sys.exit(1)

    ds_id = sys.argv[1]
    table_suffix = sys.argv[2]
    output_dir = sys.argv[3] if len(sys.argv) > 3 else None
    process_dataset(ds_id, table_suffix, output_dir)
