import json
from datetime import datetime
from logging import getLogger
from pathlib import Path
from time import time
import os
import numpy as np
import pyarrow as pa
from ase import Atoms
from ase.io.extxyz import write_extxyz
from vastdb.session import Session
from colabfit.tools.vast.utils import get_session
from dotenv import load_dotenv

load_dotenv()
access = os.getenv("VAST_DB_ACCESS")
print(access)
secret = os.getenv("VAST_DB_SECRET")
print(secret)
endpoint = os.getenv("VAST_DB_ENDPOINT")
print(endpoint)
logger = getLogger(__name__)
SLURM_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
if not SLURM_TASK_ID:
    raise ValueError("SLURM_ARRAY_TASK_ID environment variable not set")


def json_serialize(col):
    if isinstance(col, (datetime)):
        return col.strftime("%Y-%m-%dT%H:%M:%SZ")
    raise TypeError("Type %s not serializable" % type(col))


class ColabfitExporter:
    export_root_dir = Path("/scratch/gw2338/xyz_exports")

    def __init__(self):
        load_dotenv()
        _, self.bucket, self.schema, self.ds_table = (
            "ndb.colabfit-prod.prod.dataset_arrays".split(".")
        )
        self.co_table = "co_arrays"
        self.cs_table = "configuration_set_arrays"
        self.cs_co_map_table = "cs_co_map"
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
        # Only fetch columns we need from database
        self.co_columns = self.info_cols + [
            "atomic_numbers",
            "positions",
            "cell",
            "pbc",
            "atomic_forces",
        ]

    def get_ds(self, dataset_id):
        with get_session().transaction() as tx:
            table = tx.bucket(self.bucket).schema(self.schema).table(self.ds_table)
            ds_batch_rdr = table.select(predicate=(table["id"] == dataset_id))
            ds = ds_batch_rdr.read_all().to_pylist()[0]
            return ds

    def get_cos(self, dataset_id):
        with get_session().transaction() as tx:
            table = tx.bucket(self.bucket).schema(self.schema).table(self.co_table)
            co_batch_rdr = table.select(
                predicate=(table["dataset_id"] == dataset_id),
                columns=self.co_columns,
            )
            yield from self.batch_manager(co_batch_rdr)

    def batch_manager(self, data_iterator, target_batch_size=100_000):
        leftover_table = None
        batch_num = 0
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
                batch_to_yield = combined_table.slice(current_offset, target_batch_size)
                yield batch_to_yield
                batch_num += 1
                current_offset += target_batch_size
            remaining_rows = combined_table.num_rows - current_offset
            if remaining_rows > 0:
                leftover_table = combined_table.slice(current_offset, remaining_rows)
        if leftover_table is not None and leftover_table.num_rows > 0:
            yield leftover_table

    def create_atoms_from_table(self, pa_table):
        n_rows = pa_table.num_rows

        # Extract required columns once
        atomic_numbers = pa_table.column("atomic_numbers").to_pylist()
        positions = pa_table.column("positions").to_pylist()
        cells = pa_table.column("cell").to_pylist()
        pbcs = pa_table.column("pbc").to_pylist()
        forces = pa_table.column("atomic_forces").to_pylist()

        # Extract info columns once (only if they exist)
        info_data = {}
        for col in self.info_cols:
            if col in pa_table.column_names:
                info_data[col] = pa_table.column(col).to_pylist()

        for i in range(n_rows):
            # Build info dict for this row
            info = {}
            for col, values in info_data.items():
                val = values[i]
                if val is not None and val != []:
                    info[col] = val

            force_val = forces[i]
            has_forces = force_val is not None and force_val != []

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

    def write_atoms_to_extxyz(self, atoms_iter, export_path):
        atoms = [
            a for a in atoms_iter
        ]  # have to gather bc of "if 'move_mask' in fr_cols:" block in write_extxyz
        with open(export_path, "w") as f:
            write_extxyz(f, atoms)

    def export_dataset(self, dataset_id):
        # Create the export directory
        logger.info(f"Exporting dataset: {dataset_id}")
        start_time = time()
        export_dir = self.export_root_dir / dataset_id
        if export_dir.exists():
            logger.info(f"dataset dir {dataset_id} exists...")
            if (export_dir / "dataset.json").exists():
                logger.info(f"Dataset {dataset_id} already exported. Skipping.")
                return
        export_dir.mkdir(parents=True, exist_ok=True)
        co_dir = export_dir / "co"
        co_dir.mkdir(parents=True, exist_ok=True)
        ds = self.get_ds(dataset_id)
        co_batches = self.get_cos(dataset_id)
        total_configs = 0
        for i, batch in enumerate(co_batches):
            atoms_batch = self.create_atoms_from_table(batch)
            export_path = co_dir / f"co_{i}.extxyz"
            self.write_atoms_to_extxyz(atoms_batch, export_path)
            total_configs += batch.num_rows
            # Log every 10 batches to reduce overhead
            if (i + 1) % 10 == 0:
                logger.info(f"Processed {i + 1} batches ({total_configs} configs)")
        logger.info(
            f"Finished exporting {total_configs} configurations "
            f"for dataset {dataset_id}"
        )
        with open(export_dir / "dataset.json", "w") as f:
            json.dump(ds, f, indent=4, default=json_serialize)
        logger.info(f"Wrote dataset.json for dataset {dataset_id}")
        logger.info(f"Finished in {time() - start_time:.2f} seconds")


def main(slurm_task_id):
    with get_session().transaction() as tx:
        table = tx.bucket("colabfit-prod").schema("prod").table("dataset_arrays")
        ids = table.select(columns=["id", "nconfigurations"]).read_all()
    ids = ids.sort_by("nconfigurations")
    if slurm_task_id > ids.num_rows:
        raise ValueError(
            f"SLURM_TASK_ID {slurm_task_id} exceeds number of datasets {ids.num_rows}"
        )
    id = ids["id"].to_pylist()[slurm_task_id]
    exporter = ColabfitExporter()
    exporter.export_dataset(id)


if __name__ == "__main__":
    main(SLURM_TASK_ID)
