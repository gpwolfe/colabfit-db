import logging
import numbers
import os
import sys

from ast import literal_eval
from pathlib import Path
from time import time

import pyarrow as pa
import vastdb

from dotenv import load_dotenv
from ibis import _

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

CONFIG = {
    "CO_BATCH_SIZE": 100_000,
    "CS_BATCH_SIZE": 100_000,
    "FILE_ROW_LIMIT": 500_000,
    "CSCO_BATCH_SIZE": 10_000,
    "COMPRESSION_LEVEL": 18,
    "LARGE_DATASET_THRESHOLD": 5_000_000,
}


def write_parquet_file(table, output_path, compression_level=None):
    if compression_level is None:
        compression_level = CONFIG["COMPRESSION_LEVEL"]
    with pa.parquet.ParquetWriter(
        output_path,
        table.schema,
        compression="zstd",
        compression_level=compression_level,
    ) as writer:
        writer.write_table(table)


def batch_manager(data_iterator, target_batch_size=100_000):
    leftover_table = None
    batch_num = 0
    for raw_batch in data_iterator:
        if raw_batch.num_rows == 0:
            logger.info("Skipping empty raw batch")
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
            logger.info(
                f"Yielding batch {batch_num} with " f"{batch_to_yield.num_rows} rows"
            )
            yield batch_to_yield
            batch_num += 1
            current_offset += target_batch_size
        remaining_rows = combined_table.num_rows - current_offset
        if remaining_rows > 0:
            leftover_table = combined_table.slice(current_offset, remaining_rows)
    if leftover_table is not None and leftover_table.num_rows > 0:
        logger.info(
            f"Yielding final leftover batch {batch_num} with "
            f"{leftover_table.num_rows} rows"
        )
        yield leftover_table


def get_vastdb_session():
    endpoint = "http://10.32.38.210"
    with open(f"/home/{os.environ['USER']}/.vast-dev/access_key_id", "r") as f:
        access_key = f.read().rstrip("\n")
    with open(f"/home/{os.environ['USER']}/.vast-dev/secret_access_key", "r") as f:
        secret_key = f.read().rstrip("\n")
    return vastdb.connect(endpoint=endpoint, access=access_key, secret=secret_key)


def export_configuration_parquets(dataset_id, dataset_dir, session):
    """Export configuration parquet files using VastDB SDK"""
    start = time()
    logger.info(f"Starting export for dataset: {dataset_id}")

    co_output_path = dataset_dir / "co"
    co_output_path.mkdir(parents=True, exist_ok=True)

    predicate = _.dataset_id == dataset_id
    batch_count, file_count, total_rows = _export_configs(
        predicate, co_output_path, session, 0
    )

    logger.info(
        f"CO processing complete: {batch_count} batches, {total_rows} total rows"
    )
    logger.info(f"CO export took {time() - start:.2f} seconds")

    if batch_count == 0:
        logger.warning(f"No CO batches found for dataset {dataset_id}")
    if total_rows == 0:
        logger.warning(f"No CO rows found for dataset {dataset_id}")


def _export_configs(predicate, co_output_path, session, initial_file_count):

    with session.transaction() as tx:
        co_table = tx.bucket("colabfit-prod").schema("prod").table("co")
        co_data = co_table.select(predicate=predicate)
        batch_count = 0
        file_rows = 0
        file_tables = []
        file_row_size = CONFIG["FILE_ROW_LIMIT"]
        file_count = initial_file_count
        total_rows = 0
        try:
            managed_batches = batch_manager(
                co_data, target_batch_size=CONFIG["CO_BATCH_SIZE"]
            )
            for i, co_batch in enumerate(managed_batches):
                batch_count += 1
                batch_rows = co_batch.num_rows
                total_rows += batch_rows
                file_rows += batch_rows
                logger.info(f"Read CO batch {i}: {batch_rows} rows")
                if batch_rows == 0:
                    logger.warning(f"CO batch {i} is empty, skipping")
                    continue
                file_tables.append(co_batch)

                if file_rows >= file_row_size:
                    file_table = pa.concat_tables(file_tables)
                    file_tables = []
                    output_file = co_output_path / f"co_{file_count}.parquet"
                    logger.info(f"Saving CO batch {file_count} to {output_file}")
                    write_parquet_file(
                        file_table, output_file, CONFIG["COMPRESSION_LEVEL"]
                    )
                    logger.info(f"Successfully saved CO batch {file_count}")
                    file_count += 1
                    file_rows = 0
            if file_tables:
                file_table = pa.concat_tables(file_tables)
                file_tables = []
                output_file = co_output_path / f"co_{file_count}.parquet"
                logger.info(f"Saving final CO batch {file_count} to {output_file}")
                write_parquet_file(file_table, output_file, CONFIG["COMPRESSION_LEVEL"])
                logger.info(f"Successfully saved final CO batch {file_count}")
                file_count += 1

        except Exception as e:
            logger.error(f"Error processing CO data: {e}")
            raise
    return batch_count, file_count, total_rows


def export_configurations_in_batches(dataset_id, dataset_dir, session):
    """Export configuration parquet files using VastDB SDK in batches"""
    start = time()
    logger.info(f"Starting export for dataset: {dataset_id}")
    existing_files = list((dataset_dir / "co" / "tmp").glob("*.parquet"))
    if existing_files:
        logger.info(
            f"Found {len(existing_files)} existing temporary CO files, "
            "they will be removed."
        )
        for tmp_file in existing_files:
            tmp_file.unlink()
    co_dir = dataset_dir / "co"
    if not co_dir.exists():
        co_dir.mkdir(parents=True, exist_ok=True)
    co_tmp_path = dataset_dir / "co" / "tmp"
    if not co_tmp_path.exists():
        co_tmp_path.mkdir(parents=True, exist_ok=True)
    total_batch_count = 0
    total_rows = 0
    prefix_div = [f"PO_{c}" for c in "0123456789abcdef"]
    existing_prefix_paths = {p.name for p in co_dir.glob("PO_*")}

    # Find last file count from existing files
    max_file_count = 0
    for prefix_dir in co_dir.glob("PO_*"):
        if prefix_dir.is_dir():
            for parquet_file in prefix_dir.glob("co_*.parquet"):
                try:
                    file_num = int(parquet_file.stem.split("_")[1])
                    max_file_count = max(max_file_count, file_num)
                except (ValueError, IndexError):
                    continue

    for parquet_file in co_dir.glob("co_*.parquet"):
        try:
            file_num = int(parquet_file.stem.split("_")[1])
            max_file_count = max(max_file_count, file_num)
        except (ValueError, IndexError):
            continue

    file_count = max_file_count + 1 if max_file_count > 0 else 0
    logger.info(
        f"Starting file count at {file_count} (found max existing: {max_file_count})"
    )
    for prefix in prefix_div:
        if prefix in existing_prefix_paths:
            logger.info(f"Prefix {prefix} already processed, skipping")
            continue
        logger.info(f"Processing prefix: {prefix} for dataset: {dataset_id}")
        predicate = (_.dataset_id == dataset_id) & (_.property_id.startswith(prefix))
        batch_count, file_count, batch_rows = _export_configs(
            predicate, co_tmp_path, session, file_count
        )
        total_batch_count += batch_count
        total_rows += batch_rows
        logger.info("CO processing complete")
        logger.info(f"Prefix {prefix}: {batch_count} batches, {batch_rows} total rows")
        co_prefix_path = co_dir / prefix
        if not co_prefix_path.exists():
            co_prefix_path.mkdir(parents=True, exist_ok=True)
        for file in co_tmp_path.glob("*.parquet"):
            final_path = co_prefix_path / file.name
            file.rename(final_path)
        logger.info(f"Moved temporary CO files for {prefix} to {co_prefix_path}")

    logger.info(f"CO export took {time() - start:.2f} seconds")
    logger.info(f"Consolidating prefix directories in {co_dir}")
    for file in co_dir.glob("PO_*/*.parquet"):
        final_path = co_dir / file.name
        file.rename(final_path)
    for prefix in prefix_div:
        prefix_path = co_dir / prefix
        if prefix_path.exists() and prefix_path.is_dir():
            try:
                prefix_path.rmdir()
                logger.info(f"Removed empty directory: {prefix_path}")
            except OSError as e:
                logger.warning(f"Could not remove directory {prefix_path}: {e}")


def export_configuration_sets(dataset_id, dataset_dir, session):
    cs_dir_created = False
    cs_dir = dataset_dir / "cs"
    cs_ids_all = []
    with session.transaction() as tx:
        cs_table = (
            tx.bucket("colabfit-prod").schema("prod").table("configuration_set_arrays")
        )
        cs_data = cs_table.select(predicate=cs_table["dataset_id"] == dataset_id)
        for i, batch in enumerate(
            batch_manager(cs_data, target_batch_size=CONFIG["CS_BATCH_SIZE"])
        ):
            logger.info(f"Read CS batch {i}: {batch.num_rows} rows")
            if batch.num_rows == 0:
                logger.warning(f"CS batch {i} is empty, skipping")
                continue
            if not cs_dir_created:
                cs_dir.mkdir(parents=True, exist_ok=True)
                cs_dir_created = True
            cs_output_path = cs_dir / f"cs_{i}.parquet"
            write_parquet_file(batch, cs_output_path, CONFIG["COMPRESSION_LEVEL"])
            logger.info(f"Saved CS data to: {cs_output_path}")

            cs_ids = batch.column("id").to_pylist()
            cs_ids_all.extend(cs_ids)
    return cs_ids_all


def export_cs_co_mapping(cs_ids_all, dataset_dir, session):
    if not cs_ids_all:
        return

    cs_co_map_dir = dataset_dir / "cs_co_map"
    cs_co_map_dir_created = False

    batch_size = CONFIG["CSCO_BATCH_SIZE"]
    file_count = 0
    file_tables = []
    file_rows = 0

    with session.transaction() as tx:
        cs_co_map_table = tx.bucket("colabfit-prod").schema("prod").table("cs_co_map")

        for i in range(0, len(cs_ids_all), batch_size):
            cs_id_batch = cs_ids_all[i : i + batch_size]  # noqa: E203
            cs_co_map_data = cs_co_map_table.select(
                predicate=cs_co_map_table["configuration_set_id"].isin(cs_id_batch)
            ).read_all()

            logger.info(f"Read CS-CO mapping batch: {cs_co_map_data.num_rows} rows")
            if cs_co_map_data.num_rows == 0:
                logger.info("CS-CO mapping batch is empty, skipping write")
                continue

            file_tables.append(cs_co_map_data)
            file_rows += cs_co_map_data.num_rows

            if file_rows >= CONFIG["FILE_ROW_LIMIT"]:
                if not cs_co_map_dir_created:
                    cs_co_map_dir.mkdir(parents=True, exist_ok=True)
                    cs_co_map_dir_created = True
                output_file = cs_co_map_dir / f"cs_co_map_{file_count}.parquet"
                write_parquet_file(pa.concat_tables(file_tables), output_file)
                file_tables = []
                file_rows = 0
                file_count += 1

        if file_rows > 0:
            if not cs_co_map_dir_created:
                cs_co_map_dir.mkdir(parents=True, exist_ok=True)
                cs_co_map_dir_created = True
            output_file = cs_co_map_dir / f"cs_co_map_{file_count}.parquet"
            write_parquet_file(pa.concat_tables(file_tables), output_file)


def get_dataset_data(dataset_id, session):
    with session.transaction() as tx:
        ds_table = tx.bucket("colabfit-prod").schema("prod").table("dataset_arrays")
        ds_data = ds_table.select(predicate=ds_table["id"] == dataset_id)
        ds_data = ds_data.read_all()
        logger.info(f"Read DS rows: {ds_data.num_rows}")
    return ds_data


def check_table_exists(session, table_name):
    with session.transaction() as tx:
        exists = (
            tx.bucket("colabfit-prod")
            .schema("prod")
            .table(table_name, fail_if_missing=False)
        )
        if exists is not None:
            return True
        else:
            return False


def write_dataset_parquet(ds_data, dataset_dir):
    if ds_data.num_rows > 0:
        ds_output_path = dataset_dir / "ds.parquet"
        write_parquet_file(ds_data, ds_output_path, CONFIG["COMPRESSION_LEVEL"])
        logger.info(f"Saved DS data to: {ds_output_path}")


def generate_dataset_citation_string(item):
    def _ensure_list(value):
        if value is None:
            return []
        if isinstance(value, str):
            try:
                return literal_eval(value)
            except (ValueError, SyntaxError):
                return [value]
        return value

    logger.info(f"Generating citation for dataset {item['id']}")
    joined_names = []
    for author in _ensure_list(item["authors"]):
        name_parts_orig = author.split(" ")
        name_parts_new = []
        family_name = name_parts_orig.pop()
        for name_part in name_parts_orig:
            if name_part[0].islower():
                continue
            name_parts_new.append(name_part[0] + ".")
        joined_names.append(family_name + ", " + " ".join(name_parts_new))

    if len(joined_names) > 1:
        joined_names[-1] = "and " + joined_names[-1]
    joined_names_string = ", ".join(joined_names)
    item_name_converted = item["name"].replace("_", " ")
    return (
        f"{joined_names_string} _{item_name_converted}_. ColabFit, "
        f"{item['publication_year']}. https://doi.org/{item['doi']}"
    )


def write_dataset_readme(dataset_dir, ds_row, cs_exists):
    def _ensure_list(value):
        if value is None:
            return []
        if isinstance(value, str):
            try:
                return literal_eval(value)
            except (ValueError, SyntaxError):
                return [value]
        return value

    def _ensure_dict(value):
        if value is None:
            return {}
        if isinstance(value, str):
            try:
                parsed = literal_eval(value)
                return parsed if isinstance(parsed, dict) else {}
            except (ValueError, SyntaxError):
                return {}
        return value

    elements = ", ".join(_ensure_list(ds_row["elements"]))
    dslicense = ds_row["license"]
    if dslicense.lower() == "nist-pd":
        dslicense = "unknown"
    if dslicense.lower() == "cc0":
        dslicense = "cc0-1.0"
    links = _ensure_dict(ds_row["links"])
    properties_cols = ", ".join(
        [
            col.replace("_count", "").replace("_", " ")
            for col, value in ds_row.items()
            if col.endswith("count") and isinstance(value, numbers.Number) and value
        ]
    )
    citation = generate_dataset_citation_string(ds_row)
    dataset_name = ds_row["name"].replace("_", " ")

    text = (
        "---\n"
        "configs:\n"
        "- config_name: default\n"
        '  data_files: "co/*.parquet"\n'
        "- config_name: info\n"
        '  data_files: "ds.parquet"\n'
    )
    if cs_exists:
        text += (
            "- config_name: configuration_sets\n"
            '  data_files: "cs/*.parquet"\n'
            "- config_name: config_set_mapping\n"
            '  data_files: "cs_co_map/*.parquet"\n'
        )
    text += (
        "license: {license}\n"
        "tags:\n"
        "- molecular dynamics\n"
        "- mlip\n"
        "- interatomic potential\n"
        "pretty_name: {pretty_name}\n"
        "---\n"
    ).format(license=dslicense.lower().replace("-only", ""), pretty_name=dataset_name)

    text += (
        f"### <details><summary>Cite this dataset </summary>{citation}</details>  \n"
        "#### This dataset has been curated and formatted for the ColabFit Exchange  \n"
        "#### This dataset is also available on the ColabFit Exchange:  \n"
        f"https://materials.colabfit.org/id/{ds_row['id']}  \n"
        "#### Visit the ColabFit Exchange to search additional datasets by author, "
        "description, element content and more.  \nhttps://materials.colabfit.org\n<br>"
        "<hr>  \n"
        f"# Dataset  Name  \n{dataset_name}  \n"
        f"### Description  \n{ds_row['description']}  \n"
        "### Dataset authors  \n"
        f"{', '.join(_ensure_list(ds_row['authors']))}  \n"
    )
    source_publication = links.get("source-publication")
    if source_publication:
        text += f"### Publication  \n{source_publication}  \n"
    source_data = links.get("source-data")
    if source_data:
        text += f"### Original data link  \n{source_data}  \n"
    text += (
        f"### License  \n{dslicense}  \n"
        "### Number of unique molecular configurations  \n"
        f"{ds_row['nconfigurations']}  \n"
        f"### Number of atoms  \n{ds_row['nsites']}  \n"
        f"### Elements included  \n{elements}  \n"
        f"### Properties included  \n{properties_cols}  \n<br>\n"
        "<hr>  \n\n"
        "# Usage  \n"
        "- `ds.parquet` : Aggregated dataset information.  \n"
        "- `co/` directory: Configuration rows each include a structure, calculated "
        "properties, and metadata.  \n"
        "- `cs/` directory : Configuration sets are subsets of configurations grouped "
        "by some common characteristic. If `cs/` does not exist, no configurations sets "
        "have been defined for this dataset.  \n"
        "- `cs_co_map/` directory : The mapping of configurations to configuration sets "
        "(if defined).  \n<br>\n"
        "#### ColabFit Exchange documentation includes descriptions of content and "
        "example code for parsing parquet files:  \n"
        "- [Parquet parsing: example code]"
        "(https://materials.colabfit.org/docs/how_to_use_parquet)  \n"
        "- [Dataset info schema]"
        "(https://materials.colabfit.org/docs/dataset_schema)  \n"
        "- [Configuration schema]"
        "(https://materials.colabfit.org/docs/configuration_schema)  \n"
        "- [Configuration set schema]"
        "(https://materials.colabfit.org/docs/configuration_set_schema)  \n"
        "- [Configuration set to configuration mapping schema]"
        "(https://materials.colabfit.org/docs/cs_co_mapping_schema)  \n"
    )

    with open(dataset_dir / "README.md", "w") as f:
        f.write(text)
    logger.info("README written")


def process_dataset(dataset_id):
    """
    Export parquet files for a single dataset from VastDB.

    Args:
        dataset_id: Dataset ID string to export
    """
    logger.info(f"Processing dataset: {dataset_id}")
    start = time()
    output_dir = Path().cwd()

    try:
        dataset_dir = output_dir / dataset_id
        if (dataset_dir / "ds.parquet").exists():
            logger.info(f"Dataset {dataset_id} already exported, skipping")
            return
        possible_tar_file = Path("tarfiles") / f"{dataset_id}.tar.gz"
        if possible_tar_file.exists():
            logger.info(f"Dataset {dataset_id} tar file already exists, skipping")
            return
        dataset_dir.mkdir(parents=True, exist_ok=True)
        session = get_vastdb_session()
        ds_data = get_dataset_data(dataset_id, session)
        nconfigs = ds_data.column("nconfigurations")[0].as_py()
        if nconfigs > CONFIG["LARGE_DATASET_THRESHOLD"]:
            logger.info(
                f"Dataset {dataset_id} has {nconfigs} configurations. " "Using batches."
            )
            export_configurations_in_batches(dataset_id, dataset_dir, session)
        else:
            logger.info(
                f"Dataset {dataset_id} has {nconfigs} configurations. "
                "Selecting all at once."
            )
            export_configuration_parquets(dataset_id, dataset_dir, session)
        cs_ids_all = []
        if check_table_exists(session, "configuration_set_arrays"):
            cs_ids_all = export_configuration_sets(dataset_id, dataset_dir, session)
        else:
            logger.info(
                "Table configuration_set_arrays does not exist, skipping CS export"
            )

        if cs_ids_all and check_table_exists(session, "cs_co_map"):
            export_cs_co_mapping(cs_ids_all, dataset_dir, session)
        elif cs_ids_all:
            logger.info(
                "Table cs_co_map does not exist, " "skipping CS-CO mapping export"
            )
        write_dataset_parquet(ds_data, dataset_dir)
        ds_row = ds_data.to_pylist()[0]
        write_dataset_readme(dataset_dir, ds_row, bool(cs_ids_all))
    except Exception as e:
        logger.error(f"Error processing dataset {dataset_id}: {str(e)}")
    logger.info(
        f"Export completed for dataset {dataset_id} in {time() - start:.2f} seconds"
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python export_parquets_no_md.py <dataset_id>")
        sys.exit(1)

    ds_id = sys.argv[1]
    process_dataset(ds_id)
