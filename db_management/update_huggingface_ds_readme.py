import logging
import numbers
import os
import sys
from ast import literal_eval
from pathlib import Path
from time import sleep

import pyarrow as pa
import vastdb
from dotenv import load_dotenv
from huggingface_hub import HfApi

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel("INFO")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.handlers = [handler]

CONFIG = {
    "COMPRESSION_LEVEL": 18,
}

_HF_DATASET_TRANSLATION_TABLE = str.maketrans(
    {"(": "_", ")": "_", "@": "_", "/": "_", "+": "_"}
)


def normalize_hf_dataset_name(name: str) -> str:
    return name.translate(_HF_DATASET_TRANSLATION_TABLE)


def get_vastdb_session():
    endpoint = "http://10.32.38.210"
    with open(f"/home/{os.environ['USER']}/.vast-dev/access_key_id", "r") as f:
        access_key = f.read().rstrip("\n")
    with open(f"/home/{os.environ['USER']}/.vast-dev/secret_access_key", "r") as f:
        secret_key = f.read().rstrip("\n")
    return vastdb.connect(endpoint=endpoint, access=access_key, secret=secret_key)


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


def get_dataset_data(dataset_id, session):
    with session.transaction() as tx:
        ds_table = tx.bucket("colabfit-prod").schema("prod").table("dataset_arrays")
        ds_data = ds_table.select(predicate=ds_table["id"] == dataset_id)
        ds_data = ds_data.read_all()
        logger.info(f"Read DS rows: {ds_data.num_rows}")
    return ds_data


def write_dataset_parquet(ds_data, dataset_dir):
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
    joined_names_string = None
    joined_names = []

    for author in _ensure_list(item["authors"]):
        name_parts_orig = author.split(" ")
        name_parts_new = []
        family_name = name_parts_orig.pop()
        for name_part in name_parts_orig:
            if name_part[0].islower():
                continue
            s = name_part[0] + "."
            name_parts_new.append(s)

        formatted_name = family_name + ", " + " ".join(name_parts_new)
        joined_names.append(formatted_name)

    if len(joined_names) > 1:
        joined_names[-1] = "and " + joined_names[-1]

    joined_names_string = ", ".join(joined_names)
    item_name_converted = item["name"].replace("_", " ")
    citation_string = (
        f"{joined_names_string} _{item_name_converted}_. ColabFit, "
        f"{item['publication_year']}. https://doi.org/{item['doi']}"
    )
    return citation_string


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
        "#### Visit the ColabFit Exchange to search additional datasets by author, description, element content and more.  \nhttps://materials.colabfit.org\n<br>"
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
        "- `co/` directory: Configuration rows each include a structure, calculated properties, and metadata.  \n"
        "- `cs/` directory : Configuration sets are subsets of configurations grouped by some common characteristic. If `cs/` does not exist, no configurations sets have been defined for this dataset.  \n"
        "- `cs_co_map/` directory : The mapping of configurations to configuration sets (if defined).  \n<br>\n"
        "#### ColabFit Exchange documentation includes descriptions of content and example code for parsing parquet files:  \n"
        "- [Parquet parsing: example code](https://materials.colabfit.org/docs/how_to_use_parquet)  \n"
        "- [Dataset info schema](https://materials.colabfit.org/docs/dataset_schema)  \n"
        "- [Configuration schema](https://materials.colabfit.org/docs/configuration_schema)  \n"
        "- [Configuration set schema](https://materials.colabfit.org/docs/configuration_set_schema)  \n"
        "- [Configuration set to configuration mapping schema](https://materials.colabfit.org/docs/cs_co_mapping_schema)  \n"
    )

    with open(dataset_dir / "README.md", "w") as f:
        f.write(text)
    logger.info("README written")


def get_cs_exists(dataset_id):
    with get_vastdb_session().transaction() as tx:
        cs_table = (
            tx.bucket("colabfit-prod").schema("prod").table("configuration_set_arrays")
        )
        rec_batch_reader = cs_table.select(
            predicate=cs_table["dataset_id"] == dataset_id,
            columns=["dataset_id"],
            limit_rows=1,
        )
        cs_data = rec_batch_reader.read_all()
        if cs_data.num_rows > 0:
            logger.info(f"Configuration sets exist for dataset {dataset_id}")
            return True
    logger.info(f"No configuration sets for dataset {dataset_id}")
    return False


def update_dataset_files(dataset_id, local_output_dir=None):
    """
    Regenerate ds.parquet and README.md for a single dataset

    Args:
        dataset_id: The dataset ID to update
        local_output_dir: Directory where dataset files are stored (defaults to CWD)
    """
    if local_output_dir is None:
        local_output_dir = Path.cwd()
    else:
        local_output_dir = Path(local_output_dir)

    dataset_dir = local_output_dir / dataset_id

    if not dataset_dir.exists():
        os.mkdir(dataset_dir)

    logger.info(f"Updating files for dataset {dataset_id}")

    # Get dataset data from VastDB
    session = get_vastdb_session()
    ds_data = get_dataset_data(dataset_id, session)

    if ds_data.num_rows == 0:
        logger.warning(f"Dataset {dataset_id} has no dataset rows")
        return False

    ds_row = ds_data.to_pylist()[0]

    # Write ds.parquet
    write_dataset_parquet(ds_data, dataset_dir)

    # Check if cs directory exists to determine if we have configuration sets

    cs_exists = get_cs_exists(dataset_id)

    # Write README.md
    write_dataset_readme(dataset_dir, ds_row, cs_exists)

    logger.info(f"Successfully updated ds.parquet and README.md for {dataset_id}")
    return True


def upload_to_huggingface(dataset_id, dataset_dir, token=None):
    """
    Upload updated ds.parquet and README.md to HuggingFace

    Args:
        dataset_id: The dataset ID
        dataset_dir: Path to the dataset directory
        token: HuggingFace API token (defaults to HF_TOKEN env var)
    """
    if token is None:
        token = os.getenv("HF_TOKEN")

    if not token:
        logger.error("HF_TOKEN not found in environment variables")
        return False

    # Read dataset name from ds.parquet
    ds_data = pa.parquet.read_table(dataset_dir / "ds.parquet")
    dataset_name = ds_data.column("name")[0].as_py()
    dataset_name = normalize_hf_dataset_name(dataset_name)

    repo_id = f"colabfit/{dataset_name}"

    logger.info(f"Uploading to {repo_id}")

    api = HfApi(token=token)

    if not api.repo_exists(repo_id=repo_id, repo_type="dataset"):
        logger.error(f"Repository {repo_id} does not exist on HuggingFace")
        return False

    # Upload README.md
    readme_path = dataset_dir / "README.md"
    if readme_path.exists():
        api.upload_file(
            path_or_fileobj=str(readme_path),
            path_in_repo="README.md",
            commit_message=f"Update README.md for {dataset_name}",
            repo_id=repo_id,
            token=token,
            repo_type="dataset",
        )
        logger.info("Uploaded README.md")

    # Upload ds.parquet
    ds_path = dataset_dir / "ds.parquet"
    if ds_path.exists():
        api.upload_file(
            path_or_fileobj=str(ds_path),
            path_in_repo="ds.parquet",
            commit_message=f"Update ds.parquet for {dataset_name}",
            repo_id=repo_id,
            token=token,
            repo_type="dataset",
        )
        logger.info("Uploaded ds.parquet")

    logger.info(f"Successfully uploaded files for {dataset_name}")
    return True


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: python update_huggingface_ds_readme.py <dataset_ids_file> [--upload]"
        )
        print("  dataset_ids_file: Path to file with dataset IDs (one per line)")
        print("  --upload: Optional flag to upload to HuggingFace after updating")
        sys.exit(1)

    dataset_ids_file = sys.argv[1]
    upload_flag = "--upload" in sys.argv

    if not Path(dataset_ids_file).exists():
        logger.error(f"Dataset IDs file {dataset_ids_file} does not exist")
        sys.exit(1)

    # Read dataset IDs from file
    with open(dataset_ids_file, "r") as f:
        dataset_ids = [line.strip() for line in f.readlines() if line.strip()]

    logger.info(f"Found {len(dataset_ids)} datasets to update")

    for i, dataset_id in enumerate(dataset_ids, 1):
        logger.info(f"Processing dataset {i}/{len(dataset_ids)}: {dataset_id}")

        try:
            success = update_dataset_files(dataset_id)

            if success and upload_flag:
                upload_to_huggingface(dataset_id, Path.cwd() / dataset_id)
                sleep(2)  # Small delay between uploads

        except Exception as e:
            logger.error(f"Error processing dataset {dataset_id}: {str(e)}")
            continue

    logger.info("Done updating all datasets")


if __name__ == "__main__":
    main()
