import logging
import numbers
import os
import sys
from ast import literal_eval
from pathlib import Path
from time import sleep

import pyarrow.parquet as pq
from dotenv import load_dotenv
from huggingface_hub import HfApi

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel("INFO")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.handlers = [handler]

_HF_DATASET_TRANSLATION_TABLE = str.maketrans(
    {"(": "_", ")": "_", "@": "_", "/": "_", "+": "_"}
)


def normalize_hf_dataset_name(name: str) -> str:
    return name.translate(_HF_DATASET_TRANSLATION_TABLE)


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


def write_dataset_readme(dataset_dir, ds_row, cs_exists):

    elements = ", ".join(_ensure_list(ds_row["elements"]))
    dslicense = ds_row["license"]
    if dslicense.lower() == "nist-pd":
        dslicense = "unknown"
    if dslicense.lower() == "cco":
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
        f"https://materials.colabfit.org/id/{ds_row['id']}  \n"  # noqa: E501
        "#### Visit the ColabFit Exchange to search additional datasets by author, description, element content and more.  \nhttps://materials.colabfit.org\n<br>"  # noqa: E501
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
        "- `cs/` directory : Configuration sets are subsets of configurations grouped by some common characteristic. If `cs/` does not exist, no configurations sets have been defined for this dataset.  \n"  # noqa: E501
        "- `cs_co_map/` directory : The mapping of configurations to configuration sets (if defined).  \n<br>\n"  # noqa: E501
        "#### ColabFit Exchange documentation includes descriptions of content and example code for parsing parquet files:  \n"  # noqa: E501
        "- [Parquet parsing: example code](https://materials.colabfit.org/docs/how_to_use_parquet)  \n"  # noqa: E501
        "- [Dataset info schema](https://materials.colabfit.org/docs/dataset_schema)  \n"  # noqa: E501
        "- [Configuration schema](https://materials.colabfit.org/docs/configuration_schema)  \n"  # noqa: E501
        "- [Configuration set schema](https://materials.colabfit.org/docs/configuration_set_schema)  \n"  # noqa: E501
        "- [Configuration set to configuration mapping schema](https://materials.colabfit.org/docs/cs_co_mapping_schema)  \n"  # noqa: E501
    )
    with open(dataset_dir / "README.md", "w") as f:
        f.write(text)
    logger.info("README written")


def main():
    token = os.getenv("HF_TOKEN")
    datasets_dir = Path(
        "/scratch/gw2338/vast/data-lake-main/spark/scripts/huggingface_export"  # noqa E501
    )
    dss = sorted(
        [x for x in datasets_dir.glob("DS_*") if not str(x).endswith(".tar.gz")]
    )
    logger.info(f"Found {len(dss)} datasets to process: {dss}")
    for local_dir in dss:
        if not (local_dir / "ds.parquet").exists():
            logger.info(f"Skipping {local_dir}: not complete")
            sleep(1)
            continue
        ds_info = pq.read_table(local_dir / "ds.parquet")
        dataset_name = ds_info.column("name")[0].as_py()

        dataset_name = normalize_hf_dataset_name(dataset_name)
        logger.info(f"Processing {dataset_name}")
        repo_id = f"colabfit/{dataset_name}"
        api = HfApi(token=token)
        if api.repo_exists(repo_id=repo_id, repo_type="dataset"):
            # logger.info(f"Repo {repo_id} already exists, skipping upload")
            # For already-existing repos, delete all files and re-upload
            logger.info(f"Repo {repo_id} already exists, removing all files and dirs")
            files = api.list_repo_files(repo_id=repo_id, repo_type="dataset")
            top_level_dirs = set()
            for file_path in files:
                if ".gitattributes" in file_path:
                    continue
                top_level = file_path.split("/")[0]
                top_level_dirs.add(top_level)
            logger.info(f"Top-level directories to delete: {top_level_dirs}")
            for dir_name in top_level_dirs:
                api.delete_folder(
                    path_in_repo=dir_name,
                    repo_id=repo_id,
                    repo_type="dataset",
                    token=token,
                    commit_message=f"Delete {dir_name}",
                )
            sleep(1)
            # break
        cs_dir = local_dir / "cs"
        cs_exists = cs_dir.exists() and any(cs_dir.glob("*.parquet"))
        write_dataset_readme(local_dir, ds_info.to_pylist()[0], cs_exists)
        logger.info(f"Uploading to {repo_id}")
        repo_exists = api.repo_exists(repo_id=repo_id, repo_type="dataset")
        if not repo_exists:
            api.create_repo(repo_id=repo_id, repo_type="dataset")
        if len(list(local_dir.rglob("*.parquet"))) > 10:
            api.upload_large_folder(
                folder_path=str(local_dir),
                repo_id=repo_id,
                repo_type="dataset",
                ignore_patterns=["_SUCCESS", "*.crc"],
            )
        else:
            api.upload_folder(
                folder_path=local_dir,
                commit_message=f"Add {dataset_name} files",
                repo_id=repo_id,
                token=token,
                repo_type="dataset",
                ignore_patterns=["_SUCCESS", "*.crc"],
            )

        logger.info(f"Uploaded {dataset_name}")
        if not Path("uploaded_datasets").exists():
            os.mkdir("uploaded_datasets")
        local_dir.rename(Path("uploaded_datasets") / local_dir.name)
        logger.info(f"Moved {dataset_name} to uploaded_datasets/")
        sleep(5)

        # break
    logger.info("Done")


if __name__ == "__main__":
    logger.info("Starting upload script")
    main()
