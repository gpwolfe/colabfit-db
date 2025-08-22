from huggingface_hub import HfApi, delete_repo
from pathlib import Path
import os
from time import sleep


def main():
    token = os.getenv("HF_TOKEN")
    datasets_dir = Path(
        "/scratch/gw2338/vast/data-lake-main/spark/scripts/export_dataset_huggingface/datasets"  # noqa E501
    )

    dss = sorted(list(datasets_dir.glob("*")))
    for local_dir in dss:
        if not (local_dir / "README.md").exists():
            print(f"Skipping {local_dir}: not complete")
            sleep(5)
            continue
        # dataset_name = local_dir.parts[-1]
        dataset_name = (
            local_dir.parts[-1]
            .replace("@", "_")
            .replace("(", "_")
            .replace(")", "_")
            .replace("/", "_")
            .replace("+", "_")
        )
        print(f"Processing {dataset_name}")
        repo_id = f"colabfit/{dataset_name}"
        api = HfApi()
        if api.repo_exists(repo_id=repo_id, repo_type="dataset"):
            print(f"Repo {repo_id} already exists, skipping upload")
            # print(f"Deleting {repo_id}")
            # delete_repo(
            #     repo_id=repo_id,
            #     token=token,
            #     repo_type="dataset",
            # )
            sleep(5)
            continue
        ds_file_dir = local_dir / dataset_name
        if not ds_file_dir.exists():
            print(f"ds_file_dir {ds_file_dir} does not exist")
            continue
        readme = local_dir / "README.md"
        readmetext = readme.read_text()
        if '"license' in readmetext:
            readmetext = (
                readmetext.replace(
                    '"license',
                    "license",
                    1,
                )
                # .replace("data_files: *.parquet", 'data_files: "main/*.parquet"')
                .replace("license: nist-pd", "license: unknown", 1)
            )
            readme.write_text(readmetext)
        print(repo_id)
        repo_exists = api.repo_exists(repo_id=repo_id, repo_type="dataset")
        if not repo_exists:
            api.create_repo(repo_id=repo_id, repo_type="dataset")
        api.upload_file(
            path_or_fileobj=str(readme),
            path_in_repo="README.md",
            commit_message=f"Add {dataset_name} readme file",
            repo_id=repo_id,
            token=token,
            repo_type="dataset",
        )
        api.upload_folder(
            folder_path=str(ds_file_dir),
            path_in_repo="main",
            commit_message=f"Add {dataset_name} data files",
            repo_id=repo_id,
            repo_type="dataset",
            token=token,
            ignore_patterns=["_SUCCESS", "*.crc"],
        )
        # local_dir.rename(local_dir.parents[1] / "uploaded_datasets" / dataset_name)
        print(f"Uploaded {dataset_name}")
        sleep(20)
        # break
    print("Done")


if __name__ == "__main__":
    main()
