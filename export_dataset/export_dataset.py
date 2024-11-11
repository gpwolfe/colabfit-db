# Test datasets...
#                     id  nconfigurations
# 0    DS_otx1qc9f3pm4_0         19995525
# 1    DS_hu0btdblv8x6_0              500
# 2    DS_4lev0d7cs1yl_0             6550

# ORDER BY nsites DESC...
# +-----------------+----------+
# |id               |nsites    |
# +-----------------+----------+
# |DS_dgop3abwq9ya_0|7466801654|
# |DS_otx1qc9f3pm4_0|1464936288|
# |DS_jgaid7espcoc_0|669615870 |

# >>> exporter.export_dataset("DS_hu0btdblv8x6_0", True)
# [0:00:00.659020] Wrote dataset.json
# [0:00:01.081975] Wrote 0 configuration sets to json
# [0:00:14.089735] Wrote 500 configurations to parquet in 182 batches
# [0:00:12.154911] Wrote 500 property objects to parquet in 259 batches
# [0:00:01.704806] Successfully downloaded 501 metadata files
# $ du -sh *
# 3.9M    configs.parquet
# 528K    data
# 512     dataset.json
# 4.8M    properties.parquet
# Compressed:
# 2.4M    export_DS_hu0btdblv8x6_0_20240909-1223.tar.gz

# >>> exporter.export_dataset("DS_otx1qc9f3pm4_0")
# [0:00:05.491815] Wrote dataset.json
# [0:00:00.048012] Wrote 0 configuration sets to json
# [0:14:29.889110] Wrote 19995525 configurations to parquet in 244 batches
# $ du -sh *
# 36G     configs.parquet
# 512     dataset.json

from __future__ import print_function

import datetime
import os
import sys
import tarfile
from concurrent import futures
from sys import argv

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import vastdb
from dotenv import load_dotenv

from colabfit.tools.database import S3FileManager


class ColabfitExporter:
    def __init__(self, access_key, secret_key):
        self.session = None
        self.database = "colabfit-prod"
        self.schema = "prod"
        self.data_bucket = "colabfit-data"
        self.export_root_dir = "/scratch/work/martiniani/for_gregory/tmp"

        self.endpoint = "http://10.32.38.210"
        self.session = vastdb.connect(
            endpoint=self.endpoint, access=access_key, secret=secret_key
        )
        self.boto_client = boto3.client(
            "s3",
            use_ssl=False,
            endpoint_url=self.endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="fake-region",
            config=boto3.session.Config(
                signature_version="s3v4", s3={"addressing_style": "path"}
            ),
        )
        # s = S3FileManager(
        #     bucket_name=self.data_bucket,
        #     endpoint=self.endpoint,
        #     access_id=access_key,
        #     secret_key=secret_key,
        # )

    def _download_file(self, key, export_dir):
        file = os.path.join(export_dir, key)
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with open(file, "wb") as data:
            self.boto_client.download_fileobj(self.data_bucket, key, data)
        return file

    def _download_files(self, keys, export_dir):
        with futures.ThreadPoolExecutor() as executor:
            future_to_key = {
                executor.submit(self._download_file, key, export_dir): key
                for key in keys
            }
            for future in futures.as_completed(future_to_key):
                key = future_to_key[future]
                yield key, future

    def export_dataset(self, dataset_id, include_unstructured_metadata=False):
        # Create the export directory
        ts = datetime.datetime.now().strftime("%Y%m%d-%H%M")
        export_id = f"export_{dataset_id}_{ts}"
        export_dir = os.path.join(self.export_root_dir, export_id)
        os.makedirs(export_dir, exist_ok=False)
        start = datetime.datetime.now()

        # Get the DS
        with self.session.transaction() as tx:
            table = tx.bucket(self.database).schema(self.schema).table("ds")
            ds = table.select(predicate=(table["id"] == dataset_id)).read_all()
            if len(ds) != 1:
                raise KeyError(f"Found {len(ds)} datasets with id {dataset_id}")
            # TODO: Format to write this?
            with open(os.path.join(export_dir, "dataset.json"), "w") as f:
                json_str = ds.to_pandas().to_json(
                    f, orient="records", index=False, force_ascii=False, indent=1
                )
        ds_finished = datetime.datetime.now()
        print(f"[{str(ds_finished - start)}] Wrote dataset.json")

        # Get the CSs
        with self.session.transaction() as tx:
            table = tx.bucket(self.database).schema(self.schema).table("cs")
            cs = table.select(predicate=(table["dataset_id"] == dataset_id)).read_all()
            if len(cs) > 0:
                # TODO: Format to write this?
                # writer = pq.ParquetWriter(
                #     os.path.join(export_dir, "configuration_sets.parquet"),
                #     cs.schema,
                # )
                # writer.write_table(cs)
                with open(
                    os.path.join(export_dir, "configuration_sets.json"), "w"
                ) as f:
                    json_str = cs.to_pandas().to_json(
                        f, orient="records", index=False, force_ascii=False, indent=1
                    )

        cs_finished = datetime.datetime.now()
        print(
            f"[{str(cs_finished - ds_finished)}] Wrote {len(cs)} configuration sets to json"
        )

        # Get the COs
        co_metadata_paths = set()
        with self.session.transaction() as tx:
            table = tx.bucket(self.database).schema(self.schema).table("co")
            co_table = table.select(
                predicate=(table["dataset_ids"].contains(dataset_id))
            )
            nconfigs, nbatches = 0, 0
            writer = pq.ParquetWriter(
                os.path.join(export_dir, "configs.parquet"), co_table.schema
            )
            for batch in iter(co_table.read_next_batch, None):
                df = batch.to_pandas()
                nbatches += 1
                nconfigs += batch.num_rows
                # Normalize metadata_path to relative path
                df["metadata_path"] = df["metadata_path"].apply(
                    lambda path: path.split("colabfit-data/")[1]
                )
                # # Derive partition column from hash
                # df["partition"] = df["hash"].apply(lambda h: int(h[:1]))
                if include_unstructured_metadata:
                    co_metadata_paths = co_metadata_paths.union(
                        set(df["metadata_path"])
                    )
                writer.write_batch(
                    pa.RecordBatch.from_pandas(df, schema=co_table.schema)
                )
            writer.close()
        co_finished = datetime.datetime.now()
        print(
            f"[{str(co_finished - cs_finished)}] Wrote {nconfigs} configurations to parquet in {nbatches} batches"
        )

        # Get the POs
        po_metadata_paths = set()
        with self.session.transaction() as tx:
            table = tx.bucket(self.database).schema(self.schema).table("po")
            po_table = table.select(predicate=(table["dataset_id"] == dataset_id))
            nprops, nbatches = 0, 0
            writer = pq.ParquetWriter(
                os.path.join(export_dir, "properties.parquet"), po_table.schema
            )
            for batch in iter(po_table.read_next_batch, None):
                df = batch.to_pandas()
                nbatches += 1
                nprops += batch.num_rows
                # Normalize metadata_path to relative path
                df["metadata_path"] = df["metadata_path"].apply(
                    lambda path: path.split("colabfit-data/")[1]
                )
                # # Derive partition column from hash
                # df["partition"] = df["hash"].apply(lambda h: int(h[:1]))
                if include_unstructured_metadata:
                    po_metadata_paths = po_metadata_paths.union(
                        set(df["metadata_path"])
                    )
                writer.write_batch(
                    pa.RecordBatch.from_pandas(df, schema=po_table.schema)
                )
            writer.close()
        po_finished = datetime.datetime.now()
        print(
            f"[{str(po_finished - co_finished)}] Wrote {nprops} property objects to parquet in {nbatches} batches"
        )

        # Get the metadata
        metadata = co_metadata_paths.union(po_metadata_paths)
        download_count = 0
        for key, result in self._download_files(metadata, export_dir):
            if result.exception():
                print(f"{key}: {result.exception()}")
            else:
                download_count += 1
        md_finished = datetime.datetime.now()
        print(
            f"[{str(md_finished - po_finished)}] Successfully downloaded {download_count} metadata files"
        )

        # Compress the export dir
        tar_filename = f"{export_id}.tar.gz"
        with tarfile.open(
            os.path.join(self.export_root_dir, tar_filename), "w:gz"
        ) as tar:
            tar.add(export_dir, arcname=os.path.basename(export_dir))


if __name__ == "__main__":
    dataset_id = sys.argv[1]
    load_dotenv()
    assert "VAST_DB_ACCESS" in os.environ, "VAST_DB_ACCESS not found in environment"
    assert "VAST_DB_SECRET" in os.environ, "VAST_DB_SECRET not found in environment"
    exporter = ColabfitExporter(
        os.getenv("VAST_DB_ACCESS"), os.getenv("VAST_DB_SECRET")
    )
    exporter.export_dataset(dataset_id, True)
