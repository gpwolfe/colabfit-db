from __future__ import print_function

import datetime
import os
import sys
import vastdb


def get_vastdb_session():
    endpoint = "http://10.32.38.210"
    with open(f"/home/{os.environ['USER']}/.vast-dev/access_key_id", "r") as f:
        access_key = f.read().rstrip("\n")
    with open(f"/home/{os.environ['USER']}/.vast-dev/secret_access_key", "r") as f:
        secret_key = f.read().rstrip("\n")
    return vastdb.connect(endpoint=endpoint, access=access_key, secret=secret_key)


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: copy_table <src> <dest>", file=sys.stderr)
        exit(-1)
    SRC = sys.argv[1]
    DEST = sys.argv[2]
    print(f"Copying {SRC} to {DEST}")
    start = datetime.datetime.now()

    session = get_vastdb_session()
    with session.transaction() as tx:
        # Source table
        src_path = SRC.split(".")
        src_table = tx.bucket(src_path[0]).schema(src_path[1]).table(src_path[2])
        src_reader = src_table.select()

        # Destination table
        dest_path = DEST.split(".")
        dest_schema = tx.bucket(dest_path[0]).schema(dest_path[1])
        dest_table = dest_schema.create_table(dest_path[2], src_reader.schema)

        # Copy
        num_batches = 0
        for batch in iter(src_reader.read_next_batch, None):
            dest_table.insert(batch)
            num_batches += 1

    print(f"Copied {SRC} to {DEST}")
    print(f"Total batches: {num_batches}")
    print(f"Total runtime: {str(datetime.datetime.now() - start)}")
