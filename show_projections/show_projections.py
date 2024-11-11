import os

import pyarrow as pa
from dotenv import load_dotenv
from vastdb.session import Session

load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
sess = Session(access=access, secret=secret, endpoint=endpoint)


def columns(self) -> pa.Schema:
    """Return this projections' columns as an Arrow schema."""
    columns = []
    next_key = 0
    while True:
        curr_columns, next_key, is_truncated, _count = (
            self.tx._rpc.api.list_projection_columns(
                self.bucket.name,
                self.schema.name,
                self.table.name,
                self.name,
                txid=self.table.tx.txid,
                next_key=next_key,
            )
        )
        if not curr_columns:
            break
        columns.extend(curr_columns)
        if not is_truncated:
            break
    self.arrow_schema = pa.schema([(col[0], col[1]) for col in columns])
    return self.arrow_schema


with sess.transaction() as tx:
    for t in ["ds", "cs", "co", "po"]:
        table = tx.bucket("colabfit-prod").schema("prod").table(t)
        print(f"{t} projections\n")
        for x in table.projections():
            print(x.name, columns(x), sep="\n", end="\n\n")
