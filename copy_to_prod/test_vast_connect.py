from vastdb.session import Session
from dotenv import load_dotenv
import os

load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
sess = Session(endpoint=endpoint, access=access, secret=secret)
for t in ["co"]:
    with sess.transaction() as tx:
        table = tx.bucket("colabfit").schema("dev").table("co")
        print(table.get_stats())
    with sess.transaction() as tx:
        table = tx.bucket("colabfit-prod").schema("prod").table(t)
        print(table)
