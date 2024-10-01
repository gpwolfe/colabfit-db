from vastdb.session import Session
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
sess = Session(access=access, secret=secret, endpoint=endpoint)

date = datetime(2024, 10, 1).strftime("%Y%m%d")
for t in ["ds", "cs", "co", "po"]:
    try:
        print(f"Renaming table {t} to {t}_{date}")
        with sess.transaction() as tx:
            table = tx.bucket("colabfit-prod").schema("prod").table(t)
            table.rename(f"{t}_{date}")
    except Exception as e:
        print(f"Error renaming table {t}: {e}")
        raise e

for t in ["ds", "cs", "co", "po"]:
    try:
        old_name = f"{t}_tmp"
        print(f"Renaming table {old_name} to {t}")
        with sess.transaction() as tx:
            table = tx.bucket("colabfit-prod").schema("prod").table(old_name)
            table.rename(t)
    except Exception as e:
        print(f"Error renaming table {old_name}: {e}")
        raise e
