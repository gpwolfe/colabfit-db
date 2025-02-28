from vastdb.session import Session
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()
endpoint = os.getenv("VAST_DB_ENDPOINT")
access = os.getenv("VAST_DB_ACCESS")
secret = os.getenv("VAST_DB_SECRET")
sess = Session(access=access, secret=secret, endpoint=endpoint)

date = datetime.now().strftime("%Y%m%d")
for t in ["ds", "cs", "co", "po", "cs_co_map"]:
    try:
        print(f"Renaming table {t} to {t}_{date}")
        with sess.transaction() as tx:
            table = tx.bucket("colabfit-prod").schema("prod").table(t)
            table.rename(f"{t}_{date}")
            for proj in table.projections():
                try:
                    print(f"Dropping projection {proj.name}")
                except Exception:
                    print(proj)
                proj.drop()
    except Exception as e:
        print(f"Error renaming table {t}: {e}")
        raise e

for t in ["ds", "cs", "co", "po", "cs_co_map"]:
    try:
        old_name = f"{t}_tmp"
        print(f"Renaming table {old_name} to {t}")
        with sess.transaction() as tx:
            table = tx.bucket("colabfit-prod").schema("prod").table(old_name)
            table.rename(t)
    except Exception as e:
        print(f"Error renaming table {old_name}: {e}")
        raise e
