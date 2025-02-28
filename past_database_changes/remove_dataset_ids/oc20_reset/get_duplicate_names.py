from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as sf
from ast import literal_eval

spark = SparkSession.builder.appName("get_duplicate_names").getOrCreate()
cos = spark.table("ndb.colabfit.dev.co_oc_reset")
cos = cos.filter(sf.col("dataset_ids").contains("DS_otx1qc9f3pm4_0"))
# pos = spark.table("ndb.colabfit.dev.po_oc_reset").filter(
#     'dataset_id =="DS_otx1qc9f3pm4_0"'
# )
# pos = pos.filter("multiplicity > 1").select("configuration_id")
# cids = [x["configuration_id"] for x in pos.collect()]
# batched_cids = [cids[i : i + 100] for i in range(0, len(cids), 100)]
# names = set()


@sf.udf(StringType())
def get_number(names):
    names = literal_eval(names)
    for name in names:
        if "OC20_S2EF_train_20M" not in name:
            continue
        else:
            name = name.replace("OC20_S2EF_train_20M__file_", "").split("__config")[0]
            return int(name)
    return None


missing_nums = []
cos = cos.withColumn("name_number", get_number("names"))
cos = cos.select("name_number").distinct().collect()
for num in range(0, 4000):
    if num not in [x["name_number"] for x in cos]:
        missing_nums.append(num)
print(missing_nums)
# for batch in batched_cids:
#     name = cos.filter(sf.col("id").isin(batch)).select("names").collect()
#     name = [x["names"] for x in name]
#     names.update([x.split("__config__")[0] for x in name])
with open("files_missing.txt", "w") as f:
    print(missing_nums, file=f)
