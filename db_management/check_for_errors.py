"""
Remember that if you run this script while a job is ingesting, logs will show as
incomplete."""

from pathlib import Path


def get_nums_from_fnames(fnames):
    nums = []
    for fname in fnames:
        num = fname.split(".")[0].split("_")[-1]
        if num:
            nums.append(int(num))
    nums = sorted(nums)
    nums = str(nums)[1:-1].replace(" ", "")
    return nums


def main(path: str = None, pattern: str = None):
    
    if pattern is None and path is None:
        subdirs = [Path().cwd()]
    else:
        subdirs = sorted(list(Path(path).glob(pattern)))
    print(subdirs)
    for sd in subdirs:
        print("Processing subdir:")
        print(sd)
        if not sd.exists():
            print(f"Path to dir {sd} does not exist")
            return
        fps = sd.glob("*.out")

        fps = sorted(list(fps))
        print("Num files:")
        print(len(fps))
        time_limit = []
        disk_quota = []
        duplicates = []
        other_err = []
        oom = []
        spark_ui = []
        for fp in fps:
            with open(fp, "r") as f:
                text = f.read()
                if "Disk quota exceeded" in text:
                    disk_quota.append(fp.name)
                elif "Service 'SparkUI' failed" in text:
                    spark_ui.append(fp.name)
                elif "DUE TO TIME LIMIT" in text:
                    time_limit.append(fp.name)
                elif "OutOfMemoryError" in text:
                    oom.append(fp.name)
                elif "ValueError: Duplicate IDs found in table. Not writing" in text:
                    duplicates.append(fp.name)
                elif "Done!" in text or "Finished" in text or "Finished in" in text  or "Total runtime" in text or "Complete" in text or "finished" in text:
                    continue
                else:
                    other_err.append(fp.name)

        print("disk quota exceeded: ", str(disk_quota), sep=" ")
        print("time limit exceeded: ", str(time_limit), sep=" ")
        print("duplicates found: ", str(duplicates), sep=" ")
        print("out of memory: ", str(oom), sep=" ")
        print("spark ui not found", str(spark_ui), sep=" ")
        print("error is present, but not like above: ", str(other_err), sep=" ")
        print(
            get_nums_from_fnames(
                sorted(disk_quota + oom + time_limit + duplicates+ spark_ui + other_err)
            )
        )


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        main()
    else:
        main(sys.argv[1], sys.argv[2])
