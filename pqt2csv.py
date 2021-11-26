import sys
import os
import glob
import pandas as pd


def show_help():
    print("python pqt2csv.py [file | dir]")
    print("where file - parquet file name")
    print("      dir - all parquet files in the directory")


def get_csv_name(file):
    return "{}.csv".format(file)


def process(path):
    if os.path.isdir(path):
        os.chdir(path)
        for file in glob.glob("*.parquet"):
            convert(file, get_csv_name(file))
    elif os.path.isfile(path):
        file = os.path.basename(path)
        os.chdir(os.path.dirname(path))
        convert(path, get_csv_name(file))


def convert(source, destination):
    if os.path.exists(source):
        df = pd.read_parquet(source)
        df.to_csv(destination)
        print("File '{}' processed".format(source))


if len(sys.argv) < 2:
    show_help()
    sys.exit(1)

path = sys.argv[1]

if __name__ == "__main__":
    process(path)
