import sys
import os
import glob
import pandas as pd


def show_help():
    print("python csv2pqt.py [file | dir]")
    print("where file - csv file name")
    print("      dir - all csv files in the directory")


def get_pqt_name(file):
    return "{}.parquet".format(file)


def process(path):
    if os.path.isdir(path):
        os.chdir(path)
        for file in glob.glob("*.csv"):
            convert(file, get_pqt_name(file))
    elif os.path.isfile(path):
        file = os.path.basename(path)
        os.chdir(os.path.dirname(path))
        convert(path, get_pqt_name(file))


def convert(source, destination):
    if os.path.exists(source):
        df = pd.read_csv(source)
        df.to_parquet(destination)
        print("File '{}' processed".format(source))


if len(sys.argv) < 2:
    show_help()
    sys.exit(1)

path = sys.argv[1]

if __name__ == "__main__":
    process(path)
