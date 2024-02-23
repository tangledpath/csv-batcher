import unittest
from csv_batcher.utils.time import time_and_log
from csv_batcher.pooler import Pooler
import pandas as pd


def df_apply(row):
    return row.iloc[0]


# @time_and_log("test_big_file")
def process_to_dataframe(csv_chunk_filename):
    # print("processing ", csv_chunk_filename)
    df = pd.read_csv(csv_chunk_filename, skipinitialspace=True, index_col=None)
    foo = df.apply(df_apply, axis=1)
    return len(df)


def process_as_dataframe(df):
    foo = df.apply(df_apply, axis=1)
    return len(df)


class TestPooler(unittest.TestCase):
    def test_big_file(self):
        with time_and_log("test_big_file"):
            pooler = Pooler("5mSalesRecords.csv", process_to_dataframe)
            pooler.process()

    def test_big_file_as_dataframe(self):
        with time_and_log("test_big_file_pool_chunk"):
            pooler = Pooler("5mSalesRecords.csv", process_as_dataframe, as_dataframe=True, pool_size=16)
            pooler.process()

    # @unittest.skip("takes long")
    def test_no_pooler(self):
        with time_and_log("test_no_pooler"):
            process_to_dataframe("5mSalesRecords.csv")


if __name__ == "__main__":
    unittest.main()
