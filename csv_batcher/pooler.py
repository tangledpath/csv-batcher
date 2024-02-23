from multiprocessing import Pool
from typing import Callable

import pandas as pd

from csv_splitter import CSVSplitter


class Pooler:
    def __init__(
        self,
        csv_filename: str,
        process_fn: Callable,
        as_dataframe: bool = False,
        pool_size: int = 8,
        chunk_size: int = 10000,
    ):
        """
        Construct `Pooler` with given `csv_filename`, `process_fn`, `as_dataframe`, `pool_size', 'chunk_size'

        Args:
            csv_filename (str): Name of CSV file
            process_fn (Callable): A function that accepts a single argument
                By default, this is the path to a chunked CSV file
                If `as_dataframe` is True, then the argument sent is a dataframe of the chunked CSV
            as_dataframe (bool): When true, a dataframe is created with the chunked CSV File and
                that is sent instead.  Defaults to False.
            pool_size (int, optional): Number of workers to uses. Defaults to 8.
            chunk_size (int, optional): Target row count for each chunked CSV. Last chunk may
                have fewer rows.  Defaults to 10000.
        """
        self.csv_filename = csv_filename
        self.process_fn = process_fn
        self.pool_size = pool_size
        self.chunk_size = chunk_size
        self.processed_count = 0

        print("process_fn", process_fn)

    def process(self):
        self.processed_count = 0
        csv_splitter = CSVSplitter(self.csv_filename, self.chunk_size)
        try:
            print(f"Pooling against {len(csv_splitter.csv_files())} files")
            with Pool(5) as p:
                for result in p.imap(self._process_csv, csv_splitter.csv_files()):
                    pass
                    # print(result)
        finally:
            csv_splitter.cleanup()

    def _process_csv(self, csv_chunk_filename):
        if (as_dataframe):
            df = pd.read_csv(csv_chunk_filename, skipinitialspace=True, index_col=None)
            self.process_fn(df)
        else:
            self.process_fn(csv_chunk_filename)



# def test_apply(row):
#     return row.iloc[0]

# def process(chunk):
#     # print("processing")
#     df = pd.read_csv(chunk, skipinitialspace=True, index_col=None)
#     foo = df.apply(test_apply, axis=1)
#     return len(df)

# @time_and_log("test_batching")
# def test_csv_batching():
#     csv_splitter = CSVSplitter("5mSalesRecords.csv", 100000)
#     try:
#         print(f"Pooling against {len(csv_splitter.csv_files())} files")
#         with Pool(5) as p:
#             for result in p.imap(process, csv_splitter.csv_files()):
#                 print(result)

#     finally:
#         csv_splitter.cleanup()


# if __name__ == "__main__":
#     test_csv_batching()
