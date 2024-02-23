from multiprocessing import Pool
from typing import Callable
from csv_batcher.utils.logger import logging
import pandas as pd

from csv_batcher.csv_splitter import CSVSplitter


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
        self.as_dataframe = as_dataframe
        self.pool_size = pool_size
        self.chunk_size = chunk_size

    def process(self):
        processed_count = 0
        csv_splitter = CSVSplitter(self.csv_filename, self.chunk_size)
        try:
            csv_file_cnt = len(csv_splitter.csv_files())
            logging.info(f"Pooling against {csv_file_cnt} files")
            with Pool(5) as p:
                for result in p.imap(self._process_csv, csv_splitter.csv_files()):
                    processed_count += result
        finally:
            csv_splitter.cleanup()

        logging.info(
            f"Processed {processed_count} rows from {csv_file_cnt} CSV Files"
        )

    def _process_csv(self, csv_chunk_filename):
        if self.as_dataframe:
            df = pd.read_csv(csv_chunk_filename, skipinitialspace=True, index_col=None)
            result = df.shape[0]
            self.process_fn(df)
        else:
            self.process_fn(csv_chunk_filename)
            with open(csv_chunk_filename) as f:
                # Get total lines and subtract for header:
                result = sum(1 for line in f) - 1
        return result
