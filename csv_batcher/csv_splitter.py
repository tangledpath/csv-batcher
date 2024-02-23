import tempfile
from csv_batcher.utils.logger import logging

class CSVSplitter:
    def __init__(self, csv_filename, chunk_size= 10000):
        self.csv_filename = csv_filename
        self.chunk_size = chunk_size  # lines
        self.chunk_dir =  tempfile.TemporaryDirectory()
        self.chunk_files = []
        self._split()

    def _split(self):
        self.chunk_files = []
        with open(self.csv_filename, 'r') as f:
            count = 0
            header = f.readline()
            lines = []
            for line in f:
                count += 1
                lines.append(line)
                if count % self.chunk_size == 0:
                    self.write_chunk(header, count // self.chunk_size, lines)
                    lines = []

            # write remainder
            if len(lines) > 0:
                self.write_chunk((count // self.chunk_size) + 1, lines)
        logging.info(f"Split ({self.csv_filename}) into {len(self.chunk_files)}")

    def write_chunk(self, header, part, lines):
        chunk_filename = f"{self.chunk_dir.name}/data_part_{str(part)}.csv"
        with open(chunk_filename, 'w') as f_out:
            f_out.write(header)
            f_out.writelines(lines)
            self.chunk_files.append(chunk_filename)

    def csv_files(self):
        return self.chunk_files

    def cleanup(self):
        self.chunk_dir.cleanup()
