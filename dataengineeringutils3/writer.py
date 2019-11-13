import sys

from dataengineeringutils3.s3 import gzip_string_write_to_s3


class SplitFileWriter:
    """
    Base class for splitting large datasets in to chunks and writing to s3

    The extension and the _write methods are defined in classes which extend this class

    lines = [
        '{"key": "value"}'
    ]
    with JsonNlSplitFileWriter("s3://test/test-file.josnl.gz") as writer:
        for line in lines:
            writer.write_line(line)
    """
    extension = ""

    def __init__(self, outfile_path, max_bytes=800000000, chunk_size=1000):
        """
        Set the outfile path and specify the chunk byte size and line number chunk size

        :param outfile_path: string: s3://test/test-file.jsonl.gz
        :param max_bytes: int: Bytes to chunk file to: 800000000
        :param chunk_size: int: number of rows  check file size for chunking: 1000
        """
        self.outfile_path = outfile_path.replace(self.extension, "")
        self.max_bytes = max_bytes
        self.chunk_size = chunk_size
        self.string = ""
        self.total_lines = 0
        self.num_lines = 0
        self.num_files = 0

    def __enter__(self):
        self.string = ""
        self.num_lines = 0
        self.num_files = 0
        return self

    def __exit__(self, *args):
        self.close()

    def write_line(self, line):
        """Writes line as string"""
        self.string += f"{line}\n"
        self.num_lines += 1
        if not self.num_lines % self.chunk_size \
                and sys.getsizeof(self.string) > self.max_bytes:
            self.write_file()

    def _write(self, file_path):
        """Writes file part to local storage"""
        with open(file_path, "rb") as f:
            f.write(bytes(self.string, 'utf-8'))

    def write_file(self):
        """Writes and updates number of files and sets lines to 0"""
        self._write(f"{self.outfile_path}_{self.num_files}{self.extension}")
        self.num_files += 1
        self.string = ""
        self.total_lines += self.num_lines
        self.num_lines = 0

    def close(self):
        """Write all remaining lines to a final file"""
        if self.num_lines:
            self.write_file()


class JsonNlSplitFileWriter(SplitFileWriter):
    extension = ".jsonl.gz"

    def _write(self, file_path):
        """Gzip string to file path in s3"""
        gzip_string_write_to_s3(self.string, file_path)
