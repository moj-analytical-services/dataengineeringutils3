import sys

from dataengineeringutils3.s3 import gzip_string_write_to_s3


class SplitFileWriter:
    extension = ""

    def __init__(self, outfile_path, max_bytes, chunk_size):
        self.outfile_path = outfile_path
        self.max_bytes = max_bytes
        self.chunk_size = chunk_size
        self.string = ""
        self.lines = 0
        self.files = 0

    def write_line(self, line):
        self.string += "%s\n" % line
        self.lines += 1
        if not self.lines % self.chunk_size \
                and sys.getsizeof(self.string) > self.max_bytes:
            self.write_file()

    def _write(self, file_path):
        raise NotImplementedError()

    def write_file(self):
        self._write(f"{self.outfile_path}_{self.files}{self.extension}")
        self.files += 1
        self.string = ""
        self.lines = 0

    def close(self):
        if self.lines:
            self.write_file()


class JsonNlSplitFileWriter(SplitFileWriter):
    extension = ".jsonl.gz"

    def _write(self, file_path):
        gzip_string_write_to_s3(self.string, file_path)
