import sys
import os
import gzip
import boto3

from dataengineeringutils3.s3 import gzip_string_write_to_s3, s3_path_to_bucket_key

from io import BytesIO, StringIO


class BaseSplitFileWriter:
    """
    Base class for splitting large datasets in to chunks and writing to s3.
    This is acts as a file like object. Data is written to an in memory file
    (note this file is defined in subclasses and not set in this base class).
    until it hits a max_bytes limit at which point the data is written to S3
    as single file. The in memory file is defined by the sub classes 
    BytesSlitFileWriter and StringSplitFileWriter. These subclasses attempt
    to mimic the expected response of BytesIO and StringIO. 

    :param s3_basepath: The base path to the s3 location you want to write to S3://...
    :param filename_prefix: The filename that you want to keep constant. Every written file is prefixed with this string.
    S3 objects written will end in the file number and the extension.
    :param max_bytes: The maximum number of bytes for each file (uncompressed file size) default set at 1GB.
    :param compress_on_upload: If the file should be compressed before writing to S3 (default True). Note does not affect 
    the file_extension parameter.
    :param file_extension: String representing the file extension. Should not be prefixed with a '.'.

    :Example:

    from dataengineeringutils3.writer import BytesSplitFileWriter
    from jsonlines import Writer

    test_dict = {"foo": "bar"}
    
    bsfw = BytesSplitFileWriter("s3://test/folder/", "test-file", max_bytes=30, compress_on_upload=False, file_extension="jsonl")

    # Same functionality passing 
    json_writer = Writer(bsfw)
    for i in range(0, 10):
        json_writer.write(test_dict)
    
    json_writer.close()

    # Closing the objects sends off the remaining data in the io buffer
    bsfw.close()
    """

    def __init__(
        self,
        s3_basepath,
        filename_prefix,
        max_bytes=1000000000,
        compress_on_upload=True,
        file_extension=None,
    ):
        self.filename_prefix = filename_prefix
        self.s3_basepath = s3_basepath
        self.max_bytes = max_bytes
        self.num_files = 0
        self.mem_file = None
        self.compress_on_upload = compress_on_upload
        self.file_extension = "" if file_extension is None else file_extension
        self.mem_file = self.get_new_mem_file()

    def __enter__(self):
        self.mem_file = self.get_new_mem_file()
        self.num_files = 0
        return self

    def __exit__(self, *args):
        self.close()

    def get_new_mem_file(self):
        """
        Overwritten by subclasses. Should return a filelike object.
        """
        return ""

    def _compress_data(self, data):
        """
        Can be overwritten by subclasses. Should return compressed data.
        """
        return gzip.compress(data)

    def write(self, b):
        self.mem_file.write(b)
        if self.file_size_limit_reached():
            self.write_to_s3()

    def writelines(self, lines):
        self.mem_file.writelines(lines)
        if self.file_size_limit_reached():
            self.write_to_s3()

    def file_size_limit_reached(self):
        if (self.max_bytes) and (self.mem_file.tell() > self.max_bytes):
            return True
        else:
            return False

    def write_to_s3(self):
        s3_resource = boto3.resource("s3")
        b, k = s3_path_to_bucket_key(self.get_s3_filepath())
        data = self.mem_file.getvalue()
        if self.compress_on_upload:
            data = self._compress_data(data)

        s3_resource.Object(b, k).put(Body=data)

        self.reset_file_buffer()

    def reset_file_buffer(self):
        self.num_files += 1
        self.mem_file.close()
        self.mem_file = self.get_new_mem_file()

    def get_s3_filepath(self):
        fn = f"{self.filename_prefix}-{self.num_files}.{self.file_extension}"
        return os.path.join(self.s3_basepath, fn)

    def close(self):
        """Write all remaining lines to a final file"""
        if self.mem_file.tell():
            self.write_to_s3()
            self.mem_file.close()


class BytesSplitFileWriter(BaseSplitFileWriter):
    """
    BytesIO file like object for splitting large datasets in to chunks and
    writing to s3. Data is written to a BytesIO file buffer until it hits a
    max_bytes limit at which point the data is written to S3
    as single file.

    :param s3_basepath: The base path to the s3 location you want to write to S3://...
    :param filename_prefix: The filename that you want to keep constant. Every written file is prefixed with this string.
    S3 objects written will end in the file number and the extension.
    :param max_bytes: The maximum number of bytes for each file (uncompressed file size) default set at 1GB.
    :param compress_on_upload: If the file should be compressed before writing to S3 (default True). Note does not affect 
    the file_extension parameter.
    :param file_extension: String representing the file extension. Should not be prefixed with a '.'.

    :Example:

    from dataengineeringutils3.writer import BytesSplitFileWriter
    from jsonlines import Writer

    test_dict = {"foo": "bar"}
    
    bsfw = BytesSplitFileWriter("s3://test/folder/", "test-file", max_bytes=30, compress_on_upload=False, file_extension="jsonl")

    # Same functionality passing 
    json_writer = Writer(bsfw)
    for i in range(0, 10):
        json_writer.write(test_dict)
    
    json_writer.close()

    # Closing the objects sends off the remaining data in the io buffer
    bsfw.close()
    """

    def get_new_mem_file(self):
        return BytesIO()


class StringSplitFileWriter(BaseSplitFileWriter):
    """
    StringIO file like object for splitting large datasets in to chunks and
    writing to s3. Data is written to a StringIO file buffer until it hits a
    max_bytes limit at which point the data is written to S3
    as single file.
    :param s3_basepath: The base path to the s3 location you want to write to S3://...
    :param filename_prefix: The filename that you want to keep constant. Every written file is prefixed with this string.
    S3 objects written will end in the file number and the extension.
    :param max_bytes: The maximum number of bytes for each file (uncompressed file size) default set at 1GB.
    :param compress_on_upload: If the file should be compressed before writing to S3 (default True). Note does not affect 
    the file_extension parameter.
    :param file_extension: String representing the file extension. Should not be prefixed with a '.'.

    :Example:

    from dataengineeringutils3.writer import StringSplitFileWriter
    from jsonlines import Writer

    test_dict = {"foo": "bar"}
    
    ssfw = StringSplitFileWriter("s3://test/folder/", "test-file", max_bytes=30, compress_on_upload=False, file_extension="jsonl")

    # Same functionality passing 
    json_writer = Writer(ssfw)
    for i in range(0, 10):
        json_writer.write(test_dict)
    
    json_writer.close()

    # Closing the objects sends off the remaining data in the io buffer
    ssfw.close()
    """

    def get_new_mem_file(self):
        return StringIO()

    def _compress_data(self, data):
        """
        Converts string data to bytes and then compresses
        """
        return gzip.compress(bytes(data, "utf-8"))


class JsonNlSplitFileWriter(BaseSplitFileWriter):
    """
    Class for writing json line into large datasets in to chunks and writing to s3.
    This class writes to a string (rather than fileIO) and does smaller checks for a speedier
    read write. Espeicially when writing multiple lines. However, if scaling to large amounts of data
    it is probably better to use a json writer like jsonlines with the BytesSplitFileWriter.
    The extension and the _write methods are defined in classes which extend this class
    lines = [
        '{"key": "value"}'
    ]
    with JsonNlSplitFileWriter("s3://test/", "test-file") as writer:
        for line in lines:
            writer.write_line(line)
    """

    def __init__(
        self, s3_basepath, filename_prefix, max_bytes=1000000000, chunk_size=1000
    ):
        super(JsonNlSplitFileWriter, self).__init__(
            s3_basepath=s3_basepath,
            filename_prefix=filename_prefix,
            max_bytes=max_bytes,
            compress_on_upload=True,
            file_extension="jsonl.gz",
        )

        self.chunk_size = chunk_size
        self.total_lines = 0
        self.num_lines = 0

    def __enter__(self):
        self.mem_file = self.get_new_mem_file()
        self.num_lines = 0
        self.num_files = 0
        return self

    def __exit__(self, *args):
        self.close()

    def write_line(self, line):
        """Writes line as string"""
        self.mem_file += f"{line}\n"
        self.num_lines += 1
        self.total_lines += 1
        if self.file_size_limit_reached():
            self.write_to_s3()

    def file_size_limit_reached(self):

        limit_met = sys.getsizeof(self.mem_file) > self.max_bytes

        if not limit_met and self.chunk_size:
            return self.num_lines >= self.chunk_size
        else:
            return limit_met

    def write_lines(self, lines, line_transform=lambda x: x):
        """
        Writes multiple lines then checks if file limit hit.
        So will be quicker but less accurate on breaking up files.
        """
        self.mem_file += "\n".join(line_transform(l) for l in lines) + "\n"
        self.num_lines += len(lines)
        self.total_lines += len(lines)
        if self.file_size_limit_reached():
            self.write_to_s3()

    def reset_file_buffer(self):
        self.num_files += 1
        self.num_lines = 0
        self.mem_file = self.get_new_mem_file()

    def write_to_s3(self):
        gzip_string_write_to_s3(self.mem_file, self.get_s3_filepath())
        self.reset_file_buffer()

    def close(self):
        """Write all remaining lines to a final file"""
        if self.num_lines:
            self.write_to_s3()
