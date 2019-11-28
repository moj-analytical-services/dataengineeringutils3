import sys

import pytest

from dataengineeringutils3.s3 import gzip_string_write_to_s3
from dataengineeringutils3.writer import JsonNlSplitFileWriter
from tests.helpers import time_func


def test_json_split_file_writer(s3):
    """Test Writer splits files, gzips and sends to s3"""
    file_key = "test-key"
    bucket_name = "test"
    s3_path = f"s3://{bucket_name}/{file_key}"
    s3.meta.client.create_bucket(Bucket=bucket_name)
    bucket = s3.Bucket(bucket_name)
    with JsonNlSplitFileWriter(s3_path, 1024, 2) as writer:
        for i in range(150):
            writer.write_line(f"{i}. This test line number {i + 1}")
    keys_in_bucket = [f"s3://{bucket_name}/{o.key}" for o in bucket.objects.all()]
    files_in_bucket = len(keys_in_bucket)
    assert files_in_bucket == 5
    assert keys_in_bucket == [
        f"{s3_path}_{i}.jsonl.gz" for i in range(files_in_bucket)
    ]


MAX_BYTES = 80000
CHUNK_SIZE = 1000


def write_with_writer(result_set):
    with JsonNlSplitFileWriter(
            "s3://test/test-file.josnl.gz", MAX_BYTES, CHUNK_SIZE) as writer:
        [writer.write_line(line) for line in result_set]


def write_manually(result_set):
    string = ""
    num_files = 0
    num_lines = 0
    while True:
        for l in result_set:
            string += f"{l}"
            if not num_lines % CHUNK_SIZE and sys.getsizeof(string) > MAX_BYTES:
                gzip_string_write_to_s3(
                    string, f"s3://test/test-file-two_{num_files}.josnl.gz")
                num_files += 1
                num_lines = 0
                string = ""
            num_lines += 1
        break
    if string:
        gzip_string_write_to_s3(
            string, f"s3://test/test-file-two_{num_files}.josnl.gz")


@pytest.mark.skipif("--cov-report" in sys.argv)
def test_speed_of_writer(result_set, s3):
    """
    Test that generator is not much slower than a flat list
    """
    s3.meta.client.create_bucket(Bucket="test")

    range_time = time_func(write_manually, result_set)

    qs_time = time_func(write_with_writer, result_set)

    assert qs_time * 0.6 < range_time
