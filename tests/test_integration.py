import sys
from unittest.mock import call

from dataengineeringutils3.db import SelectQuerySet
from dataengineeringutils3.s3 import gzip_string_write_to_s3
from dataengineeringutils3.writer import JsonNlSplitFileWriter
from tests.helpers import time_func
from tests.mocks import MockQs


FILE_KEY = "test-key"
BUCKET_NAME = "test"
S3_BASEPATH = f"s3://{BUCKET_NAME}/"
MAX_BYTES = 80000
CHUNK_SIZE = 1000


def test_large_select_queryset_with_writer(s3, large_select_queryset):
    """
    Test file writer and queryset with large result set.
    """
    s3.meta.client.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    bucket = s3.Bucket(BUCKET_NAME)
    with JsonNlSplitFileWriter(S3_BASEPATH, FILE_KEY, 1024, 10000) as writer:
        for rows in large_select_queryset.iter_chunks():
            writer.write_lines(rows)

    keys_in_bucket = [f"s3://{BUCKET_NAME}/{o.key}" for o in bucket.objects.all()]
    files_in_bucket = len(keys_in_bucket)
    assert files_in_bucket == 10

    large_select_queryset.cursor.fetchmany.assert_has_calls(
        [
            call(10000),
            call(10000),
            call(10000),
            call(10000),
            call(10000),
            call(10000),
            call(10000),
            call(10000),
            call(10000),
            call(10000),
            call(10000),
        ]
    )


def write_with_writer_and_qs(result_set):
    select_queryset = SelectQuerySet(MockQs(result_set), "", 10000)

    with JsonNlSplitFileWriter(S3_BASEPATH, FILE_KEY, MAX_BYTES, CHUNK_SIZE) as writer:
        for results in select_queryset.iter_chunks():
            writer.write_lines(results)


def write_with_write_to_file(result_set):
    select_queryset = SelectQuerySet(MockQs(result_set), "", 10000)

    def transform_line(x):
        return x

    with JsonNlSplitFileWriter(S3_BASEPATH, FILE_KEY, MAX_BYTES, CHUNK_SIZE) as writer:
        select_queryset.write_to_file(writer, transform_line)


def write_manually(result_set):
    string = ""
    num_files = 0
    num_lines = 0

    def transform_line(x):
        return f"{x}"

    while True:
        for line in result_set:
            string += transform_line(line)
            if not num_lines % CHUNK_SIZE and sys.getsizeof(string) > MAX_BYTES:
                gzip_string_write_to_s3(
                    string, f"s3://test/test-file-two_{num_files}.josnl.gz"
                )
                num_files += 1
                num_lines = 0
                string = ""
            num_lines += 1
        break
    if string:
        gzip_string_write_to_s3(string, f"s3://test/test-file-two_{num_files}.josnl.gz")


def test_speed_of_writer_and_iterator(result_set, s3):
    """
    Test that generator is not much slower than a flat list
    """
    s3.meta.client.create_bucket(
        Bucket="test", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
    )

    range_time = time_func(write_manually, result_set)

    qs_time = time_func(write_with_writer_and_qs, result_set)

    assert qs_time < range_time


def test_speed_of_write_to_file(result_set, s3):
    """
    Test that generator is not much slower than a flat list
    """
    s3.meta.client.create_bucket(
        Bucket="test", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
    )

    range_time = time_func(write_manually, result_set)

    write_to_file_time = time_func(write_with_write_to_file, result_set)

    assert write_to_file_time < range_time
