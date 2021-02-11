import sys
import gzip

import pytest
import csv
from io import StringIO, BytesIO

from dataengineeringutils3.s3 import gzip_string_write_to_s3
from dataengineeringutils3.writer import (
    BytesSplitFileWriter,
    StringSplitFileWriter,
    JsonNlSplitFileWriter,
)
from tests.helpers import time_func

import jsonlines


@pytest.mark.parametrize(
    "max_bytes,chunk_size,expected_num",
    [(1024, None, 5), (100000000000, 50, 3), (100000000000, None, 1)],
)
def test_json_split_file_writer(s3, max_bytes, chunk_size, expected_num):
    """Test Writer splits files, gzips and sends to s3"""
    file_key = "test-key"
    bucket_name = "test"
    s3_basepath = f"s3://{bucket_name}/"

    s3.meta.client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    bucket = s3.Bucket(bucket_name)
    with JsonNlSplitFileWriter(s3_basepath, file_key, max_bytes, chunk_size) as writer:
        for i in range(150):
            writer.write_line(f"{i}. This test line number {i + 1}")

    assert writer.total_lines == 150
    keys_in_bucket = [f"s3://{bucket_name}/{o.key}" for o in bucket.objects.all()]
    files_in_bucket = len(keys_in_bucket)
    assert files_in_bucket == expected_num
    assert files_in_bucket == writer.num_files

    assert keys_in_bucket == [
        f"{s3_basepath}{file_key}-{i}.jsonl.gz" for i in range(files_in_bucket)
    ]


MAX_BYTES = 80000
CHUNK_SIZE = 1000


def write_with_writer(result_set):
    with JsonNlSplitFileWriter(
        "s3://test/", "test-file", MAX_BYTES, CHUNK_SIZE
    ) as writer:
        writer.write_lines(result_set)


def write_manually(result_set):
    string = ""
    num_files = 0
    num_lines = 0
    while True:
        for line in result_set:
            string += f"{line}"
            if not num_lines % CHUNK_SIZE and sys.getsizeof(string) > MAX_BYTES:
                gzip_string_write_to_s3(
                    string, f"s3://test/test-file-two-{num_files}.jsonl.gz"
                )
                num_files += 1
                num_lines = 0
                string = ""
            num_lines += 1
        break
    if string:
        gzip_string_write_to_s3(string, f"s3://test/test-file-two-{num_files}.josnl.gz")


def test_speed_of_writer(result_set, s3):
    """
    Test that generator is not much slower than a flat list
    """
    s3.meta.client.create_bucket(
        Bucket="test", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
    )

    range_time = time_func(write_manually, result_set)

    qs_time = time_func(write_with_writer, result_set)

    assert qs_time < range_time


@pytest.mark.parametrize(
    "folder,filename,compress", [("test-csv/", "test-file", False), ("", "a", True)]
)
def test_with_csv_string_split_file_writer(s3, folder, filename, compress):
    """Test string writer with statement csv"""
    bucket_name = "test"
    s3.meta.client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )

    csv_data = [
        ("i", "x1", "x2"),
        (1, "a", "b"),
        (2, "a", "b"),
        (3, "a", "b"),
        (4, "a", "b"),
        (5, "a", "b"),
        (6, "a", "b"),
        (7, "a", "b"),
    ]

    expected_file = StringIO()
    e_csv_writer = csv.writer(expected_file)

    ext = "csv.gz" if compress else "csv"

    # Test using with statement
    with StringSplitFileWriter(
        f"s3://{bucket_name}/{folder}",
        filename,
        max_bytes=30,
        compress_on_upload=compress,
        file_extension=ext,
    ) as f:
        csv_writer = csv.writer(f)
        for row in csv_data:
            csv_writer.writerow(row)
            e_csv_writer.writerow(row)

    actual_s3_objects = sorted([o.key for o in s3.Bucket(bucket_name).objects.all()])

    # Test files written to s3
    expected_s3_objects = [f"{folder}{filename}-0.{ext}", f"{folder}{filename}-1.{ext}"]
    assert expected_s3_objects == actual_s3_objects

    expected = expected_file.getvalue()

    # Test file contents
    actual = ""
    for expeceted_object in expected_s3_objects:
        file_object = BytesIO()
        s3.Object(bucket_name, expeceted_object).download_fileobj(file_object)
        if compress:
            actual += gzip.decompress(file_object.getvalue()).decode("utf-8")
        else:
            actual += file_object.getvalue().decode("utf-8")
        file_object.close()

    assert actual == expected


@pytest.mark.parametrize(
    "folder,filename,compress", [("test-csv/", "test-file", False), ("", "a", True)]
)
def test_csv_string_split_file_writer(s3, folder, filename, compress):
    """Test string writer csv"""
    bucket_name = "test"
    s3.meta.client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )

    csv_data = [
        ("i", "x1", "x2"),
        (1, "a", "b"),
        (2, "a", "b"),
        (3, "a", "b"),
        (4, "a", "b"),
        (5, "a", "b"),
        (6, "a", "b"),
        (7, "a", "b"),
    ]

    expected_file = StringIO()
    e_csv_writer = csv.writer(expected_file)

    ext = "csv.gz" if compress else "csv"

    # Test using with statement
    f = StringSplitFileWriter(
        f"s3://{bucket_name}/{folder}",
        filename,
        max_bytes=30,
        compress_on_upload=compress,
        file_extension=ext,
    )
    csv_writer = csv.writer(f)
    for row in csv_data:
        csv_writer.writerow(row)
        e_csv_writer.writerow(row)
    f.close()

    actual_s3_objects = sorted([o.key for o in s3.Bucket(bucket_name).objects.all()])

    # Test files written to s3
    expected_s3_objects = [f"{folder}{filename}-0.{ext}", f"{folder}{filename}-1.{ext}"]
    assert expected_s3_objects == actual_s3_objects

    # Test file contents
    expected = expected_file.getvalue()
    actual = ""
    for expeceted_object in expected_s3_objects:
        file_object = BytesIO()
        s3.Object(bucket_name, expeceted_object).download_fileobj(file_object)
        if compress:
            actual += gzip.decompress(file_object.getvalue()).decode("utf-8")
        else:
            actual += file_object.getvalue().decode("utf-8")
        file_object.close()

    assert actual == expected


@pytest.mark.parametrize(
    "folder,filename,compress,filewriter_type",
    [
        ("test-jsonl/", "test-jsonl", False, "bytes"),
        ("", "a", True, "bytes"),
        ("test-jsonl/", "test-jsonl", False, "string"),
        ("", "a", True, "string"),
    ],
)
def test_split_file_writer_with_json(s3, folder, filename, compress, filewriter_type):
    """Test jsonline string and bytes writer"""

    bucket_name = "test"
    ext = "jsonl.gz" if compress else "jsonl"

    s3.meta.client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )

    jsonl_data = [
        {"i": 1, "x1": "a", "x2": "b"},
        {"i": 2, "x1": "a", "x2": "b"},
        {"i": 3, "x1": "a", "x2": "b"},
        {"i": 4, "x1": "a", "x2": "b"},
        {"i": 5, "x1": "a", "x2": "b"},
    ]

    if filewriter_type == "string":
        f = StringSplitFileWriter(
            f"s3://{bucket_name}/{folder}",
            filename,
            max_bytes=60,
            compress_on_upload=compress,
            file_extension=ext,
        )

    elif filewriter_type == "bytes":
        f = BytesSplitFileWriter(
            f"s3://{bucket_name}/{folder}",
            filename,
            max_bytes=60,
            compress_on_upload=compress,
            file_extension=ext,
        )

    else:
        raise ValueError("Input filewriter_type must be either 'string' or 'bytes'")

    # Write data
    j_writer = jsonlines.Writer(f)

    expected_file = StringIO()
    e_j_writer = jsonlines.Writer(expected_file)

    for row in jsonl_data:
        j_writer.write(row)
        e_j_writer.write(row)
    f.close()

    actual_s3_objects = sorted([o.key for o in s3.Bucket(bucket_name).objects.all()])

    # Test files written to s3
    expected_s3_objects = [
        f"{folder}{filename}-0.{ext}",
        f"{folder}{filename}-1.{ext}",
        f"{folder}{filename}-2.{ext}",
    ]
    assert expected_s3_objects == actual_s3_objects

    # Test file contents
    expected = expected_file.getvalue()
    actual = ""
    for expeceted_object in expected_s3_objects:
        file_object = BytesIO()
        s3.Object(bucket_name, expeceted_object).download_fileobj(file_object)
        if compress:
            actual += gzip.decompress(file_object.getvalue()).decode("utf-8")
        else:
            actual += file_object.getvalue().decode("utf-8")
        file_object.close()

    assert actual == expected
