import gzip
import io
import pytest

from dataengineeringutils3.s3 import (
    s3_path_to_bucket_key,
    gzip_string_write_to_s3
)


@pytest.mark.parametrize("s3_path,exp_bucket,exp_key", [
    ("s3://test/file.json", "test", "file.json"),
    ("s3://xds-fddf.e/fffdsa/asdf-xx.zp", "xds-fddf.e", "fffdsa/asdf-xx.zp"),
])
def test_s3_path_to_bucket_key(s3_path, exp_bucket, exp_key):
    bucket, key = s3_path_to_bucket_key(s3_path)
    assert bucket == exp_bucket
    assert key == exp_key


def test_gzip_string_write_to_s3(s3):
    """
    Test that file is gziped correctly and sent to s3
    :param s3: mocked s3 resource
    :return:
    """
    file_text = "test-text"
    file_key = "test-key.txt.gz"
    bucket_name = "test"
    s3_path = f"s3://{bucket_name}/{file_key}"
    s3.meta.client.create_bucket(Bucket=bucket_name)
    gzip_string_write_to_s3(file_text, s3_path)
    file_object = io.BytesIO()
    s3.Object(bucket_name, file_key).download_fileobj(file_object)
    assert gzip.decompress(file_object.getvalue()).decode("utf-8") == file_text
