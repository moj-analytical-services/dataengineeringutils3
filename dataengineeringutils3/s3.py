import gzip

import boto3


def gzip_string_write_to_s3(file_as_string, s3_path):
    """
    Writes IoString to s3 path as gziped output
    :param file_as_string: IOString
    :param s3_path: "s3://....
    :return:
    """
    s3_resource = boto3.resource("s3")
    b, k = s3_path_to_bucket_key(s3_path)
    compressed_out = gzip.compress(bytes(file_as_string, 'utf-8'))
    s3_resource.Object(b, k).put(Body=compressed_out)


def s3_path_to_bucket_key(s3_path):
    """
    Splits out s3 file path to bucket key combination
    """
    return s3_path.replace("s3://", "").split("/", 1)
