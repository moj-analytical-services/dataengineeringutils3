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
    compressed_out = gzip.compress(bytes(file_as_string, "utf-8"))
    s3_resource.Object(b, k).put(Body=compressed_out)


def s3_path_to_bucket_key(s3_path):
    """
    Splits out s3 file path to bucket key combination
    """
    return s3_path.replace("s3://", "").split("/", 1)


def bucket_key_to_s3_path(bucket, key):
    """
    Takes an S3 bucket and key combination and returns the full S3 path to that location.
    """
    return f"s3://{bucket}/{key}"


def _add_slash(s):
    """
    Adds slash to end of string
    """
    return s if s[-1] == "/" else s + "/"


def get_filepaths_from_s3_folder(
    s3_folder_path, file_extension=None, exclude_zero_byte_files=True
):
    """
    Get a list of filepaths from a bucket. If extension is set to a string then only return files with that extension otherwise if set to None (default) all filepaths are returned.
    :param s3_folder_path: "s3://...."
    :param extension: file extension, e.g. .json
    :param exclude_zero_byte_files: Whether to filter out results of zero size: True

    """

    s3_resource = boto3.resource("s3")

    if file_extension is not None:
        if file_extension[0] != ".":
            file_extension = "." + file_extension

    # This guarantees that the path the user has given is really a 'folder'.
    s3_folder_path = _add_slash(s3_folder_path)

    bucket, key = s3_path_to_bucket_key(s3_folder_path)

    s3b = s3_resource.Bucket(bucket)
    obs = s3b.objects.filter(Prefix=key)

    if file_extension is not None:
        obs = [o for o in obs if o.key.endswith(file_extension)]

    if exclude_zero_byte_files:
        obs = [o for o in obs if o.size != 0]

    ob_keys = [o.key for o in obs]

    paths = sorted([bucket_key_to_s3_path(bucket, o) for o in ob_keys])

    return paths
