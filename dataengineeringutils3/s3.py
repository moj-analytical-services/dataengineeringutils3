import gzip
import json
import boto3
import botocore

from io import StringIO


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
    Takes an S3 bucket and key combination and returns the
    full S3 path to that location.
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
    Get a list of filepaths from a bucket. If extension is set to a string
    then only return files with that extension otherwise if set to None (default)
    all filepaths are returned.
    :param s3_folder_path: "s3://...."
    :param extension: file extension, e.g. .json
    :param exclude_zero_byte_files: Whether to filter out results of zero size: True
    :return: A list of full s3 paths that were in the given s3 folder path
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


def read_json_from_s3(s3_path, encoding="utf-8", *args, **kwargs):
    """
    Reads a json from the provided s3 path
    :param s3_path: "s3://...."
    :param encoding: File type encoding (utf-8 default)
    :param *args: Passed to json.loads call
    :param **kwargs: Passed to json.loads call
    :return: data from the json
    """
    s3_resource = boto3.resource("s3")
    bucket, key = s3_path_to_bucket_key(s3_path)
    obj = s3_resource.Object(bucket, key)
    text = obj.get()["Body"].read().decode(encoding)
    return json.loads(text, *args, **kwargs)


def write_json_to_s3(data, s3_path, *args, **kwargs):
    """
    Writes a json to the provided s3 path
    :param data: data to be written to a json file
    :param s3_path: "s3://...."
    :param *args: Passed to json.dump call
    :param **kwargs: Passed to json.dump call
    :return: response dict of upload to s3
    """
    bucket, key = s3_path_to_bucket_key(s3_path)
    s3_resource = boto3.resource("s3")
    log_file = StringIO()
    json.dump(data, log_file, *args, **kwargs)
    log_obj = s3_resource.Object(bucket, key)
    log_upload_resp = log_obj.put(Body=log_file.getvalue())
    return log_upload_resp


def copy_s3_folder_contents_to_new_folder(
    from_s3_folder_path, to_s3_folder_path, exclude_zero_byte_files=False
):
    """
    Copies complete folder structure within from_s3_folder_path
    to the to_s3_folder_path.
    Note any s3 objects in the destination folder will be overwritten if it matches the
    object name being written.
    :param from_s3_folder_path: Folder path that you want to copy "s3://...."
    :param to_s3_folder_path: Folder path that you want to write contents to "s3://...."
    """
    from_s3_folder_path = _add_slash(from_s3_folder_path)
    to_s3_folder_path = _add_slash(to_s3_folder_path)

    all_from_filepaths = get_filepaths_from_s3_folder(
        from_s3_folder_path, exclude_zero_byte_files=exclude_zero_byte_files
    )
    for afp in all_from_filepaths:
        tfp = afp.replace(from_s3_folder_path, to_s3_folder_path)
        copy_s3_object(afp, tfp)


def delete_s3_object(s3_path):
    """
    Deletes the file at the s3_path given.
    :param s3_path: "s3://...."
    """
    s3_resource = boto3.resource("s3")
    b, o = s3_path_to_bucket_key(s3_path)
    s3_resource.Object(b, o).delete()


def delete_s3_folder_contents(s3_folder_path, exclude_zero_byte_files=False):
    """
    Deletes all files within the s3_folder_path given given.
    :param s3_folder_path: Folder path that you want to delete "s3://...."
    :param exclude_zero_byte_files: Whether to filter out results of zero size: False
    """
    s3_folder_path = _add_slash(s3_folder_path)
    all_filepaths = get_filepaths_from_s3_folder(
        s3_folder_path, exclude_zero_byte_files=exclude_zero_byte_files
    )
    for f in all_filepaths:
        delete_s3_object(f)


def copy_s3_object(from_s3_path, to_s3_path):
    """
    Copies a file in S3 from one location to another.
    Automatically overwrites to_s3_path if already exists.
    :param from_s3_path: S3 path that you want to copy "s3://...."
    :param to_s3_path: S3 destination path "s3://...."
    """
    s3_resource = boto3.resource("s3")
    to_bucket, to_key = s3_path_to_bucket_key(to_s3_path)
    if "s3://" in from_s3_path:
        from_s3_path = from_s3_path.replace("s3://", "")
    s3_resource.Object(to_bucket, to_key).copy_from(CopySource=from_s3_path)


def check_for_s3_file(s3_path):
    """
    Checks if a file exists in the S3 path provided.
    :param s3_path: "s3://...."
    :returns: Boolean stating if file exists in S3
    """
    # Taken from:
    # https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
    s3_resource = boto3.resource("s3")
    bucket, key = s3_path_to_bucket_key(s3_path)
    try:
        s3_resource.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise
    else:
        # The object does exist.
        return True


def write_local_file_to_s3(local_file_path, s3_path, overwrite=False):
    """
    Checks if a file exists in the S3 path provided.
    :param local_file_path: "myfolder/myfile.json"
    :param s3_path: "s3://path/to/myfile.json"

    :returns: s3_resource response
    """

    bucket, key = s3_path_to_bucket_key(s3_path)
    s3_resource = boto3.resource("s3")

    if check_for_s3_file(s3_path) and overwrite is False:
        raise ValueError("File already exists.  Pass overwrite = True to overwrite")
    else:
        resp = s3_resource.meta.client.upload_file(local_file_path, bucket, key)

    return resp
