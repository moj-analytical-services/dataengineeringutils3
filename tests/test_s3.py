import gzip
import io
import os
import pytest
import json

from dataengineeringutils3.s3 import (
    s3_path_to_bucket_key,
    gzip_string_write_to_s3,
    get_filepaths_from_s3_folder,
    read_json_from_s3,
    write_json_to_s3,
    copy_s3_folder_contents_to_new_folder,
    delete_s3_object,
    delete_s3_folder_contents,
    copy_s3_object,
    check_for_s3_file,
    write_local_file_to_s3,
)


@pytest.mark.parametrize(
    "s3_path,exp_bucket,exp_key",
    [
        ("s3://test/file.json", "test", "file.json"),
        ("s3://xds-fddf.e/fffdsa/asdf-xx.zp", "xds-fddf.e", "fffdsa/asdf-xx.zp"),
    ],
)
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


def test_get_filepaths_from_s3_folder(s3):

    bucket_name = "test"

    files = [
        {"folder": "f1", "key": "my_file.json", "body": "test"},
        {"folder": "f1", "key": "df.first.py", "body": "test"},
        {"folder": "f1", "key": "otherfile.json", "body": ""},
        {"folder": "f", "key": "ffile.json", "body": "test"},
        {"folder": "f.2", "key": "otherfile.json", "body": "test"},
    ]

    s3.meta.client.create_bucket(Bucket=bucket_name)

    for f in files:
        s3.Object(bucket_name, f["folder"] + "/" + f["key"]).put(Body=f["body"])

    fps = get_filepaths_from_s3_folder("s3://test/f1")
    assert fps == ["s3://test/f1/df.first.py", "s3://test/f1/my_file.json"]

    fps = get_filepaths_from_s3_folder("s3://test/f1/", exclude_zero_byte_files=False)
    assert fps == [
        "s3://test/f1/df.first.py",
        "s3://test/f1/my_file.json",
        "s3://test/f1/otherfile.json",
    ]

    fps = get_filepaths_from_s3_folder("s3://test/f")
    assert fps == ["s3://test/f/ffile.json"]

    fps = get_filepaths_from_s3_folder(
        "s3://test/f1", file_extension="json", exclude_zero_byte_files=False
    )
    assert fps == ["s3://test/f1/my_file.json", "s3://test/f1/otherfile.json"]


def test_read_json_from_s3(s3):

    bucket_name = "test"

    test_dict = {"foo": "bar"}
    body = json.dumps(test_dict)
    files = [
        {"folder": "f1", "key": "my_file.json", "body": body},
        {"folder": "f1/agfa", "key": "file_no_ext", "body": body},
    ]

    s3.meta.client.create_bucket(Bucket=bucket_name)
    for f in files:
        s3.Object(bucket_name, f["folder"] + "/" + f["key"]).put(Body=f["body"])

    assert read_json_from_s3("s3://test/f1/my_file.json") == test_dict

    assert read_json_from_s3("s3://test/f1/agfa/file_no_ext") == test_dict


def test_write_json_s3(s3):

    bucket_name = "test"
    s3.meta.client.create_bucket(Bucket=bucket_name)

    test_dict = {"foo": "bar", "something": 0}

    write_json_to_s3(test_dict, "s3://test/one.json")
    expected1 = json.dumps(test_dict)
    file_object1 = io.BytesIO()
    s3.Object(bucket_name, "one.json").download_fileobj(file_object1)
    actual1 = file_object1.getvalue().decode("utf-8")
    assert expected1 == actual1

    write_json_to_s3(test_dict, "s3://test/two.json", indent=4, separators=(",", ": "))
    expected2 = json.dumps(test_dict, indent=4, separators=(",", ": "))
    file_object2 = io.BytesIO()
    s3.Object(bucket_name, "two.json").download_fileobj(file_object2)
    actual2 = file_object2.getvalue().decode("utf-8")
    assert expected2 == actual2


def test_copy_s3_folder_contents_to_new_folder(s3):

    files = [
        {"folder": "f1", "key": "my_file.json", "body": "test"},
        {"folder": "f1", "key": "df.first.py", "body": "test"},
        {"folder": "f1", "key": "otherfile.json", "body": ""},
        {"folder": "f.2", "key": "ffile.json", "body": "test"},
        {"folder": "f.2", "key": "otherfile.json", "body": "test"},
    ]
    s3.meta.client.create_bucket(Bucket="source")
    s3.meta.client.create_bucket(Bucket="dest1")
    s3.meta.client.create_bucket(Bucket="dest2")

    expected1 = []
    expected2 = []

    for f in files:
        s3.Object("source", f["folder"] + "/" + f["key"]).put(Body=f["body"])
        expected1.append(f["folder"] + "/" + f["key"])
        if f["folder"] == "f1":
            expected2.append("f1-copy/" + f["key"])

    copy_s3_folder_contents_to_new_folder("s3://source/", "s3://dest1/")
    copy_s3_folder_contents_to_new_folder("s3://source/f1/", "s3://dest2/f1-copy/")

    actual1 = [o.key for o in s3.Bucket("dest1").objects.all()]
    actual2 = [o.key for o in s3.Bucket("dest2").objects.all()]

    assert sorted(actual1) == sorted(expected1)
    assert sorted(actual2) == sorted(expected2)


def test_delete_s3_object(s3):

    bucket_name = "test"
    s3.meta.client.create_bucket(Bucket=bucket_name)
    s3.Bucket(bucket_name).objects.all()
    s3.Object(bucket_name, "test.txt").put(Body="test")
    s3.Object(bucket_name, "test2.txt").put(Body="")

    delete_s3_object("s3://test/test.txt")
    delete_s3_object("s3://test/test2.txt")

    assert [o.key for o in s3.Bucket("test").objects.all()] == []


def test_delete_s3_folder_contents(s3):

    bucket_name = "test"

    files = [
        {"folder": "f1", "key": "my_file.json", "body": "test"},
        {"folder": "f1", "key": "df.first.py", "body": "test"},
        {"folder": "f1", "key": "otherfile.json", "body": ""},
        {"folder": "f", "key": "ffile.json", "body": "test"},
        {"folder": "f.2", "key": "otherfile.json", "body": "test"},
    ]

    s3.meta.client.create_bucket(Bucket=bucket_name)
    expected1 = ["f1/otherfile.json", "f/ffile.json", "f.2/otherfile.json"]
    expected2 = ["f/ffile.json", "f.2/otherfile.json"]
    expected3 = []

    for f in files:
        s3.Object(bucket_name, f["folder"] + "/" + f["key"]).put(Body=f["body"])

    delete_s3_folder_contents("s3://test/f1", exclude_zero_byte_files=True)
    actual1 = [o.key for o in s3.Bucket("test").objects.all()]
    assert sorted(expected1) == sorted(actual1)

    delete_s3_folder_contents("s3://test/f1/")
    actual2 = [o.key for o in s3.Bucket("test").objects.all()]
    assert sorted(expected2) == sorted(actual2)

    delete_s3_folder_contents("s3://test/")
    actual3 = [o.key for o in s3.Bucket("test").objects.all()]
    assert sorted(expected3) == sorted(actual3)


def test_copy_s3_object(s3):

    s3.meta.client.create_bucket(Bucket="source")
    s3.meta.client.create_bucket(Bucket="dest")

    files = [
        {"folder": "f1", "key": "my_file.json", "body": "test"},
        {"folder": "f1", "key": "df.first.py", "body": "test"},
        {"folder": "f1", "key": "otherfile.json", "body": ""},
        {"folder": "f", "key": "ffile.json", "body": "test"},
        {"folder": "f.2", "key": "otherfile.json", "body": "test"},
    ]

    expected = []
    for f in files:
        key = f["folder"] + "/" + f["key"]
        expected.append(key)
        s3.Object("source", key).put(Body=f["body"])
        copy_s3_object(f"s3://source/{key}", f"s3://dest/{key}")

    actual = [o.key for o in s3.Bucket("dest").objects.all()]

    assert sorted(actual) == sorted(expected)


def test_check_for_s3_file(s3):

    bucket_name = "test"
    s3.meta.client.create_bucket(Bucket=bucket_name)

    files = [
        {"folder": "f1", "key": "df.first.py", "body": "test"},
        {"folder": "f1", "key": "otherfile.json", "body": ""},
    ]

    for f in files:
        s3.Object(bucket_name, f["folder"] + "/" + f["key"]).put(Body=f["body"])

    assert check_for_s3_file(f"s3://{bucket_name}/f1/df.first.py")

    assert check_for_s3_file(f"s3://{bucket_name}/f1/otherfile.json")

    assert not check_for_s3_file(f"s3://{bucket_name}/f1/no_file.json")


def test_upload_local_file(s3, tmp_path):

    path = os.path.join(tmp_path, "abctestfile.json")
    test_file = open(path, "w")
    test_file.write("some test contents")
    test_file.close()

    bucket_name = "test"
    s3.meta.client.create_bucket(Bucket=bucket_name)

    write_local_file_to_s3(path, "s3://test/abctestfile.json")
    assert check_for_s3_file(f"s3://test/abctestfile.json")

    with pytest.raises(ValueError):
        write_local_file_to_s3(path, "s3://test/abctestfile.json")

    write_local_file_to_s3(path, "s3://test/abctestfile.json", overwrite=True)

