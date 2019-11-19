from unittest.mock import call

from dataengineeringutils3.writer import JsonNlSplitFileWriter


def test_large_select_queryset_with_writer(s3, large_select_queryset):
    """
    Test file writer and queryset with large result set.
    """
    file_key = "test-key"
    bucket_name = "test"
    s3_path = f"s3://{bucket_name}/{file_key}"
    s3.meta.client.create_bucket(Bucket=bucket_name)
    bucket = s3.Bucket(bucket_name)
    with JsonNlSplitFileWriter(s3_path, 1024, 10000) as writer:
        for line in large_select_queryset:
            writer.write_line(line)

    keys_in_bucket = [f"s3://{bucket_name}/{o.key}" for o in bucket.objects.all()]
    files_in_bucket = len(keys_in_bucket)
    assert files_in_bucket == 10

    large_select_queryset.cursor.fetchmany.assert_has_calls([
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
    ])
