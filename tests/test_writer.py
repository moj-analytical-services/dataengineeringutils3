from dataengineeringutils3.writer import JsonNlSplitFileWriter


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
