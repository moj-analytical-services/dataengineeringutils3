import cProfile

from mock import patch

from tests.test_db import loop_through_qs, loop_through_list
from tests.test_integration import write_with_write_to_file, write_manually

result_set = [
    '{"uuid": "fkjherpiutrgponfevpoir3qjgp8prueqhf9pq34hf89hwfpu92q"}'
] * 100000


def profile(fn, *args, **kwargs):
    pr = cProfile.Profile()
    pr.enable()
    fn(*args, **kwargs)
    pr.disable()
    pr.print_stats()


if __name__ == "__main__":
    print("loop_through_lists:")
    profile(loop_through_list, result_set)
    print("loop_through_qs:")
    profile(loop_through_qs, result_set)

    with patch("tests.test_integration.gzip_string_write_to_s3"):
        print("write_manually:")
        profile(write_manually, result_set)

    with patch("dataengineeringutils3.writer.JsonNlSplitFileWriter._write"):
        print("write_with_write_to_file:")
        profile(write_with_write_to_file, result_set)
