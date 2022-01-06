import json
import re

from dataengineeringutils3.logging import get_logger


def test_output():
    """
    ensures the log ouput is as expected (including context filter)
    """

    # get the logger and the IO stream
    logger, logger_io_stream = get_logger()

    # log and retrieve a message
    log_message = "a message!"
    logger.info(log_message)
    a = logger_io_stream.getvalue().strip()

    # ensure it matches the required pattern
    regex = re.compile(
        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \| "
        f"test_output | INFO | {log_message}$"
    )

    assert regex.match(a)


def test_diff_fmt():
    """
    makes sure different formats work correctly
    """

    # get the logger and the IO stream
    logger, logger_io_stream = get_logger(
        fmt="%(asctime)s | %(module)s %(table)s | %(levelname)s | %(message)s"
    )

    # log and retrieve a message
    log_message = "a message!"
    logger.info(log_message, extra={"table": "a_very_nice_table"})
    a = logger_io_stream.getvalue()

    # ensure it matches the required pattern
    regex = re.compile(
        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \| "
        f"{__name__.split('.')[1]} a_very_nice_table | INFO | {log_message}$"
    )

    assert regex.match(a)
