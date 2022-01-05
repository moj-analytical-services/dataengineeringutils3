import pytest
import re

from dataengineeringutils3.logging import get_logger


@pytest.mark.parametrize("context_filter", (None, {"context": "A_DIFF_CONTEXT"}))
def test_output(context_filter):
    """
    ensures the log ouput is as expected (including context filter)
    """

    # get the logger and the IO stream
    logger, logger_io_stream = get_logger()

    # log and retrieve a message
    log_message = "a message!"
    logger.info(log_message, extra=context_filter)
    a = logger_io_stream.getvalue()

    # get the required context
    context = "PROCESSING" if not context_filter else list(context_filter.values())[0]

    # ensure it matches the required pattern
    regex = re.compile(
        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \| " 
        f"test_output \| INFO \| {context} \| {log_message}$"
    )

    assert regex.search(a)
