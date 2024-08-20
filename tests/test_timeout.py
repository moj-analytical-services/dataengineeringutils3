import pytest
import time
from dataengineeringutils3.timeout import Timeout


@pytest.mark.parametrize(
    "seconds",
    [
        (5),
    ],
)
def test_timeout_raised(seconds):
    """
    Tests that the timeout error raises an exception if stated time elapses.
    """
    with pytest.raises(Exception):
        with Timeout(seconds=seconds):
            time.sleep(seconds+1)


@pytest.mark.parametrize(
    "seconds, expected_output",
    [
        (10, "banana"),
    ],
)
def test_timeout_not_raised(seconds, expected_output):
    """
    Tests that the timeout error raises an exception if stated time elapses.
    """
    with pytest.raises(Exception):
        with Timeout(seconds=seconds):
            x = expected_output
    assert x == expected_output
