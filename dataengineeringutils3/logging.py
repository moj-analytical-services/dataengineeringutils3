import logging
import io
import sys

from typing import Tuple

default_fmt = "%(asctime)s | %(funcName)s | %(levelname)s | %(context)s | %(message)s"
default_date_fmt = "%Y-%m-%d %H:%M:%S"


class ContextFilter(logging.Filter):
    """
    This is just overkill to apply a default context param to the log.
    But it does mean I don't have to define extra everytime I wanna log.
    So keeping it.
    """

    def filter(self, record):
        if not getattr(record, "context", None):
            record.context = "PROCESSING"
        return True


def get_logger(
    fmt=default_fmt, datefmt=default_date_fmt
) -> Tuple[logging.Logger, io.StringIO]:
    """
    returns a logger object and an io stream of the data that is logged
    """

    log = logging.getLogger("root")
    log.setLevel(logging.DEBUG)

    log_stringio = io.StringIO()
    handler = logging.StreamHandler(log_stringio)

    log_formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    handler.setFormatter(log_formatter)
    log.addHandler(handler)

    # Add console output
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(log_formatter)
    log.addHandler(console)

    cf = ContextFilter()
    log.addFilter(cf)

    return log, log_stringio
