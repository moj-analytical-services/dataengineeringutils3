import logging
import io

from typing import Tuple

default_fmt = "%(asctime)s | %(funcName)s | %(levelname)s | %(message)s"
default_date_fmt = "%Y-%m-%d %H:%M:%S"


def get_logger(
    fmt: str = default_fmt, datefmt: str = default_date_fmt
) -> Tuple[logging.Logger, io.StringIO]:
    """
    returns a logger object and an io stream of the data that is logged
    """

    log = logging.getLogger()
    log.setLevel(logging.DEBUG)

    # for some reason, log.hasHandlers doesn't work below <3.9
    if not log.handlers:
        # set the io handler
        log_stringio = io.StringIO()
        io_handler = logging.StreamHandler(log_stringio)
        # set the console output
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        # add the handlers
        log.addHandler(io_handler)
        log.addHandler(console_handler)
    else:
        # this relies on the StringIO logger being added first (which we did do above)
        log_stringio = log.handlers[0].stream

    log_formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    # add the formatters
    for log_handler in log.handlers:
        log_handler.setFormatter(log_formatter)

    return log, log_stringio
