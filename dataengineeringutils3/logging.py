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

    return log, log_stringio


def add_stream_handlers(log: logging.Logger):
    log_stringio = io.StringIO()
    io_handler = logging.StreamHandler(log_stringio)
    # set the console output
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    # add the handlers
    log.addHandler(io_handler)
    log.addHandler(console_handler)
