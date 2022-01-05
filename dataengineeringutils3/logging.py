import logging
import io

from typing import Tuple

default_fmt = "%(asctime)s | %(funcName)s | %(levelname)s | %(message)s"
default_date_fmt = "%Y-%m-%d %H:%M:%S"


def _make_fmt_json(fmt: str, sep: str):
    fmt_list = fmt.split(sep)
    fmt_val_list = [i.strip() for i in fmt_list]
    fmt_key_list = [i[2:-2] for i in fmt_val_list]

    json_str = ""
    for key, val in zip(fmt_key_list, fmt_val_list):
        json_str += f'"{key}" : "{val}", '
    return "{" + json_str[:-2] + "}"


def get_logger(
    fmt: str = default_fmt,
    datefmt: str = default_date_fmt,
    output_format="human",
    sep: str = "|",
) -> Tuple[logging.Logger, io.StringIO]:
    """
    returns a logger object and an io stream of the data that is logged
    """

    fmt = _make_fmt_json(fmt, sep) if output_format == "json" else fmt

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
