#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import enum
import typing
import decimal
import logging


Formatter = typing.Callable[[typing.Dict[int, typing.Dict[str, typing.Any]]], str]

_logger = logging.getLogger(__name__)


class Format(enum.Enum):
    YAML = enum.auto()
    JSON = enum.auto()
    TSV = enum.auto()


def make_formatter(format_type: Format) -> Formatter:
    return {
        Format.YAML: _make_yaml_formatter,
        Format.JSON: _make_json_formatter,
        Format.TSV:  _make_tsv_formatter,
    }[format_type]()


def _make_yaml_formatter() -> Formatter:
    from ._yaml import YAMLDumper
    dumper = YAMLDumper(explicit_start=True)
    return lambda data: dumper.dumps(data)


def _make_json_formatter() -> Formatter:
    try:
        import simplejson as json
    except ImportError:
        _logger.info('Please install simplejson to avoid numerical precision loss during serialization. '
                     'The command is: pip install simplejson')
        import json

    def json_default(o: object) -> object:
        if isinstance(o, decimal.Decimal):
            return float(o)
        raise TypeError

    return lambda data: json.dumps(data,
                                   default=json_default,
                                   ensure_ascii=False,
                                   separators=(',', ':'))


def _make_tsv_formatter() -> Formatter:
    # TODO print into a TSV (tab separated values, like CSV with tabs instead of commas).
    # The TSV format should place one scalar per column for ease of parsing by third-party software.
    # Variable-length entities such as arrays should expand into the maximum possible number of columns?
    # Unions should be represented by adjacent groups of columns where only one such group contains values?
    # We may need to obtain the full type information here in order to build the final representation.
    # Sounds complex. Search for better ways later. We just need a straightforward way of dumping data into a
    # standard tabular format for later processing using third-party software.
    raise NotImplementedError('Sorry, the TSV formatter is not yet implemented')
