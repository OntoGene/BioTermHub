#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015
# Modified: Lenz Furrer, 2016


'''
Miscellaneous helper tools.
'''


import re
import csv
import logging
from pathlib import Path
from collections import namedtuple


# Special value for the `idprefix` parameter: make all IDs URIs.
URI_PREFIX = '$URI$'


# Fields of the output TSV.
Fields = namedtuple('Fields', 'cui resource original_id '
                              'term preferred_term entity_type')


class TSVDialect(csv.Dialect):
    'TSV dialect used for the Hub output.'
    lineterminator = '\r\n'
    delimiter = '\t'
    skipinitialspace = False
    escapechar = '\\'
    quoting = csv.QUOTE_NONE  # no special treatment of quote characters
    quotechar = '"'
    doublequote = False
    strict = False


def sanitise(text):
    '''
    Remove any characters except for ASCII a-zA-Z0-9.
    '''
    return re.sub(r'[^a-zA-Z0-9]', '', text)


class classproperty(property):
    '''Decorator for class properties.'''
    def __get__(self, _instance, owner):
        return super().__get__(owner)


def quiet_option(argparser):
    '''
    Add a -q/--quiet option to this argument parser.
    '''
    argparser.add_argument(
        '-q', '--quiet', action='store_true',
        help='suppress progress info')


def setup_logging(quiet=False):
    '''
    Call basicConfig with default values.
    '''
    logging.basicConfig(format='%(asctime)s: %(message)s',
                        level=logging.WARNING if quiet else logging.INFO)


class Tempfile:
    '''Context handler for transparent temp files as intermediate target.'''
    def __init__(self, target: Path):
        self.target = target
        self.tmp = target.with_suffix(target.suffix + '.tmp')

    def __enter__(self):
        return self.tmp

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.tmp.rename(self.target)
        except OSError:
            pass
        return False  # don't suppress exceptions
