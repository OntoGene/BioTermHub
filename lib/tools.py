#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015
# Modified: Lenz Furrer, 2016


'''
Miscellaneous helper tools.
'''


import re
import csv
from collections import namedtuple


# Fields of the output TSV.
Fields = namedtuple('Fields', 'oid resource original_id '
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
