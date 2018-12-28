#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015
# Modified: Lenz Furrer, 2016


'''
Miscellaneous helper tools.
'''


import csv
from collections import OrderedDict, Counter, namedtuple


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


class DefaultOrderedDict(OrderedDict):
    # Source: http://stackoverflow.com/a/6190500/562769
    def __init__(self, default_factory=None, *a, **kw):
        if default_factory is not None and not callable(default_factory):
            raise TypeError('first argument must be callable')
        super().__init__(*a, **kw)
        self.default_factory = default_factory

    def __getitem__(self, key):
        try:
            return OrderedDict.__getitem__(self, key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):
        if self.default_factory is None:
            args = tuple()
        else:
            args = self.default_factory,
        return type(self), args, None, None, self.items()

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return DefaultOrderedDict(self.default_factory, self)


class StatDict(DefaultOrderedDict):
    def __init__(self):
        super(StatDict, self).__init__(int, {"ids":0, "terms":0, "avg. terms/id":0, "avg. ids/term":0, "ratios":Counter()})
