#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Filters for suppressing certain output rows.
'''


import re
import csv
import json
import gzip

from ..core import settings
from .tools import Fields, TSVDialect


__all__ = ('RegexFilter', 'CommonWordFilter', 'BlackListFilter',
           'EntrezGeneFilter',
           'from_json', 'combine')


class _BaseFilter:
    # Explicit method for backwards-compatibility; use __call__() now.
    def test(self, row):
        '''
        Does this row pass the filter?
        '''
        return self(row)

    def __call__(self, row):
        """
        Apply the filter: True means pass, False means skip.
        """
        raise NotImplementedError


class RegexFilter(_BaseFilter):
    '''
    Only allow terms that match a given regular expression.
    '''

    # Predefined RegExes.
    _alph = r'[^\W\d_]'
    _alnum = r'[^\W_]'
    _num = r'\d'

    # At least three alpha-numerical characters and at least one alphabetical.
    THREEALNUM_ONEALPH = '|'.join(r'{}.*?{}.*?{}'.format(*x)
                                  for x in ((_alph, _alnum, _alnum),
                                            (_num, _alph, _alnum),
                                            (_num, _num, _alph)))

    def __init__(self, pattern=None, field='term'):
        if pattern is None:
            pattern = self.THREEALNUM_ONEALPH

        self.pattern = re.compile(pattern)
        self.field = Fields._fields.index(field)

    def __call__(self, row):
        return bool(self.pattern.search(row[self.field]))


class BlackListFilter(_BaseFilter):
    '''
    Remove specifically listed terms.
    '''
    def __init__(self, resource, blacklist):
        self.resource = resource
        self.blacklist = self._load(blacklist)

    def __call__(self, row):
        if (row.resource == self.resource and
                self._normalise(row.term) in self.blacklist):
            return False
        return True

    @classmethod
    def _load(cls, blacklist):
        if isinstance(blacklist, (str, int)):
            blacklist = cls._read(blacklist)
        return frozenset(blacklist)

    @staticmethod
    def _read(source):
        with open(source, 'r', encoding='utf8') as f:
            for word in f:
                yield word.strip()

    @staticmethod
    def _normalise(term):
        return term.lower()


class EntrezGeneFilter(BlackListFilter):
    '''
    Remove hopeless general-vocaulary terms from EntrezGene records.
    '''
    _blacklist = '''act
                    and
                    all
                    but
                    camp
                    can
                    cap
                    cell
                    chip
                    damage
                    early
                    end
                    for
                    had
                    has
                    large
                    light
                    not
                    ray
                    rat
                    the
                    type
                    via
                    was
                    with'''.split()

    def __init__(self, resource='EntrezGene', blacklist=None):
        blacklist = self._blacklist if blacklist is None else blacklist
        super().__init__(resource, blacklist)


class CommonWordFilter(_BaseFilter):
    '''
    Remove frequent words based on Google n-grams.
    '''
    def __init__(self, threshold=1e-4):
        with gzip.open(settings.gen_voc_db_file, 'rt', encoding='utf8') as f:
            rows = csv.reader(f, dialect=TSVDialect)
            self.frequent = frozenset(ngram for ngram, _, freq in rows
                                      if float(freq) >= threshold)

    def __call__(self, row):
        return row.term not in self.frequent


def from_json(expression):
    '''
    Create a postfilter instance from a JSON expression.
    '''
    info = json.loads(expression)
    class_ = info['class']
    if class_ not in __all__:
        raise ValueError('unknown postfilter: {}'.format(class_))
    constr = globals()[class_]
    args, kwargs = info.get('args', ()), info.get('kwargs', {})
    return constr(*args, **kwargs)


def combine(filters):
    '''
    Wrap the test methods of all filters in a single function.
    '''
    if len(filters) == 1:
        return filters[0]

    def _test(row):
        return all(f(row) for f in filters)
    return _test
