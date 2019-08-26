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
           'EntrezGeneFilter', 'PatternReplaceAdder', 'LookupReplaceAdder',
           'from_json', 'from_spec', 'combine')


class _BaseFilter:
    """Abstract base for row-removing filters."""

    def test(self, row):
        '''
        Does this row pass the filter?
        '''
        raise NotImplementedError

    def __call__(self, rows):
        """
        Apply the filter to an iterable of rows.
        """
        return filter(self.test, rows)


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

    def test(self, row):
        return bool(self.pattern.search(row[self.field]))


class BlackListFilter(_BaseFilter):
    '''
    Remove specifically listed terms.
    '''
    def __init__(self, resource, blacklist):
        self.resource = resource
        self.blacklist = self._load(blacklist)

    def test(self, row):
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

    def test(self, row):
        return row.term not in self.frequent


class _BaseAdder:
    """Abstract base for row-adding filters."""

    def __call__(self, rows):
        for row in rows:
            yield row
            yield from self._extend(row)

    def _extend(self, row):
        raise NotImplementedError


class LookupReplaceAdder(_BaseAdder):
    """Add rows with term variants using a lookup table."""

    def __init__(self, mapping, resources, from_file=False):
        """
        Specify term replacements and target resources.

        Args:
            mapping (dict(str, str)): full terms matched
                exactly against the row.term field
            resources (list(str)): limit replacements to these,
                eg. ["CTD (MESH)"]
            from_file (bool): if True, the mapping parameter
                is interpreted as a filename pointing to a
                2-column TSV file containing key-value pairs
        """
        if from_file:
            with open(mapping, encoding='utf8') as f:
                mapping = dict(csv.reader(f, dialect=TSVDialect))
        self.repl = {(r, k): v for r in resources for k, v in mapping.items()}

    def _extend(self, row):
        try:
            variant = self.repl[row.resource, row.term]
        except KeyError:
            return
        else:
            yield row._replace(term=variant)


class PatternReplaceAdder(_BaseAdder):
    """
    Add rows with term variants using `re.sub()`.

    Example:
        For a row with the term "Parkinson disease", copy
        the row and add another one with only "Parkinson".
    """

    def __init__(self, replacements, resources):
        r"""
        Specify term replacements and target resources.

        Args:
            replacements (dict(str, str)): regex patterns
                mapped to replacements, eg. {r"\\s+disease$": ""}.
                If order matters, specify an OrderedDict.
            resources (list(str)): limit replacements to these,
                eg. ["CTD (MESH)"]
        """
        self.repl = [(re.compile(p), r) for p, r in replacements.items()]
        self.resources = set(resources)

    def _extend(self, row):
        if row.resource not in self.resources:
            return

        for pat, rep in self.repl:
            variant, n = pat.subn(rep, row.term)
            if n:
                yield row._replace(term=variant)


def from_json(expression):
    '''
    Create a postfilter callable from a JSON expression.

    Examples:
        {"class": "RegexFilter", "args": ["C[0-9]*", "cui"]}
        {"class": "RegexFilter", "kwargs": {"field": "preferred_term"}}
        [{"class": "CommonWordFilter"}, {"class": "EntrezGeneFilter"}]
    '''
    info = json.loads(expression)
    if isinstance(info, list):
        return combine(list(map(from_spec, info)))
    return from_spec(info)


def from_spec(info):
    """
    Construct a postfilter instance from class/args/kwargs.
    """
    class_ = info['class']
    if class_ not in __all__:
        raise ValueError('unknown postfilter: {}'.format(class_))
    constr = globals()[class_]
    args, kwargs = info.get('args', ()), info.get('kwargs', {})
    return constr(*args, **kwargs)


def combine(filters):
    '''
    Wrap all filters in a single function.
    '''
    def _filter(rows):
        for f in filters:
            rows = f(rows)
        return rows
    return _filter
