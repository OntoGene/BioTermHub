#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Filters for suppressing certain output rows.
'''


import re

from termhub.lib.tools import Fields


__all__ = ('RegexFilter', 'THREEALNUM_ONEALPH')


# Predefined RegExes.
alph = r'[^\W\d_]'
alnum = r'[^\W_]'
num = r'\d'

# At least three alpha-numerical characters and at least one alphabetical.
THREEALNUM_ONEALPH = '|'.join(r'{}.*?{}.*?{}'.format(*x)
                              for x in ((alph, alnum, alnum),
                                        (num, alph, alnum),
                                        (num, num, alph)))


class RegexFilter(object):
    '''
    Only allow terms that match a given regular expression.
    '''
    def __init__(self, pattern=THREEALNUM_ONEALPH, field='term'):
        self.pattern = re.compile(pattern)
        self.field = Fields._fields.index(field)

    def test(self, row):
        'Does this row pass the filter?'
        return bool(self.pattern.search(row[self.field]))
