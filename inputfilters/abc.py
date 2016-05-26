#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Abstract base class for RecordSet.
'''


from termhub.lib.base36gen import Base36Generator
from termhub.lib.tools import StatDict, Fields


class AbstractRecordSet(object):
    '''
    Record collector abstract base.
    '''

    ambig_unit = None
    resource = None
    entity_type = None

    def __init__(self, fn, collect_stats=False, oidgen=None, mapping=None):
        self.fn = fn
        self.stats = StatDict()
        self.collect_stats = collect_stats
        if oidgen is None:
            oidgen = Base36Generator()
        self.oidgen = oidgen
        self.resource = self.mapping(mapping, 'resource', self.resource)
        self.entity_type = self.mapping(mapping, 'entity_type', self.entity_type)

    @staticmethod
    def mapping(mapping, field, default):
        '''
        Get alternative names for one of the fixed-valued fields.
        '''
        try:
            m = mapping[field]
        except (TypeError, KeyError):
            return default
        try:
            return m[default]
        except KeyError:
            for key in m:
                if re.match(key, default):
                    return m[key]
            return default

    def update_stats(self, terms_per_id):
        '''
        Update the ambiguity/redundancy statistics.
        '''
        self.stats["ids"] += 1
        self.stats["terms"] += terms_per_id
        self.stats["ratios"][terms_per_id, "terms/id"] += 1
