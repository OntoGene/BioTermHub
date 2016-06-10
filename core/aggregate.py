#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015
# Modified: Lenz Furrer, 2016


'''
Aggregator for joining data from all the different input filters.
'''


import csv
from collections import defaultdict, OrderedDict, Counter

# Helper modules.
from termhub.lib.tools import StatDict, Fields, TSVDialect
from termhub.lib.base36gen import Base36Generator

# Input parsers.
from termhub.inputfilters import FILTERS


# Cross lookup: ID/term pairs are skipped in "CROSS_DUPLICATES" if they are
# also found in "CROSS_REFS".
# Format:
# Reference:  resource: list of counterparts
# Duplicates: resource: flag name, counterpart
CROSS_REFS = {
    'mesh': ['ctd_chem', 'ctd_disease'],
}
CROSS_DUPLICATES = {
    'ctd_chem': ('ctd_lookup', 'mesh'),
    'ctd_disease': ('ctd_lookup', 'mesh'),
}


class RecordSetContainer(object):
    '''
    Handler for multiple inputfilter instances.
    '''
    def __init__(self, resources=(), flags=()):
        self.resources = [(name, FILTERS[name])
                          for name in sorted(resources, key=self._sort_args)]
        self.flags = flags

        self.stats = OrderedDict()
        self.ambig_units = {}
        self.cross_lookup = defaultdict(set)

    @staticmethod
    def _sort_args(arg):
        '''
        Make sure "duplicate" resources are read last
        (thus after their reference).
        '''
        return (arg in CROSS_DUPLICATES, arg)

    def check_cross_lookup(self, resource):
        '''
        Check if a resource should be prepared for cross-lookup.
        '''
        if resource in CROSS_REFS:
            # Check if any associated origins are
            # 1) present and 2) have their cross-lookup flag set.
            for origin in CROSS_REFS[resource]:
                present = any(n == origin for n, c in self.resources)
                flag = CROSS_DUPLICATES[origin][0]
                if present and flag in self.flags:
                    return True
        return False

    def iter_resources(self, mapping=None):
        '''
        Iterate over the readily initialised inputfilters.
        '''
        oidgen = Base36Generator()
        for name, constr in self.resources:
            params = dict(oidgen=oidgen, mapping=mapping)
            # Check if a cross-lookup has to be performed for the resource
            # and if so, pass the corresponding lookup set.
            if name in CROSS_DUPLICATES:
                flag, ref = CROSS_DUPLICATES[name]
                if flag in self.flags:
                    params['exclude'] = self.cross_lookup[ref]
            recordset = constr(**params)
            self.stats[name] = recordset.stats
            self.ambig_units[name] = recordset.ambig_unit
            yield recordset, name

    def calcstats(self):
        total = StatDict()
        total["ratios"]["terms/id"] = Counter()
        total["ratios"]["ids/term"] = Counter()
        for recordset, stats in self.stats.items():

            # Calculate averages and ratios for terms and ids
            if self.ambig_units[recordset] == "terms":
                try:
                    self.stats[recordset]['avg. terms/id'] = stats["terms"]/stats["ids"]
                except ZeroDivisionError:
                    self.stats[recordset]['avg. terms/id'] = 0

                total["ratios"]["terms/id"] += self.stats[recordset]["ratios"]

            elif self.ambig_units[recordset] == "ids":
                try:
                    self.stats[recordset]['avg. ids/term'] = stats["ids"]/stats["terms"]
                except ZeroDivisionError:
                    self.stats[recordset]['avg. ids/term'] = 0

                total["ratios"]["ids/term"] += self.stats[recordset]["ratios"]

            total["terms"] += stats["terms"]
            total["ids"] += stats["ids"]
            total["avg. terms/id"] += self.stats[recordset]["avg. terms/id"]
            total["avg. ids/term"] += self.stats[recordset]["avg. ids/term"]

        try:
            total["avg. terms/id"] /= len(self.stats.keys())
        except ZeroDivisionError:
            total["avg. terms/id"] = 0
        try:
            total["avg. ids/term"] /= len(self.stats.keys())
        except ZeroDivisionError:
            total["avg. ids/term"] = 0

        self.stats["total"] = total

    def write_all(self, filename, mapping=None):
        '''
        Concatenate all resources' data into a large TSV file.
        '''
        with open(filename, 'wt', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, dialect=TSVDialect)
            writer.writerow(Fields._fields)

            for recordset, resource in self.iter_resources(mapping):

                # Initialize cross-lookup and mapping
                clookup = self.check_cross_lookup(resource)
                for row in recordset:
                    # Cross-lookup handling
                    if clookup:
                        # If reference, add id to set
                        self.cross_lookup[resource].add(
                            (row.original_id, row.term))

                    writer.writerow(row)
