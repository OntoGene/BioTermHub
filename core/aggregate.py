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
from termhub.lib.tools import StatDict, Fields, TSVDialect, CacheOneIterator
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
    def __init__(self, resources=(), flags=(), postfilter=None, **params):
        '''
        Args:
            resources (sequence): resource name, as found in FILTERS
            flags (sequence): flags for eg. cross-lookup
            params (kwargs): additional params.
                             Filter-specific params can be specified through
                             nesting, ie. the key matches a resource name
                             and the value is a dict, which is unpacked in
                             the filter constructor.
                             Example for changing the MeSH subtrees:
                                ... mesh={'tree_types': {'A': 'anatomy'}} ...
        '''
        self.resources = [(name, FILTERS[name], params.pop(name, {}))
                          for name in sorted(resources, key=self._sort_args)]
        self.flags = frozenset(flags)
        self.postfilter = postfilter
        self.params = params  # remaining params -- passed on to the filters

        self.stats = OrderedDict()
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
            # Check if any associated duplicates are
            # 1) present and 2) have their cross-lookup flag set.
            for dup in CROSS_REFS[resource]:
                present = any(n == dup for n, _, _ in self.resources)
                flag = CROSS_DUPLICATES[dup][0]
                if present and flag in self.flags:
                    return True
        return False

    def iter_resources(self, **kwargs):
        '''
        Iterate over the readily initialised inputfilters.
        '''
        oidgen = Base36Generator()
        for name, constr, custom_params in self.resources:
            # Prepare cascaded parameter overriding.
            params = dict(oidgen=oidgen)
            # Check if a cross-lookup has to be performed for the resource
            # and if so, pass the corresponding lookup set.
            if name in CROSS_DUPLICATES:
                flag, ref = CROSS_DUPLICATES[name]
                if flag in self.flags:
                    params['exclude'] = self.cross_lookup[ref]
            # Override with any global, filter-specific, and local params.
            params.update(self.params)
            params.update(custom_params)
            params.update(kwargs)
            # Create the filter instance and collect some properties.
            recordset = constr(**params)
            self.stats[name] = recordset.stats
            yield recordset, name

    def calcstats(self):
        '''
        Calculate averages and ratios for terms and IDs.
        '''
        total = StatDict()
        total["ratios"]["terms/id"] = Counter()
        total["ratios"]["ids/term"] = Counter()
        for stats in self.stats.values():

            try:
                stats['avg. terms/id'] = stats["terms"]/stats["ids"]
            except ZeroDivisionError:
                stats['avg. terms/id'] = 0

            total["ratios"]["terms/id"] += stats["ratios"]

            total["terms"] += stats["terms"]
            total["ids"] += stats["ids"]
            total["avg. terms/id"] += stats["avg. terms/id"]
            total["avg. ids/term"] += stats["avg. ids/term"]

        try:
            total["avg. terms/id"] /= len(self.stats)
        except ZeroDivisionError:
            total["avg. terms/id"] = 0
        try:
            total["avg. ids/term"] /= len(self.stats)
        except ZeroDivisionError:
            total["avg. ids/term"] = 0

        self.stats["total"] = total

    def write_tsv(self, filename, **kwargs):
        '''
        Concatenate all resources' data into a large TSV file.
        '''
        with open(filename, 'wt', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, dialect=TSVDialect)
            writer.writerows(self.iter_rows(**kwargs))

    def iter_rows(self, **kwargs):
        '''
        Iterate over all rows of all resources.
        '''
        # Mix in parameters defined in the constructor.
        params = dict(self.params, postfilter=self.postfilter)
        params.update(kwargs)
        return self._iter_rows(**params)

    def _iter_rows(self, header=True, postfilter=None, **kwargs):
        if header:
            yield Fields._fields
        if postfilter is not None:
            yield from self._filtered_rows(postfilter, **kwargs)
        else:
            yield from self._all_rows(**kwargs)

    def _filtered_rows(self, test, oidgen=None, **kwargs):
        if oidgen is None:
            oidgen = CacheOneIterator(Base36Generator())
        for row in self._all_rows(oidgen=oidgen, **kwargs):
            if test(row):
                # Successfully passed the filter.
                yield row
            else:
                # Did not pass.
                # Rewind the OID counter by one, avoiding gaps.
                oidgen.rewind()

    def _all_rows(self, **kwargs):
        for recordset, resource in self.iter_resources(**kwargs):
            if self.check_cross_lookup(resource):
                # Iterate with cross-lookup handling.
                for row in recordset:
                    # Keep all ID-term pairs in memory, so that they can be
                    # skipped in the duplicate resource.
                    self.cross_lookup[resource].add(
                        (row.original_id, row.term))
                    yield row
            else:
                # No cross-lookup handling.
                yield from recordset
