#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015
# Modified: Lenz Furrer, 2016


'''
Aggregator for joining data from all the different input filters.
'''


import csv
from collections import defaultdict

# Helper modules.
from termhub.lib.tools import Fields, TSVDialect
from termhub.lib.base36gen import Base36Generator

# Input parsers.
from termhub.inputfilters import FILTERS

# Statistics.
from termhub.stats.bgplotter import BGPlotter


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
    def __init__(self, resources=(), flags=(), **params):
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
        self.params = params  # remaining params -- handled later

        self.plots = None
        self.cross_lookup = defaultdict(set)

    @staticmethod
    def _sort_args(arg):
        '''
        Make sure "duplicate" resources are read last
        (thus after their reference).
        '''
        return (arg in CROSS_DUPLICATES, arg)

    def _check_cross_lookup(self, resource):
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

    def _iter_resources(self, **kwargs):
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
            # Override with any filter-specific and local params.
            params.update(custom_params)
            params.update(kwargs)
            # Create the filter instance and collect some properties.
            recordset = constr(**params)
            yield recordset, name

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
        params = dict(self.params, **kwargs)
        return self._iter_rows(**params)

    def _iter_rows(self, header=True, postfilter=None, stats=None, **kwargs):
        if header:
            yield Fields._fields
        rows = self._all_rows(**kwargs)
        if postfilter is not None:
            rows = filter(postfilter, rows)
        if stats:
            rows = self._collect_stats(rows, stats)
        yield from rows

    def _all_rows(self, **kwargs):
        for recordset, resource in self._iter_resources(**kwargs):
            if self._check_cross_lookup(resource):
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

    def _collect_stats(self, rows, dest_dir):
        '''
        While compiling the term list, collect and plot some statistics
        in the background.
        '''
        stats = BGPlotter(dest_dir)
        for row in rows:
            stats.update(row.original_id, row.term, row.entity_type)
            yield row
        # Start plotting (non-blocking).
        self.plots = stats.plot()  # return value is a list of file names
