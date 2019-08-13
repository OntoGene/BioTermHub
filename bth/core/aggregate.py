#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015
# Modified: Lenz Furrer, 2016--2017


'''
Aggregate records from all the different input filters.
'''


import sys
import csv
import json
import logging
import argparse
from collections import defaultdict

# Helper modules.
from ..lib.tools import Fields, TSVDialect, quiet_option, setup_logging

# Input parsers.
from ..inputfilters import FILTERS

# Postfilters.
from ..lib import postfilters as pflt

# Statistics.
from ..stats.bgplotter import BGPlotter


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


def main():
    '''
    Run as script.
    '''
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        'resources', nargs='*', choices=sorted(FILTERS)+['all'],
        metavar='resource', default='all',
        help='any subset of: %(choices)s (default: %(default)s)')
    ap.add_argument(
        '-f', '--postfilters', metavar='JSON', type=pflt.from_json,
        help='record postfiltering: specify the class and instantiation '
             'parameters as a JSON object using "class", "args" and "kwargs" '
             '(eg. {"class": "RegexFilter", "args": [null, "term"]}). '
             'For multiple postfilters, specify a JSON array of objects')
    ap.add_argument(
        '-p', '--params', type=json.loads, metavar='JSON', default={},
        help='any configuration parameters, given as a JSON object')
    quiet_option(ap)
    args = ap.parse_args()
    if 'all' in args.resources:
        args.resources = sorted(FILTERS)
    if args.postfilters is not None:
        args.params['postfilter'] = args.postfilters

    setup_logging(args.quiet)
    rsc = RecordSetContainer(args.resources, **args.params)
    rsc.write_tsv(sys.stdout.buffer.fileno())


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

        self.cross_lookup = defaultdict(set)

    @staticmethod
    def _sort_args(arg):
        '''
        Make sure "duplicate" resources are read last
        (thus after their reference).
        Otherwise, don't change the order.
        '''
        return arg in CROSS_DUPLICATES

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
        for name, constr, custom_params in self.resources:
            # Prepare cascaded parameter overriding.
            params = dict()
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
            rows = postfilter(rows)
        if stats is not None:
            rows = self._collect_stats(rows, stats)
        yield from rows

    def _all_rows(self, **kwargs):
        logging.info('aggregating %d resource(s)', len(self.resources))
        for recordset, resource in self._iter_resources(**kwargs):
            logging.info('processing %s...', resource)
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
        logging.info('done.')

    @staticmethod
    def _collect_stats(rows, stats):
        '''
        While compiling the term list, collect and plot some statistics
        in the background.
        '''
        if not isinstance(stats, BGPlotter):
            stats = BGPlotter(stats)
        for row in rows:
            stats.update(row.original_id, row.term, row.entity_type)
            yield row
        # Start plotting (non-blocking).
        stats.plot()
