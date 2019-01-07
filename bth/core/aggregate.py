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
from collections import defaultdict, OrderedDict, Counter

# Helper modules.
from ..lib.tools import StatDict, Fields, TSVDialect, quiet_option, setup_logging

# Input parsers.
from ..inputfilters import FILTERS

# Postfilters.
from ..lib import postfilters as pflt


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
        '-f', '--postfilters', nargs='+', metavar='JSON', type=pflt.from_json,
        help='record postfiltering: specify the class and instantiation '
             'parameters as a JSON object using "class", "args" and "kwargs" '
             '(eg. {"class": "RegexFilter", "args": [null, "term"]})')
    ap.add_argument(
        '-p', '--params', type=json.loads, metavar='JSON', default={},
        help='any configuration parameters, given as a JSON object')
    quiet_option(ap)
    args = ap.parse_args()
    if 'all' in args.resources:
        args.resources = sorted(FILTERS)
    if args.postfilters is not None:
        args.params['postfilter'] = pflt.combine(args.postfilters)

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

        self.stats = OrderedDict()
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
        params = dict(self.params, **kwargs)
        return self._iter_rows(**params)

    def _iter_rows(self, header=True, postfilter=None, **kwargs):
        if header:
            yield Fields._fields
        rows = self._all_rows(**kwargs)
        if postfilter is not None:
            rows = filter(postfilter, rows)
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
