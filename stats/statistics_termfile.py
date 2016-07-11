#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Author: Tilia Ellendorff, 2015
# Modified: Lenz Furrer, 2016


"""
Calculate term statistics over a term file in the ontogene term hub format.
"""


from optparse import OptionParser
import sys
from collections import Counter, defaultdict
import csv
import re
import cProfile

from termhub.lib.tools import TSVDialect


class StatsCollector(object):
    '''
    Collector for ambiguity and synonymy statistics.
    '''
    def __init__(self, label, name):
        self.label = label  # eg. "Resource", "Entity Type"
        self.name = name

        self.terms = Counter()
        # counts number of occurrences of term
        # Key: term, Value: count
        # included: calculate total number of terms (types) = length of terms

        self.synonyms = defaultdict(set)
        # counts number of synonyms for each id
        # Key: ID, Value: set of terms

        self.ambiguous_terms = defaultdict(set)
        # counts ids per term type (how many different ids are associated with
        # one term (types)?)
        # Key: term, Value: set of ids

        self.ambiguous_terms_lower = defaultdict(set)
        # counts number of ids per term (unique) with all terms lowercased
        # Key: Term, Value: set of ids

        self.ambiguous_terms_nows = defaultdict(set)
        # counts number of ids per terms (unique) with all terms lowercased
        # and all non-alphanumeric chars removed
        # Key: Term, Value: set of ids

    @property
    def ids(self):
        'Set of all IDs.'
        return set().union(*self.ambiguous_terms.values())

    @classmethod
    def strip_symbols(cls, term):
        '''
        Remove non-alphanumeric characters.
        '''
        return cls.nonalnum.sub('', term)

    nonalnum = re.compile(r'[\W_]+')

    def update_stats(self, id_, term):
        '''
        Update all counters with this entry.
        '''
        term_lw = term.lower()
        term_nws = self.strip_symbols(term_lw)

        self.terms[term] += 1
        self.ambiguous_terms[term].add(id_)
        self.ambiguous_terms_lower[term_lw].add(id_)
        self.ambiguous_terms_nows[term_nws].add(id_)
        self.synonyms[id_].add(term)

    def term_length_avg(self):
        '''
        Get the average term length.
        '''
        total_length = sum(len(term) for term in self.terms)
        avg = total_length / len(self.terms)
        return avg

    def id_freq_dist(self):
        'Terms per ID (synonymy).'
        return freq_dist(self.synonyms)

    def term_freq_dist(self):
        'IDs per term (ambiguity).'
        return freq_dist(self.ambiguous_terms)

    def term_lw_freq_dist(self):
        'IDs per lower-cased term (case-insensitive ambiguity).'
        return freq_dist(self.ambiguous_terms_lower)

    def term_lw_nows_freq_dist(self):
        'IDs per lower-cased, alphanumeric-only term (normalised ambiguity).'
        return freq_dist(self.ambiguous_terms_nows)


    def display_stats(self):
        '''
        Dump a textual description to STDOUT.
        '''
        print('\n')

        print(self.label, 'statistics for', self.name)
        print('Number of original IDs:', len(self.ids))
        print('Number or original terms:', len(self.terms))
        print('Average of IDs associated to one term ("ambiguous terms"):',
              average(self.ambiguous_terms))
        print('Average of Terms associated to one ID ("synonyms"):',
              average(self.synonyms))

        print('FREQ DIST number of terms per id', self.id_freq_dist())
        print('FREQ DIST number of ids per term', self.term_freq_dist())
        print('FREQ DIST number of ids per lower-cased term',
              self.term_lw_freq_dist())
        print('FREQ DIST number or ids per lower-cased term with '
              'non-alphanumeric characters removed',
              self.term_lw_nows_freq_dist())

        print('AVG Token Lenght', self.term_length_avg())


class OverallStats(StatsCollector):
    '''
    Collector for the whole combined resource.
    '''
    def __init__(self):
        super().__init__(None, None)

        self.all_lines_counter = 0
        # counts the total number of entries in the whole term file (tokens)
        # simple counter

        self.resources = {}
        # stores ResourceStats objects for all resources

        self.entity_types = {}

    def update_stats(self, line_dict):
        self.all_lines_counter += 1

        resource = line_dict['resource']
        entity_type = line_dict['entity_type']
        id_ = line_dict['original_id']
        term = line_dict['term']

        # Update Resource stats.
        if resource not in self.resources:
            self.resources[resource] = StatsCollector('Resource', resource)
        self.resources[resource].update_stats(id_, term)

        # Update Entity Type stats.
        if entity_type not in self.entity_types:
            self.entity_types[entity_type] = StatsCollector('Entity Type', entity_type)
        self.entity_types[entity_type].update_stats(id_, term)

        # Update global stats.
        super().update_stats(id_, term)

    def display_stats(self):
        print('\n')
        print('STATS FOR WHOLE TERM FILE')
        print('Number of lines/terms:', self.all_lines_counter)
        print('Resources:', ', '.join(self.resources))
        print('Number of Resources:', len(self.resources))
        print('Total number of unique terms (types) in the term file:', len(self.terms))
        print('Average of tokens per type:', average(self.terms))
        print('Average of ids per term:', average(self.ambiguous_terms))
        print('Average of ids per term with lowercased terms:', average(self.ambiguous_terms_lower))
        print('Average of ids per term with lowercased terms and non-alphabetical characters removed:', average(self.ambiguous_terms_nows))

        print('FREQ DIST number of terms per id', self.id_freq_dist())
        print('FREQ DIST number of ids per term', self.term_freq_dist())
        print('FREQ DIST number of ids per term (terms are lowercased)', self.term_lw_freq_dist())
        print('FREQ DIST number of ids per term (terms are lowercased and symbols are removed', self.term_lw_nows_freq_dist())
        print('AVG Token Lenght', self.term_length_avg())

        print('-----------')
        print('RESOURCE STATS')
        for resource_stats in self.resources.values():
            resource_stats.display_stats()

        print('-----------')
        print('ENTITY TYPE STATS')
        for entity_type_stats in self.entity_types.values():
            entity_type_stats.display_stats()


def freq_dist(coll):
    return Counter(len(set(v)) for v in coll.values())


def average(coll):
    try:
        total_count = sum(coll.values())
    except TypeError:
        total_count = sum(len(v) for v in coll.values())
    avg = total_count / len(coll)
    return avg


def process_file(csv_file):
    '''
    Read a csv file and produce a list of dictionaries
    with one dictionary per line using DictReader;
    Headers are used as keys.
    '''

    # Generate proper header from first line
    with open(csv_file, 'r') as infile:
        reader = csv.DictReader(infile, dialect=TSVDialect)
        #fieldnames = 'oid', 'resource', 'original_id', 'term', 'preferred_term', 'entity_type'

        overall_stats = OverallStats()

        for row in reader:
            overall_stats.update_stats(row)

    return overall_stats


def process(options=None, args=None):
    """
    Do the processing.

    Put together all functions.
    """
    input_file = args[0]
    # Read input file and calculate statistics
    overall_stats = process_file(input_file)
    overall_stats.display_stats()


def main():
    """
    Invoke this module as a script
    """
    usage = "usage: %prog [options]; args[0]: database file - csv file ('Ontogene Format')"
    parser = OptionParser(version='%prog 0.99', usage=usage)

    parser.add_option('-l', '--logfile', dest='logfilename',
                      help='write log to FILE', metavar='FILE')
    parser.add_option('-q', '--quiet',
                      action='store_true', dest='quiet', default=False,
                      help='do not print status messages to stderr')
    parser.add_option('-d', '--debug',
                      action='store_true', dest='debug', default=False,
                      help='print debug information')

    (options, args) = parser.parse_args()

    if options.debug:
        print('# Starting processing', file=sys.stderr)

    process(options=options, args=args)


if __name__ == '__main__':
    cProfile.run('main()', 'calculate_statistics.profile')
