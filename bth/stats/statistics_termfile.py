#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Author: Tilia Ellendorff, 2015
# Modified: Lenz Furrer, 2016


"""
Calculate term synonymy and ambiguity statistics.
"""


from collections import Counter, defaultdict
import csv
import re

from ..lib.tools import TSVDialect


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

    def update(self, id_, term):
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
    def __init__(self, label=None, name=None):
        super().__init__(label, name)

        # Total number of entries in the whole term file (tokens).
        self.all_lines_counter = 0

        self.substats = defaultdict(dict)

    def update(self, id_, term, **kwargs):
        self.all_lines_counter += 1

        # Update subordinate stats.
        for label, name in kwargs.items():
            if name not in self.substats[label]:
                self.substats[label][name] = StatsCollector(label, name)
            self.substats[label][name].update(id_, term)

        # Update global stats.
        super().update(id_, term)

    def display_stats(self):
        print('\n')
        print('STATS FOR WHOLE TERM FILE')
        print('Number of lines/terms:', self.all_lines_counter)
        print('Substats:')
        for label, names in self.substats.values():
            print('  {}:'.format(label), ', '.join(names))
        print('Total number of unique terms (types) in the term file:',
              len(self.terms))
        print('Average of tokens per type:', average(self.terms))
        print('Average of ids per term:', average(self.ambiguous_terms))
        print('Average of ids per term with lowercased terms:',
              average(self.ambiguous_terms_lower))
        print('Average of ids per term with lowercased terms and non-'
              'alphabetical characters removed:',
              average(self.ambiguous_terms_nows))

        print('FREQ DIST number of terms per id', self.id_freq_dist())
        print('FREQ DIST number of ids per term', self.term_freq_dist())
        print('FREQ DIST number of ids per term (terms are lowercased)',
              self.term_lw_freq_dist())
        print('FREQ DIST number of ids per term (terms are lowercased and '
              'symbols are removed', self.term_lw_nows_freq_dist())
        print('AVG Token Lenght', self.term_length_avg())

        for label, names in self.substats.values():
            print('-----------')
            print(label, 'stats')
            for substats in names.values():
                substats.display_stats()


def freq_dist(coll):
    '''
    Frequency distribution.
    '''
    return Counter(len(v) for v in coll.values())


def average(coll):
    '''
    Compute mean or mean length of coll's values.
    '''
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

        overall_stats = OverallStats()

        for row in reader:
            overall_stats.update(row['original_id'],
                                 row['term'],
                                 Resource=row['resource'],
                                 Entity_Type=row['entity_type'])

    return overall_stats
