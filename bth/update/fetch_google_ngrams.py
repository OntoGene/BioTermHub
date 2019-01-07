#!/usr/bin/env python3
# coding: utf8

# Author: Nico Colic, 2017
# Modified: Lenz Furrer, 2017


"""
Download Google-books ngrams using a frequency filter.
"""


import csv
import gzip
import logging
import argparse
import urllib.request
from collections import namedtuple

from ..core import settings
from ..lib.tools import TSVDialect


UNIGRAM_URL = ("http://storage.googleapis.com/books/ngrams/books/"
               "googlebooks-eng-all-1gram-20120701-{}.gz")
TOTALCOUNT_URL = ("http://storage.googleapis.com/books/ngrams/books/"
                  "googlebooks-eng-all-totalcounts-20120701.txt")
POS_TAGS = tuple('_' + tag for tag in
                 'NOUN VERB ADJ ADV PRON DET ADP NUM CONJ PRT . X'.split())
ENTRY = namedtuple('Entry', 'ngram year count volumecount')


def main():
    '''
    Run as script: Update the unigram count DB.
    '''
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument(
        '-q', '--quiet', action='store_true',
        help='suppress progress info')
    args = ap.parse_args()
    logging.basicConfig(format='%(asctime)s - %(message)s',
                        level=logging.WARNING if args.quiet else logging.INFO)
    update()


def update():
    '''
    Update the 1-gram DB (a gzipped TSV file).
    '''
    year_threshold = settings.gen_voc_year_threshold
    total_count = get_total_count(year_threshold)
    abs_threshold = total_count * settings.gen_voc_occ_threshold
    dest = settings.gen_voc_db_file

    with gzip.open(dest, 'wt', encoding='utf8') as f:
        writer = csv.writer(f, dialect=TSVDialect)
        for ngram, occ in uniq(iterfetch(), year_threshold, abs_threshold):
            freq = occ / total_count
            if ngram.endswith(POS_TAGS):
                ngram, pos = ngram.rsplit('_', 1)
            else:
                pos = ''
            writer.writerow((ngram, pos, freq))


def uniq(entries, year_threshold, occ_threshold):
    '''
    Merge and filter ngram-count entries.

    Skip entries older than the year threshold.
    Merge consecutive entries with the same surface form.
    Sum the occurrence counts.
    Skip entries with low counts.
    '''
    last = None
    occ = 0
    for entry in entries:
        if int(entry.year) < year_threshold:
            continue
        if entry.ngram != last:
            if occ >= occ_threshold:
                yield last, occ
            last = entry.ngram
            occ = 0
        occ += int(entry.count)
    # Last group of entries.
    if occ >= occ_threshold:
        yield last, occ


def iterfetch():
    '''
    Iterate over all lines from all 1-gram files.
    '''
    for initial in bytes(range(ord('a'), ord('z')+1)).decode():
        logging.info('processing %s', initial)
        url = UNIGRAM_URL.format(initial)
        with urllib.request.urlopen(url) as r:
            with gzip.open(r, 'rt', encoding='utf8') as f:
                for line in f:
                    yield ENTRY(*line.rstrip('\n\r').split('\t'))


def get_total_count(year_threshold):
    """
    Sum the 1-gram counts for the years >= year_threshold.
    """
    with urllib.request.urlopen(TOTALCOUNT_URL) as r:
        raw = r.read().decode('ascii')
    total_count = sum(
        int(count)
        for year, count, *_ in (e.split(',') for e in raw.split())
        if int(year) >= year_threshold)
    return total_count


if __name__ == '__main__':
    main()
