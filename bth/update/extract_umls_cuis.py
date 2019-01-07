#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2018


'''
Extract UMLS entries for target resources.
'''


import io
import csv
import gzip
import codecs
import zipfile
import argparse
from pathlib import Path

from ..core import settings
from ..inputfilters import FILTERS


DEFAULT_SOURCES = {rec.umls_abb: Path(rec.umls_dump_fn())
                   for rec in FILTERS.values() if hasattr(rec, 'umls_abb')}

# Columns in MRCONSO.RRF.
CUI = 0
SAB = 11
CODE = 13
STR = 14


def main():
    '''
    Run as script.
    '''
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument(
        '-u', '--umls-full-zip', metavar='PATH', default=find_umls_zip(),
        help='Zip archive with full UMLS download')
    ap.add_argument(
        '-s', '--sources', metavar='SRC', nargs='+',
        default=sorted(DEFAULT_SOURCES),
        help='target sources (UMLS SAB field)')
    ap.add_argument(
        '-t', '--target-dir', metavar='PATH',
        help='directory for saving extracted TSV files')
    args = ap.parse_args()
    extract_targets(**vars(args))


def extract_targets(umls_full_zip, sources=DEFAULT_SOURCES, target_dir=None):
    '''
    Extract target entries into a separate TSV per resource.
    '''
    if target_dir is not None:
        target_dir = Path(target_dir)
        target_dir.mkdir(exist_ok=True)
        sources = {sab: target_dir/'{}.tsv'.format(sab) for sab in sources}
    elif not isinstance(sources, dict):
        sources = {sab: DEFAULT_SOURCES[sab] for sab in sources}
    _extract_targets(umls_full_zip, sources)


def _extract_targets(umls_full_zip, sources):
    files = []
    writers = {}
    for sab, path in sources.items():
        files.append(path.open('w', encoding='utf-8'))
        writers[sab] = csv.writer(files[-1], delimiter='\t', quotechar=None)

    for sab, *entry in iterentries(umls_full_zip, sources):
        writers[sab].writerow(entry)

    for f in files:
        f.close()


def find_umls_zip():
    '''
    Locate the newest UMLS full Zip archive.
    '''
    p = Path(settings.path_umls_maps)
    return max(map(str, p.glob('umls-*-full.zip')), default=None)


def iterentries(umls_full_zip, sources):
    '''
    Iterate over SAB/CUI/CODE/STR entries.
    '''
    conso = concatconso(umls_full_zip)
    return parseconso(conso, sources)


def concatconso(umls_full_zip):
    '''
    Line-iterate over the concatenated fragments of the MRCONSO table.
    '''
    # Concatenate binary line buffers before decoding -- in case a multi-byte
    # UTF-8 character is split apart.
    return codecs.iterdecode(_concatconso(umls_full_zip), 'utf-8')


def _concatconso(umls_full_zip):
    # The original table is not necessarily split at the end of a line.
    # Therefore, iteration is delayed by one step, in order to hold back the
    # last line of every fragment.  Once the next fragment is opened, the last
    # seen line is checked for a trailing newline; if this fails, it is joined
    # with the next line.
    last = b''  # treat the beginning like a partial line to forward the iterator
    # Iterate over fragments.
    for nlm in iternlm(umls_full_zip):
        for conso in iterconsofragments(nlm):
            # If last is a partial line, forward the iterator without yielding.
            if not last.endswith((b'\n', b'\r\n')):
                try:
                    current = next(conso)
                except StopIteration:
                    break
                last += current

            # Iterate over lines, delayed by one step.
            for current in conso:
                yield last
                last = current

    # Don't forget the very last line.
    if last:
        yield last


def iternlm(umls_full_zip):
    '''
    Find the inner archives (zip files with .nlm extension).
    '''
    with zipfile.ZipFile(umls_full_zip) as full:
        for info in full.infolist():
            if info.filename.endswith('.nlm'):
                with full.open(info) as nlm:
                    if not nlm.seekable():
                        # Read the archive into memory to get a seekable file.
                        nlm = io.BytesIO(nlm.read())
                    yield nlm


def iterconsofragments(nlm):
    '''
    Find the MRCONSO table fragment(s) in a .nlm archive.
    '''
    with zipfile.ZipFile(nlm) as inner:
        for info in inner.infolist():
            if info.filename.split('/')[-1].startswith('MRCONSO'):
                with inner.open(info) as f:
                    with gzip.open(f, mode='rb') as conso:
                        yield conso


def parseconso(stream, sources):
    '''
    Iterate over quadruples <SAB, CUI, CODE, STR> from MRCONSO.RRF.
    '''
    r = csv.reader(stream, delimiter='|', quotechar=None)
    for row in r:
        if row[SAB] in sources:
            yield row[SAB], row[CUI], row[CODE], row[STR]


if __name__ == '__main__':
    main()
