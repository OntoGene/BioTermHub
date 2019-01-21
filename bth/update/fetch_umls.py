#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2019


'''
Download the latest version of UMLS.
'''


import logging
import argparse
import datetime as dt
import subprocess as sp
from pathlib import Path

from ..core import settings
from ..lib.tools import quiet_option, setup_logging


URL = 'https://download.nlm.nih.gov/umls/kss/{0}/umls-{0}-full.zip'
ROOT = Path(settings.path_umls_maps)


def main():
    '''
    Run as script.
    '''
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument(
        '-r', '--release', default=predict_release(),
        help='release number')
    ap.add_argument(
        '-d', '--destination', default=ROOT, type=Path,
        help='destination directory of the downloaded release dump')
    quiet_option(ap)
    args = ap.parse_args()
    setup_logging(args.quiet)
    fetch_full_release(args.release, args.destination)


def predict_release():
    '''
    Based on the current date, guess the name of the latest UMLS release.
    '''
    # The UMLS is released in May and November.
    # The last few releases all happened before the 10th of the month.
    d = dt.date.today()
    if (d.month, d.day) < (5, 10):
        year, version = d.year - 1, 'AB'
    elif (d.month, d.day) < (11, 10):
        year, version = d.year, 'AA'
    else:
        year, version = d.year, 'AB'
    return '{}{}'.format(year, version)


def fetch_full_release(release: str, destination: Path = ROOT):
    '''
    Download this release into the destination directory.
    '''
    url = URL.format(release)
    fn = url.rsplit('/', 1)[1]
    intermediate = ROOT/fn
    target = destination/fn
    try:
        _call(url, intermediate)
    except Exception:
        logging.error(
            'Could not download %s.\n'
            'Please make sure the UTS downloader script is properly installed '
            '(using `make umls-update`) and the UTS credentials are set.', fn)
        raise
    if destination != ROOT:
        logging.info('Moving downloaded file to %s', target)
        intermediate.rename(target)
    return str(target)


def _call(url, target):
    uts_script = str(ROOT/'curl-uts-download.sh')
    args = ['/bin/bash', uts_script, url]
    logging.info('Running UTS download script: %s', ' '.join(args))
    compl = sp.run(args, check=True, cwd=str(ROOT),
                   stdout=sp.PIPE, stderr=sp.STDOUT)
    if not target.exists():
        raise ValueError('downloading failed: {}'.format(compl.stdout))
    logging.info('Downloaded to: %s', target)
