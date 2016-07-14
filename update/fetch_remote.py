#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Check for changes to the remote resources and update the dumps.
'''


import os
import io
import time
import argparse
import logging
import urllib.request
import gzip
import tarfile
import zipfile

import requests

from termhub.core import settings
from termhub.inputfilters import FILTERS


def main():
    '''
    Run as script.
    '''
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument(
        'resources', nargs='*', choices=sorted(FILTERS)+['all'],
        metavar='resource', default='all',
        help='any subset of: %(choices)s')
    args = ap.parse_args()
    if 'all' in args.resources:
        args.resources = sorted(FILTERS)
    fetch(args.resources)


def fetch(which=FILTERS.keys()):
    '''
    Determine remote updates and download changed resources.
    '''
    for name in which:
        remote = RemoteChecker(name)
        if remote.sufficiently_recent():
            continue
        if remote.has_changed():
            remote.update()


class RemoteChecker(object):
    '''Checking and updating for one specific resource.'''
    def __init__(self, name, resource=None):
        if resource is None:
            resource = FILTERS[name]
        self.name = name
        self.resource = resource
        self.stat = StatLog(name)

    def last_check(self):
        '''
        When was the remote resource last checked for updates?
        '''
        return self.stat.checked

    def sufficiently_recent(self):
        '''
        The resource has been updated very recently.
        '''
        try:
            return time.time()-self.stat.modified <= settings.min_update_freq
        except TypeError:
            # There is no stat log yet.
            return False

    def has_changed(self):
        '''
        Check if the size of the remote resource has changed.
        '''
        answer = False
        for address, *_ in self.resource.update_info():
            previous_size = self.stat.sizes.get(address)
            current_size = self._content_size(address)
            answer |= current_size != previous_size
        try:
            self.stat.set(checked=int(time.time()))
        except TypeError:
            # There is no stat log yet.
            pass
        return answer

    @staticmethod
    def _content_size(address):
        req = urllib.request.Request(address, method='HEAD')
        resp = urllib.request.urlopen(req)
        return resp.headers.get('content-length')

    def update(self):
        '''
        Replace all resource dumps belonging to this resource.
        '''
        for address, *steps in self.resource.update_info():
            size = self._download(address, steps)
            self.stat.sizes[address] = size
        self.stat.set(modified=int(time.time()))

    def _download(self, address, steps):
        if address.startswith('ftp'):
            call = self._download_ftp
        else:
            call = self._download_http
        try:
            size = call(address, steps)
        except Exception:
            logging.exception('Download failed')
            raise
        else:
            return size

    def _download_ftp(self, address, steps):
        try:
            r = urllib.request.urlopen(address, timeout=settings.timeout)
            size = r.headers.get('content-length')
            self._pipe(r, *steps)
        finally:
            r.close()
        return size

    def _download_http(self, address, steps):
        try:
            r = requests.get(address, stream=True, timeout=settings.timeout)
            r.raise_for_status()  # throw an exception on HTTP error codes
            size = r.headers.get('content-length')
            self._pipe(r.raw, *steps)
        finally:
            r.close()
        return size

    def _pipe(self, stream, nextstep, *remaining):
        '''
        Control a processing pipeline mechanism.
        '''
        if remaining:
            # More steps to come. Continue piping.
            if callable(nextstep):
                # A preprocessor function.
                self._pipe(nextstep(stream), *remaining)
            else:
                # One of the decompression methods ('gz', 'zip').
                call = getattr(self, '_{}'.format(nextstep))
                call(stream, remaining)
        else:
            # `nextstep` is a file name.
            fn = os.path.join(settings.path_dumps, nextstep)
            with open(fn + '.tmp', 'wb') as f:
                for chunk in stream:
                    f.write(chunk)
            os.rename(fn + '.tmp', fn)

    def _gz(self, stream, steps):
        with gzip.open(stream, 'rb') as f:
            self._pipe(f, *steps)

    def _tar(self, stream, steps):
        targets = self._step_forking(*steps)
        with tarfile.open(fileobj=stream, mode='r|*') as t:
            for info in iter(t.next, None):
                if info.name in targets:
                    f = t.extractfile(info)
                    self._pipe(f, *targets[info.name])
                    f.close()

    def _zip(self, stream, steps):
        '''
        Usage of _zip() is discouraged, since it requires
        random access to the file (cannot stream).
        '''
        targets = self._step_forking(*steps)
        with zipfile.ZipFile(io.BytesIO(stream.read())) as z:
            for member, remaining in targets.items():
                with z.open(member) as f:
                    self._pipe(f, *remaining)

    @staticmethod
    def _step_forking(nextstep, *remaining):
        '''
        Resolve a forking in the steps sequence.

        Extracting multiple files from a single archive means
        there is a forking in the steps sequence.
        The next step is a list of extraction targets.
        Each target must be given as a sequence with the
        target member as first element.
        Any remaining steps on the top-level are seen as
        common to all targets.  They need to be copied to
        the step sequence of each target.
        '''
        return {target[0]: tuple(target[1:])+remaining for target in nextstep}


class StatLog(object):
    '''Cached reading/writing of the dump stat log.'''
    def __init__(self, name):
        self._logfn = settings.rel('update', 'logs', '{}.log'.format(name))
        self.sizes = {}
        try:
            # Get any previous stat info.
            with open(self._logfn) as f:
                modified, checked = [int(n) for n in next(f).split()]
                for line in f:
                    fn, size = line.rsplit(maxsplit=1)
                    try:
                        self.sizes[fn] = int(size)
                    except ValueError:
                        # No size yet (parsed "None").
                        pass
        except OSError:
            modified, checked = None, None
        self.modified = modified
        self.checked = checked

    def set(self, **kwargs):
        'Update the cached and on-disk values.'
        for key, value in kwargs.items():
            setattr(self, key, value)
        if self.checked < self.modified:
            self.checked = self.modified
        self.write_log()

    def write_log(self):
        'Write updated values to disk.'
        os.makedirs(settings.rel('update', 'logs'), exist_ok=True)
        with open(self._logfn, 'w') as f:
            entries = [(self.modified, self.checked)] + list(self.sizes.items())
            for entry in entries:
                f.write('{}\t{}\n'.format(*entry))


if __name__ == '__main__':
    main()
