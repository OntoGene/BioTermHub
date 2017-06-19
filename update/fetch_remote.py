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
    ap.add_argument(
        '-f', '--force', action='store_true',
        help='force a new download, even if up-to-date')
    ap.add_argument(
        '-q', '--quiet', action='store_true',
        help='no progress info')
    args = ap.parse_args()
    if 'all' in args.resources:
        args.resources = sorted(FILTERS)
    logging.basicConfig(format='%(asctime)s: %(message)s',
                        level=logging.WARNING if args.quiet else logging.INFO)
    fetch(args.resources, args.force)


def fetch(which=FILTERS.keys(), force=False):
    '''
    Determine remote updates and download changed resources.
    '''
    for name in which:
        remote = RemoteChecker(name)
        if not force and remote.sufficiently_recent():
            logging.info('Skipping %s (recent update)', name)
        elif force or remote.has_changed():
            logging.info('Updating %s ...', name)
            remote.update()
        else:
            logging.info('No change for %s', name)


class RemoteChecker(object):
    '''Checking and updating for one specific resource.'''
    def __init__(self, name, resource=None):
        if resource is None:
            resource = FILTERS[name]
        self.name = name
        self.resource = resource
        self.stat = StatLog(name, resource)

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
            # The resource has never been downloaded.
            return False

    def has_changed(self, check_interval=settings.min_check_freq):
        '''
        Check if the size of the remote resource has changed.
        '''
        # Check cached values.
        if self.stat.has_changed:
            # No need to check again: We don't expect the remote to roll back
            # changes.
            return True
        try:
            if time.time()-self.stat.checked <= check_interval:
                # Was recently checked, don't bother the remote with another
                # request.
                # The value must be False: otherwise stat.has_changed would
                # have been True.
                return False
        except TypeError:
            # Has never been checked before.
            pass

        # Check remotely.
        answer = False
        for address, *_ in self.resource.update_info():
            previous_size = self.stat.sizes.get(address)
            current_size = self._content_size(address)
            if current_size != previous_size:
                answer = True
                break
        try:
            self.stat.just_checked(answer)
        except ValueError:
            # The resource has never been downloaded.
            pass
        return answer

    @staticmethod
    def _content_size(address):
        try:
            req = urllib.request.Request(address, method='HEAD')
            resp = urllib.request.urlopen(req)
            size = int(resp.headers.get('content-length'))
        except Exception:
            logging.exception('Remote size check failed.')
            raise
        finally:
            try:
                resp.close()
            except NameError:
                pass
        return size

    def update(self):
        '''
        Replace all resource dumps belonging to this resource.
        '''
        for address, *steps in self.resource.update_info():
            size = self._download(address, steps)
            self.stat.sizes[address] = size
        self.stat.just_modified()

    def _download(self, address, steps):
        try:
            r = urllib.request.urlopen(address, timeout=settings.timeout)
            size = int(r.headers.get('content-length'))
            self._pipe(r, *steps)
        except Exception:
            logging.exception('Download failed')
            raise
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
    def __init__(self, name, resource):
        self._logfn = os.path.join(settings.path_update_logs,
                                   '{}.log'.format(name))
        self.sizes = {}
        try:
            # Get any previous stat info.
            with open(self._logfn) as f:
                modified, checked, changed = [int(n) for n in next(f).split()]
                for line in f:
                    fn, size = line.rsplit(maxsplit=1)
                    try:
                        self.sizes[fn] = int(size)
                    except ValueError:
                        # No size yet (parsed "None").
                        pass
        except FileNotFoundError:
            modified = checked = self._init_time(resource)
            # If the resource is not present yet (modified is None),
            # then an update is needed for sure.
            changed = modified is None
        self.modified = modified
        self.checked = checked
        self.has_changed = bool(changed)

    @staticmethod
    def _init_time(resource):
        "Initialise with the file's m-time."
        try:
            return min(int(os.path.getmtime(fn)) for fn in resource.dump_fns())
        except FileNotFoundError:
            return None

    def just_modified(self):
        'Update the `modified` value.'
        self.modified = int(time.time())
        # The `checked` timestamp should never be older than `modified`.
        self.checked = self.modified
        self.has_changed = False
        self._write_log()

    def just_checked(self, outcome):
        'Update the `checked` and `has_changed` values.'
        # Make sure we don't write "None" into the log file.
        if self.modified is None:
            raise ValueError('The `modified` date cannot be None.')
        self.checked = int(time.time())
        self.has_changed = outcome
        self._write_log()

    def _write_log(self):
        'Write updated values to disk.'
        os.makedirs(settings.path_update_logs, exist_ok=True)
        with open(self._logfn + '.tmp', 'w') as f:
            f.write('{}\t{}\t{:d}\n'
                    .format(self.modified, self.checked, self.has_changed))
            for fn, size in self.sizes.items():
                f.write('{}\t{}\n'.format(fn, size))
        os.rename(self._logfn + '.tmp', self._logfn)


if __name__ == '__main__':
    main()
