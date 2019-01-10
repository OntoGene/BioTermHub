#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Background handler for collecting and plotting statistics.
'''


import csv
from typing import List, Dict
from pathlib import Path
import multiprocessing as mp

from ..lib.tools import sanitise, Fields, TSVDialect


class BGPlotter:
    '''
    Handler for plotting statistics in the background.
    '''
    def __init__(self, dest_dir: Path, proc_type=mp.Process):
        self.dest_dir = dest_dir
        self.proc_type = proc_type
        self._subplotters = {}  # type: Dict[str, StatWorker]

    @property
    def destinations(self) -> List[Path]:
        '''Paths to the plot files.'''
        return [stat.dest for stat in self._subplotters.values()]

    def update(self, id_, term, group):
        '''
        Add a record.
        '''
        try:
            sub = self._subplotters[group]
        except KeyError:
            sub = StatWorker(group, self.dest_dir, self.proc_type)
            self._subplotters[group] = sub
        sub.update(id_, term)

    def plot(self) -> List[Path]:
        '''
        Finish collection phase, start plotting.

        Return the file names of all plots
        (which might not yet exist).
        '''
        return [stat.plot() for stat in self._subplotters.values()]

    def from_disk(self, path: Path, **kwargs) -> List[Path]:
        '''
        Create the plots from an on-disk file.
        '''
        with path.open(encoding='utf8') as f:
            return self.from_file(f, **kwargs)

    def from_file(self, file, header=True,
                  fields=('original_id', 'term', 'entity_type')):
        '''
        Create the plots from an open file.

        Replace the use of update(), ... update(), plot().
        '''
        # Create a list of operator.itemgetter instances.
        getters = [getattr(Fields, f).fget for f in fields]
        rows = csv.reader(file, dialect=TSVDialect)
        if header:
            next(rows)
        for row in rows:
            self.update(*(f(row) for f in getters))
        return self.plot()

    def join(self):
        '''
        Block until plotting has finished.
        '''
        for stat in self._subplotters.values():
            stat.join()


class StatWorker:
    '''
    Wrapper for a collecting/plotting child process.
    '''
    def __init__(self, title, dest_dir: Path, proc_type):
        self.title = title
        self.dest = dest_dir / (sanitise(title) + '.png')
        self._queue = mp.Queue()  # type: mp.Queue
        self._proc = proc_type(target=self._run,
                               args=(title, self._queue, self.dest))
        self._proc.start()

    def update(self, id_, term):
        '''
        Pass a record to the underlying StatsCollector.
        '''
        self._queue.put((id_, term))

    def plot(self) -> Path:
        '''
        Stop collecting, start plotting.

        Return the destination file name.
        '''
        self._queue.put(None)
        return self.dest

    def join(self):
        '''
        Block until plotting has finished.
        '''
        self._proc.join()

    @staticmethod
    def _run(title, queue, dest):
        from .statistics_termfile import StatsCollector
        from .statplot_poststats import plot_one

        stat = StatsCollector('Group', title)

        for id_, term in iter(queue.get, None):
            stat.update(id_, term)

        plot_one(title, stat, str(dest))
