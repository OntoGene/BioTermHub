#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Background handler for collecting and plotting statistics.
'''


import os
import multiprocessing as mp

from ..lib.tools import sanitise


class BGPlotter:
    '''
    Handler for plotting statistics in the background.
    '''
    def __init__(self, dest_dir):
        self.dest_dir = dest_dir
        self._subplotters = {}

    def update(self, id_, term, group):
        '''
        Add a record.
        '''
        try:
            sub = self._subplotters[group]
        except KeyError:
            sub = self._subplotters[group] = StatWorker(group, self.dest_dir)
        sub.update(id_, term)

    def plot(self):
        '''
        Finish collection phase, start plotting.

        Return the file names of all plots
        (which might not yet exist).
        '''
        return [stat.plot() for stat in self._subplotters.values()]


class StatWorker:
    '''
    Wrapper for a collecting/plotting child process.
    '''
    def __init__(self, title, dest_dir):
        self.title = title
        self.dest = os.path.join(dest_dir, sanitise(title) + '.png')
        self._queue = mp.Queue()
        self._proc = mp.Process(target=self._run,
                                args=(title, self._queue, self.dest))
        self._proc.start()

    def update(self, id_, term):
        '''
        Pass a record to the underlying StatsCollector.
        '''
        self._queue.put((id_, term))

    def plot(self):
        '''
        Stop collecting, start plotting.

        Return the destination file name.
        '''
        self._queue.put(None)
        return self.dest

    @staticmethod
    def _run(title, queue, dest):
        from .statistics_termfile import StatsCollector
        from .statplot_poststats import plot_one

        stat = StatsCollector('Group', title)

        for id_, term in iter(queue.get, None):
            stat.update(id_, term)

        plot_one(title, stat, dest)
