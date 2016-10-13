#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015


import re
import os
import sys
import matplotlib
matplotlib.use('pdf')  # Choose a non-interactive backend; the default GTK causes an error unless logged in with X forwarding.
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator, LogLocator
from matplotlib.ticker import FormatStrFormatter


from termhub.core import settings
from termhub.stats import statistics_termfile

STATSPATH = settings.path_stats

BAR_COLORS = 'red green blue yellow'.split()


def main():
    '''
    Run as script: process the given term list.
    '''
    plotstats(sys.argv[1])


def plotstats(filename):
    '''
    Create an ambiguity/synonymy plot for each resource and entity type.
    '''
    os.makedirs(STATSPATH, exist_ok=True)

    overall_stats = statistics_termfile.process_file(filename)
    stats = {}

    stats["Resource"] = overall_stats.resources
    stats["Entity type"] = overall_stats.entity_types

    for stat_set in ("Resource", "Entity type"):
        for group in stats[stat_set]:
            freq_dists = (
                stats[stat_set][group].id_freq_dist(),
                stats[stat_set][group].term_freq_dist(),
                stats[stat_set][group].term_lw_freq_dist(),
                stats[stat_set][group].term_lw_nows_freq_dist(),
            )
            fn = os.path.join(STATSPATH,
                              '{}.png'.format(re.sub(r'[\W]+', '_', group)))
            drawbars(freq_dists, group, 'ratio', 'count', fn)
    fn = os.path.join(STATSPATH, 'Legend.png')
    drawlegend(fn)


def drawbars(freqdists, title, xlab, ylab, fn):
    '''
    Draw a bar plot for a freqency distribution.
    '''
    # create the x-axis
    fig = plt.figure()
    ax = plt.subplot(111) # we create a variable for the subplot so we can access the axes

    # set the top and right axes to invisible
    # matplotlib calls the axis lines spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    # turn off ticks where there is no spine
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')

    ax.set_xscale('log', nonposx='clip')
    ax.set_yscale('log', nonposy='clip')
    ax.xaxis.set_major_formatter(FormatStrFormatter('%d'))
    ax.yaxis.set_major_formatter(FormatStrFormatter('%d'))
    ya = ax.get_yaxis()
    ya.set_major_locator(MaxNLocator(nbins=5, integer=True))
    xa = ax.get_xaxis()
    xa.set_major_locator(LogLocator(base=2))

    offset = -0.2

    colors = iter(BAR_COLORS)
    for counter in freqdists:
        key = [key + offset for key in counter]
        value = counter.values()
        color = next(colors)
        # width argument slims down the bars
        plt.bar(key, value, color=color, align='center', width=0.1)
        offset += 0.1
    plt.hold(True)

    plt.xlabel(xlab, fontsize=18)
    plt.ylabel(ylab, fontsize=18)

    plt.title(title, fontsize=24)
    ax.autoscale()

    plt.savefig(fn, bbox_inches='tight')
    plt.close(fig)


def drawlegend(fn):
    '''
    Write the common legend into a separate file.
    '''
    fig = plt.figure()
    rects = [matplotlib.patches.Rectangle((0, 0), 1, 1, color=c)
             for c in BAR_COLORS]
    labels = ['terms per ID',
              'IDs per term',
              'IDs per lower-cased term',
              'IDs per lower-cased, punctuation-stripped term']
    fig.legend(rects, labels, 'center')
    fig.set_size_inches(3.9, 1.2)
    plt.savefig(fn, bbox_inches='tight')
    plt.close(fig)


if __name__ == "__main__":
    main()
