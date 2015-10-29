from __future__ import division
__author__ = 'vicawil'

import matplotlib.cm as cm
import operator as o

import math
import sys
import os
import os.path
import matplotlib
matplotlib.use('pdf')  # Choose a non-interactive backend; the default GTK causes an error unless logged in with X forwarding.
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator, MultipleLocator, AutoMinorLocator, ScalarFormatter, LinearLocator, AutoLocator, LogLocator
from matplotlib.ticker import FormatStrFormatter, LogFormatter
import numpy as np
HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, '..', 'tilia'))
import statistics_termfile
import re

import settings

STATSPATH = settings.path_stats

def plotstats(filename):
    if not os.path.exists(STATSPATH):
        os.mkdir(STATSPATH)

    overall_stats = statistics_termfile.process_file(filename)
    stats = {}

    stats["Resource"] = overall_stats.resource_dict
    stats["Entity type"] = overall_stats.entity_type_dict
    
    
    for stat_set in ("Resource", "Entity type"):
        for group in stats[stat_set]:
            freq_dists = (stats[stat_set][group].id_freq_dist(),
                     stats[stat_set][group].term_freq_dist(),
                     stats[stat_set][group].term_lw_freq_dist(),
                     stats[stat_set][group].term_lw_nows_freq_dist())
            
            barPlt(freq_dists, group, 'ratio', 'count', group)
        
def barPlt(freqdist_list, title, xlab, ylab, group):
    # create the x-axis
    fig = plt.figure()
    ax = plt.subplot(111) # we create a variable for the subplot so we can access the axes

    # set the top and right axes to invisible
    # matplotlib calls the axis lines spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)# turn off ticks where there is no spine
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')
    ax.set_xscale('log', nonposx='clip')
    ax.set_yscale('log', nonposy='clip')
    ax.xaxis.set_major_formatter(FormatStrFormatter('%d'))
    ax.yaxis.set_major_formatter(FormatStrFormatter('%d'))
    ya = ax.get_yaxis()
    ya.set_major_locator(MaxNLocator(nbins = 5, integer=True))
    xa = ax.get_xaxis()
    xa.set_major_locator(LogLocator(base=2))
    
    offset = -0.2
    
    colors = ('red','green','blue','yellow')
    for idx, counter in enumerate(freqdist_list):
        key = [key + offset for key in counter.keys()]
        value = counter.values()
        bar_plt = plt.bar(key, value, color = colors[idx], align = 'center', width = 0.1) # width argument slims down the bars
        offset += 0.1
    plt.hold(True)
    
    plt.xlabel(xlab, fontsize = 18)
    plt.ylabel(ylab, fontsize = 18)
    
    plt.title(title, fontsize = 24)
    ax.autoscale()
    
    plt.savefig(STATSPATH+'%s.png' % re.sub('[/\s]', '_', group), bbox_inches='tight')
    plt.close(fig)


if __name__ == "__main__":
    plotstats("output.csv")
