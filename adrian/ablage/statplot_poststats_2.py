from __future__ import division
__author__ = 'vicawil'

import matplotlib.cm as cm
import operator as o

import math
import sys
import os
import os.path
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import numpy as np
HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, '..', 'tilia'))
import statistics_termfile

STATSPATH = "stats/"

def plotstats(filename):
    if not os.path.exists(STATSPATH):
        os.mkdir("stats")

    overall_stats = statistics_termfile.process_file(filename)
    stats = {}

    stats["Resource"] = overall_stats.resource_dict.iteritems()
    stats["Entity type"] = overall_stats.entity_type_dict.iteritems()

    file_counter = 0
    for stat_category in stats:
        term_freq_table = []
        id_freq_table = []
        fig_term = plt.figure()
        ax_term = fig_term.add_subplot(111)
        fig_id = plt.figure()
        ax_id = fig_id.add_subplot(111)
        for resource, resource_stats in stats[stat_category]:

            term_freq_dist = resource_stats.term_freq_dist()
            id_freq_dist = resource_stats.id_freq_dist()
            term_freq_table.extend([[resource, ratio, count] for ratio, count in term_freq_dist.iteritems()])
            id_freq_table.extend([[resource, ratio, count] for ratio, count in id_freq_dist.iteritems()])

        term_freq_table_array = np.array(term_freq_table, dtype = object)
        id_freq_table_array = np.array(id_freq_table, dtype = object)
        barplot(ax_term, term_freq_table_array)
        barplot(ax_id, id_freq_table_array)

#
def barplot(ax, dpoints):
    '''
    Create a barchart for data across different categories with
    multiple conditions for each category.

    @param ax: The plotting axes from matplotlib.
    @param dpoints: The data set as an (n, 3) numpy array
    '''

    # Aggregate the conditions and the categories according to their
    # mean values
    conditions = [(c, np.mean(dpoints[dpoints[:,0] == c][:,2].astype(float)))
                  for c in np.unique(dpoints[:,0])]
    categories = [(c, np.mean(dpoints[dpoints[:,1] == c][:,2].astype(float)))
                  for c in np.unique(dpoints[:,1])]

    # sort the conditions, categories and data so that the bars in
    # the plot will be ordered by category and condition
    conditions = [c[0] for c in sorted(conditions, key=o.itemgetter(1))]
    categories = [c[0] for c in sorted(categories, key=o.itemgetter(1))]

    dpoints = np.array(sorted(dpoints, key=lambda x: categories.index(x[1])))

    # the space between each set of bars
    space = 0.3
    n = len(conditions)
    width = (1 - space) / (len(conditions))

    # Create a set of bars at each position
    for i,cond in enumerate(conditions):
        indeces = range(1, len(categories)+1)
        vals = dpoints[dpoints[:,0] == cond][:,2].astype(np.float)
        pos = [j - (1 - space) / 2. + i * width for j in indeces]
        ax.bar(pos, vals, width=width, label=cond,
               color=cm.Accent(float(i) / n))

    # Set the x-axis tick labels to be equal to the categories
    ax.set_xticks(indeces)
    ax.set_xticklabels(categories)
    plt.setp(plt.xticks()[1], rotation=90)

    # Add the axis labels
    ax.set_ylabel("RMSD")
    ax.set_xlabel("Structure")

    # Add a legend
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], loc='upper left')



if __name__ == "__main__":
    plotstats("output.csv")