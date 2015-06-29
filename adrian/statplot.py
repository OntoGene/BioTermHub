from __future__ import division
import math
import os
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import numpy as np
from collections import OrderedDict

STATSPATH = "stats/"

def plotstats(rsc):

    if not os.path.exists(STATSPATH):
        os.mkdir("stats")

    stats = rsc.stats

    file_counter = 0
    for resource in stats:
        overall = stats[resource].copy()
        ratios_raw = overall.pop("ratios")
        if resource != "total":
            ratios = OrderedDict(sorted(ratios_raw.items(), key = lambda x: x[0][0]))
        else:
            ratios = ratios_raw

        for label, statdict in [("overall", overall), ("ratios", ratios)]:
            if resource == "total" and label == "ratios":
                ratio_dicts = [OrderedDict(sorted(statdict["terms/id"].items(), key = lambda x: x[0][0])),
                               OrderedDict(sorted(statdict["ids/term"].items(), key = lambda x: x[0][0]))]
            else:
                ratio_dicts = [statdict]
            for statdict in ratio_dicts:
                X = np.arange(len(statdict))
                setlog = True if resource == "total" and label == "overall" else False
                try:
                    statdict_keys = [count for count, label in statdict]
                    fig = plt.bar(statdict_keys, statdict.values(), align='center', width=0.15, log=setlog)
                except:
                    fig = plt.bar(X, statdict.values(), align='center', width=0.15, log=setlog)

                try:
                    statdict_keys = [count for count, label in statdict]
                except ValueError:
                    statdict_keys = statdict.keys()

                if label == "overall":
                    plt.xticks(X, statdict_keys)
                elif label == "ratios":
                    plt.gca().xaxis.set_major_locator(MaxNLocator(15))

                ymax_val = max(statdict.values())
                ymax = math.ceil(ymax_val + ymax_val / 100) if ymax_val > 20 else ymax_val + 2
                xmax= len(statdict)

                ymin = 0 if ymax_val < 300 else math.ceil(ymax_val/100)
                plt.ylim(0, ymax)
                plt.grid(True)
                plt.xlabel(label)
                plt.ylabel('count')
                # plt.xlim(xmax - 0.5, xmax + 0.5)
                # plt.figtext(0.5, 0.85, resource + " - %s" % label, color='black', weight='roman', size='x-large')
                # plt.autoscale(tight=False)

                # try:
                #     plt.show()
                # except TypeError:
                #     print "No display available."

                plt.savefig(STATSPATH + str(file_counter) + "_" + resource + "_" + label.replace("/", "_per_"), bbox_inches='tight', dpi=200)
                plt.clf()
                file_counter += 1

    plt.close()

__author__ = 'vicawil'