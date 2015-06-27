import matplotlib.pyplot as plt
import numpy as np
from collections import OrderedDict

def plotstats(rsc):
    stats = rsc.stats

    file_counter = 0
    for resource in stats:
        overall = stats[resource]
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
                plt.bar(X, statdict.values(), align='center', width=0.5, log=setlog)
                try:
                    statdict_keys = [count for count, label in statdict]
                except ValueError:
                    statdict_keys = statdict.keys()
                plt.xticks(X, statdict_keys)
                ymax = max(statdict.values()) + 1
                plt.ylim(0, ymax)
                plt.figtext(0.5, 0.85, resource + " - %s" % label, color='black', weight='roman', size='x-large')

                # try:
                #     plt.show()
                # except TypeError:
                #     print "No display available."

                plt.savefig(str(file_counter) + "_" + resource + "_" + label.replace("/", "_per_"), bbox_inches='tight', dpi=200)
                plt.clf()
                file_counter += 1


__author__ = 'vicawil'