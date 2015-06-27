import matplotlib.pyplot as plt
import numpy as np
from collections import OrderedDict

def plotstats(rsc):
    stats = rsc.stats
    for resource in stats:
        overall = stats[resource]
        if not resource == "total":
            ratios_raw = overall.pop("ratios")
            ratios = OrderedDict(sorted(ratios_raw.items(), key = lambda x: x[0][0]))
        else:
            ratios = None
        for label, statdict in [("overall", overall), ("ratios", ratios)]:
            if statdict:
                X = np.arange(len(statdict))
                setlog = True if resource == "total" else False
                plt.bar(X, statdict.values(), align='center', width=0.5, log=setlog)
                try:
                    statdict_keys = [" ".join((str(count), label)) for count, label in statdict]
                except ValueError:
                    statdict_keys = statdict.keys()
                plt.xticks(X, statdict_keys)
                ymax = max(statdict.values()) + 1
                plt.ylim(0, ymax)
                plt.figtext(0.5, 0.85, resource + " - %s" % label, color='black', weight='roman', size='x-large')
                plt.show()


__author__ = 'vicawil'
