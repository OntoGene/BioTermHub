import os.path
import sys
HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, '..', 'lib'))
import csv
from tabulate import tabulate
from unicode_csv import *
from collections import OrderedDict

def write2tsv(rsc):
    with open("stats_overall.txt", "w") as statsfile:
        fieldnames = ["ids", "terms", "terms/id", "ids/term", "avg. ids/term", "resource", "avg. terms/id"]
        writer = UnicodeDictWriter(statsfile, dialect= csv.excel_tab, quotechar=str("\""), fieldnames = fieldnames,
                                   quoting= csv.QUOTE_NONE, restval='__')
        writer.writeheader()
        for resource in rsc.stats:
            rowdict = rsc.stats[resource].copy()
            rowdict.pop("ratios")
            rowdict["resource"] = resource
            writer.writerow(rowdict)

    with open("stats_ratios_terms_per_id.txt", "w") as statsfile:

        fieldnames = [" ".join((str(count), label)) for count, label
                      in sorted(rsc.stats["total"]["ratios"]["terms/id"].keys(),
                                key = lambda x: x[0])]

        fieldnames.insert(0, "resource")

        writer = UnicodeDictWriter(statsfile, dialect= csv.excel_tab, quotechar=str("\""), fieldnames = fieldnames,
                            quoting= csv.QUOTE_NONE, restval='__')

        writer.writeheader()

        for resource in rsc.stats:
            if not (resource == "uniprot" or resource == "total"):
                rowdict = {" ".join((str(count), label)):value for (count, label), value in rsc.stats[resource]["ratios"].iteritems()}
                rowdict["resource"] = resource
                print rowdict
                writer.writerow(rowdict)

        rowdict = {" ".join((str(count), label)):value for (count, label), value in rsc.stats["total"]["ratios"]["terms/id"].iteritems()}
        rowdict["resource"] = "total"
        writer.writerow(rowdict)

    with open("stats_ratios_ids_per_term.txt", "w") as statsfile:
        fieldnames = [" ".join((str(count), label)) for count, label
                      in sorted(rsc.stats["total"]["ratios"]["ids/term"].keys(),
                                key = lambda x: x[0])]

        fieldnames.insert(0, "resource")

        writer = UnicodeDictWriter(statsfile, dialect= csv.excel_tab, quotechar=str("\""), fieldnames = fieldnames,
                            quoting= csv.QUOTE_NONE, restval='__')

        writer.writeheader()

        rowdicts = OrderedDict({"uniprot": rsc.stats["uniprot"]["ratios"], "total": rsc.stats["total"]["ratios"]["ids/term"]})
        for resource in rowdicts:
            rowdict = {" ".join((str(count), label)):value for (count, label), value in rowdicts[resource].iteritems()}
            rowdict["resource"] = resource
            print rowdict
            writer.writerow(rowdict)