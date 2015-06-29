import os.path
import sys
HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, '..', 'lib'))
import csv
import tabulate
from unicode_csv import *
from collections import OrderedDict

STATSPATH = "stats/"

def write2tsv(rsc, latex = True):
    stats_latex = "stats"

    with open(STATSPATH + "stats_overall.txt", "w") as statsfile:
        fieldnames = ["resource", "ids", "terms", "terms/id", "ids/term", "avg. ids/term", "avg. terms/id"]
        writer = UnicodeDictWriter(statsfile, dialect= csv.excel_tab, quotechar=str("\""), fieldnames = fieldnames,
                                   quoting= csv.QUOTE_NONE, restval='__')
        writer.writeheader()

        lrows = [fieldnames]

        for resource in rsc.stats:
            rowdict = rsc.stats[resource].copy()
            rowdict.pop("ratios")
            rowdict["resource"] = resource
            writer.writerow(rowdict)
            if latex:
                if resource != "total":
                    lrows.append([rowdict[field] for field in fieldnames])
                else:
                    lrows.append([r"\textbf{" + str(rowdict[field])+"}" for field in fieldnames])

        if latex:
            write2latex(lrows, stats_latex)


    with open(STATSPATH + "stats_ratios_terms_per_id.txt", "w") as statsfile:

        fieldnames = [" ".join((str(count), label)) for count, label
                      in sorted(rsc.stats["total"]["ratios"]["terms/id"].keys(),
                                key = lambda x: x[0])]

        fieldnames.insert(0, "resource")

        writer = UnicodeDictWriter(statsfile, dialect= csv.excel_tab, quotechar=str("\""), fieldnames = fieldnames,
                            quoting= csv.QUOTE_NONE, restval='__')

        writer.writeheader()

        lrows = [fieldnames]

        for resource in rsc.stats:
            if not (resource == "uniprot" or resource == "total"):
                rowdict = {" ".join((str(count), label)):value for (count, label), value in rsc.stats[resource]["ratios"].iteritems()}
                rowdict["resource"] = resource
                writer.writerow(rowdict)
                if latex:
                    rowlist = []
                    for field in fieldnames:
                        if field in rowdict:
                            rowlist.append(rowdict[field])
                        else:
                            rowlist.append("-")

                    lrows.append(rowlist)


        rowdict = {" ".join((str(count), label)):value for (count, label), value in rsc.stats["total"]["ratios"]["terms/id"].iteritems()}
        rowdict["resource"] = "total"
        writer.writerow(rowdict)
        lrows.append([r"\textbf{" + str(rowdict[field])+"}" for field in fieldnames])

        if latex:
            write2latex(lrows, stats_latex)

    with open(STATSPATH + "stats_ratios_ids_per_term.txt", "w") as statsfile:
        fieldnames = [" ".join((str(count), label)) for count, label
                      in sorted(rsc.stats["total"]["ratios"]["ids/term"].keys(),
                                key = lambda x: x[0])]

        fieldnames.insert(0, "resource")

        writer = UnicodeDictWriter(statsfile, dialect= csv.excel_tab, quotechar=str("\""), fieldnames = fieldnames,
                            quoting= csv.QUOTE_NONE, restval='__')

        lrows = [fieldnames]

        writer.writeheader()

        rowdicts = OrderedDict([("uniprot",rsc.stats["uniprot"]["ratios"]), ("total", rsc.stats["total"]["ratios"]["ids/term"])])
        for resource in rowdicts:
            rowdict = {" ".join((str(count), label)):value for (count, label), value in rowdicts[resource].iteritems()}
            rowdict["resource"] = resource
            writer.writerow(rowdict)
            if latex:
                if resource != "total":
                    lrows.append([rowdict[field] for field in fieldnames])
                else:
                    lrows.append([r"\textbf{" + str(rowdict[field])+"}" for field in fieldnames])

        if latex:
            write2latex(lrows, stats_latex)

def write2latex(table, filename):
    tabulate.LATEX_ESCAPE_RULES = {}
    with open(STATSPATH + filename+"_latex_snippet.txt", "a") as texfile:
        texfile.write(tabulate.tabulate(table, tablefmt = "latex"))
        texfile.write("\n\n")