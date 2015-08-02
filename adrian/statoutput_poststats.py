import os
import os.path
import sys
HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, '..', 'lib'))
import csv
import tabulate
from unicode_csv import *
from collections import OrderedDict, Counter

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, '..', 'tilia'))
import statistics_termfile

STATSPATH = "stats/"

def write2tsv(filename, latex = False):
    stats_latex = "stats"

    if not os.path.exists(STATSPATH):
        os.mkdir("stats")

    stats = statistics_termfile.process_file(filename)

    stats_dict = OrderedDict()
    stats_dict["total"] = OrderedDict()
    stats_dict["total"]["ids"] = Counter()
    stats_dict["total"]["terms"] = Counter()
    stats_dict["total"]["terms_lw"] = Counter()
    stats_dict["total"]["terms_lw_nows"] = Counter()

    for stats, fdist in stats.entity_type_dict.iteritems():
        stats_dict[stats] = OrderedDict()
        stats_dict[stats]["ids"] = fdist.id_freq_dist()
        stats_dict[stats]["terms"] = fdist.term_freq_dist()
        stats_dict[stats]["terms_lw"] = fdist.term_lw_freq_dist()
        stats_dict[stats]["terms_lw_nows"] = fdist.term_lw_nows_freq_dist()
        stats_dict["total"]["ids"] += fdist.id_freq_dist()
        stats_dict["total"]["terms"] += fdist.term_freq_dist()
        stats_dict["total"]["terms_lw"] += fdist.term_lw_freq_dist()
        stats_dict["total"]["terms_lw_nows"] += fdist.term_lw_nows_freq_dist()


    labels = ["ids",
                "terms",
                  "lowercase term ",
                  "lowercase terms stripped from non-alphanumerical chars"]


    for entity_type in stats_dict:
        total_term_freqs = Counter()
        for fdist in stats_dict[entity_type]:
            total_term_freqs += stats_dict[entity_type][fdist]

        output_tsv("stats_%s_freqs.txt" % (entity_type), total_term_freqs, stats_dict[entity_type], labels)

def output_tsv(filename, total_term_freqs, term_freqs, labels):
    latex = False
    with open(STATSPATH + filename.replace("/", "_per_"), "w") as statsfile:
        fieldnames_numeric = sorted(total_term_freqs.keys())
        fieldnames = [str(key) for key in fieldnames_numeric]

        fieldnames.insert(0, "distribution")

        print fieldnames

        writer = UnicodeDictWriter(statsfile, dialect= csv.excel_tab, quotechar=str("\""), fieldnames = fieldnames,
                            quoting= csv.QUOTE_NONE, restval='')

        writer.writeheader()

        lrows = [fieldnames]
        try:
            for idx, stat in enumerate(term_freqs):
                rowdict = {str(k):v for k, v in term_freqs[stat].iteritems()}
                rowdict["distribution"] = labels[idx]

                writer.writerow(rowdict)
                if latex:
                    rowlist = []
                    for field in fieldnames:
                        if field in rowdict:
                            rowlist.append(rowdict[field])
                        else:
                            rowlist.append('')

                    lrows.append(rowlist)

        except AttributeError:
            rowdict = {str(k):v for k, v in l_term_freqs.iteritems()}
            writer.writerow(rowdict)

        if latex:
            write2latex(lrows, stats_latex)

def write2latex(table, filename):
    tabulate.LATEX_ESCAPE_RULES = {}
    with open(STATSPATH + filename.replace("/", "_per_") +"_latex_snippet.txt", "a") as texfile:
        texfile.write(tabulate.tabulate(table, tablefmt = "latex"))
        texfile.write("\n\n")

if __name__ == "__main__":
    write2tsv("output.csv", True)