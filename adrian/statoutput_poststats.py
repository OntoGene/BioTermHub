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

    stats_dict = {}
    stats_dict["Resource"] = {}
    stats_dict["Entity type"] = {}

    stats_dict["Resource"]["terms"] = {stats:fdist.term_freq_dist() for stats, fdist in stats.resource_dict.iteritems()}
    stats_dict["Resource"]["terms_lw"] = {stats:fdist.term_lw_freq_dist() for stats, fdist in stats.resource_dict.iteritems()}
    stats_dict["Resource"]["terms_lw_nows"] = {stats:fdist.term_lw_nows_freq_dist() for stats, fdist in stats.resource_dict.iteritems()}

    stats_dict["Entity type"]["terms"] = {stats:fdist.term_freq_dist() for stats, fdist in stats.entity_type_dict.iteritems()}
    stats_dict["Entity type"]["terms_lw"] = {stats:fdist.term_lw_freq_dist() for stats, fdist in stats.entity_type_dict.iteritems()}
    stats_dict["Entity type"]["terms_lw_nows"] = {stats:fdist.term_lw_nows_freq_dist() for stats, fdist in stats.entity_type_dict.iteritems()}

    stats_dict["Resource"]["ids"] = {stats:fdist.id_freq_dist() for stats, fdist in stats.resource_dict.iteritems()}
    stats_dict["Entity type"]["ids"] = {stats:fdist.id_freq_dist() for stats, fdist in stats.entity_type_dict.iteritems()}

    labels = ["terms",
                  "lowercase term ",
                  "lowercase terms stripped from non-alphanumerical chars"]

    overall_term_freqs = [stats.term_freq_dist(),
                      stats.term_lw_freq_dist(),
                      stats.term_lw_nows_freq_dist()]

    overall_id_freqs = stats.id_freq_dist()

    l_overall_term_freqs = dict(zip(labels, overall_term_freqs))

    total_term_freqs = Counter()
    for fdist in l_overall_term_freqs:
        total_term_freqs += l_overall_term_freqs[fdist]

    output_tsv("stats_overall_terms_per_id.txt", total_term_freqs, l_overall_term_freqs)
    output_tsv("stats_overall_id_freqs.txt", overall_id_freqs, overall_id_freqs)

    args = [["Resource", "terms"],
            ["Resource", "terms_lw"],
            ["Resource", "terms_lw_nows"],
            ["Resource", "ids"],
            ["Entity type", "terms"],
            ["Entity type", "terms_lw"],
            ["Entity type", "terms_lw_nows"],
            ["Entity type", "ids"],
           ]

    for (stat_class, stat_type) in args:
        total_term_freqs = Counter()
        for fdist in stats_dict[stat_class][stat_type]:
            total_term_freqs += stats_dict[stat_class][stat_type][fdist]

        output_tsv("stats_%s_%s_freqs.txt" % (stat_class, stat_type), total_term_freqs, stats_dict[stat_class][stat_type])

def output_tsv(filename, total_term_freqs, l_term_freqs, latex = False):
    with open(STATSPATH + filename, "w") as statsfile:
        fieldnames_numeric = sorted(total_term_freqs.keys())
        fieldnames = [str(key) for key in fieldnames_numeric]

        fieldnames.insert(0, "distribution")

        print fieldnames

        writer = UnicodeDictWriter(statsfile, dialect= csv.excel_tab, quotechar=str("\""), fieldnames = fieldnames,
                            quoting= csv.QUOTE_NONE, restval='')

        writer.writeheader()

        lrows = [fieldnames]
        try:
            for stat in l_term_freqs:
                rowdict = {str(k):v for k, v in l_term_freqs[stat].iteritems()}
                rowdict["distribution"] = stat

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
    with open(STATSPATH + filename+"_latex_snippet.txt", "a") as texfile:
        texfile.write(tabulate.tabulate(table, tablefmt = "latex"))
        texfile.write("\n\n")

if __name__ == "__main__":
    write2tsv("output.csv", True)