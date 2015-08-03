from __future__ import division
import os
import codecs
import csv
import sys
import cPickle
from collections import defaultdict, OrderedDict, Counter
from tools import StatDict

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, '..', 'lib'))

# custom modules
import bdict
from unicode_csv import UnicodeDictWriter
from tools import UnmetDependenciesError

# parsers and parser wrappers
import uniprot_cellosaurus_parser
import taxdump_parser
import entrezgene_n2o3_wrapper
import mesh_wrapper
import ctd_parser

try:
    import chebi_o2o_wrapper
except UnmetDependenciesError:
    chebi_o2o_wrapper = None

# Resource : Resource type, counterpart

CROSS_LOOKUP_PAIRS = {'mesh': ('reference', ('ctd_chem', 'ctd_disease')),
                      'ctd_chem': ('origin', 'ctd_lookup', 'mesh'),
                      'ctd_disease': ('origin', 'ctd_lookup', 'mesh')}

class RecordSetContainer(object):
    def __init__(self, **kwargs):

        # Convert kwargs to defaultdict to create calls dictionary and to sorted OrderedDict
        # to maintain order of resources in output
        self.dkwargs = defaultdict(bool, kwargs)
        self.okwargs = OrderedDict(sorted(kwargs.items(), key= self._sort_kwargs))
        self.calls = {"uniprot":
                          {"module":uniprot_cellosaurus_parser,
                           "arguments":(self.dkwargs["uniprot"],
                                        uniprot_cellosaurus_parser.UniProtRecTypes)},
                      "cellosaurus":
                          {"module":uniprot_cellosaurus_parser,
                           "arguments":(self.dkwargs["cellosaurus"],
                                        uniprot_cellosaurus_parser.CellosaurusRecTypes)},
                      "entrezgene":
                          {"module":entrezgene_n2o3_wrapper,
                           "arguments":(self.dkwargs["entrezgene"],)},
                      "mesh":
                          {"module":mesh_wrapper,
                           "arguments":self.dkwargs["mesh"]},
                      "taxdump":
                          {"module":taxdump_parser,
                           "arguments":(self.dkwargs["taxdump"],)},
                      "chebi":
                          {"module":chebi_o2o_wrapper,
                           "arguments":(self.dkwargs["chebi"],)},
                      "ctd_chem":
                          {"module":ctd_parser,
                           "arguments":(self.dkwargs["ctd_chem"],
                                        self.dkwargs["ctd_lookup"])},
                      "ctd_disease":{"module":ctd_parser,
                                     "arguments":(self.dkwargs["ctd_disease"],
                                                  self.dkwargs["ctd_lookup"])}
                      }

        self.stats = OrderedDict()
        self.ambig_units = {}
        self.cross_lookup = defaultdict(set)

        if not self.pickles_exist:
            self.bidict_originalid_oid = bdict.bidict()
            self.bidict_originalid_term = bdict.bidict()
        #self.bidict_originalid_term = bdict.defaultbidict(set)

    def _sort_kwargs(self, element):
        origins = [resource for resource in CROSS_LOOKUP_PAIRS if CROSS_LOOKUP_PAIRS[resource][0] == 'origin']
        sorttuple = (element[0] in origins, element[0])
        return sorttuple

    # Check if a resource should be prepared for cross-lookup
    def check_cross_lookup(self, resource):
        if resource in CROSS_LOOKUP_PAIRS and CROSS_LOOKUP_PAIRS[resource][0] == 'reference':
            # Check if any associated origins are 1) present and 2) have their cross-lookup flag set
            for origin in CROSS_LOOKUP_PAIRS[resource][1]:
                origin_arg = CROSS_LOOKUP_PAIRS[origin][1]
                if origin in self.dkwargs and self.dkwargs[origin_arg]:
                    return True
        return False

    @property
    def pickles_exist(self):
        return os.path.exists('data/originalid_oid.pkl') and os.path.exists('data/originalid_term.pkl')

    def recordsets(self):
        for resource, infile in self.okwargs.iteritems():
            if resource in self.calls:
                if self.calls[resource]["module"]:

                    # Check if a cross-lookup has to be performed for the resource and if so, pass corresponding lookup set
                    if resource in CROSS_LOOKUP_PAIRS \
                        and CROSS_LOOKUP_PAIRS[resource][0] == 'origin' \
                        and self.calls[resource]["arguments"][1]:
                        recordset = self.calls[resource]["module"].RecordSet(self.calls[resource]["arguments"][0],
                                                                             self.cross_lookup[CROSS_LOOKUP_PAIRS[resource][2]])
                    else:
                        recordset = self.calls[resource]["module"].RecordSet(*self.calls[resource]["arguments"])
                    self.stats[resource] = recordset.stats
                    self.ambig_units[resource] = recordset.ambig_unit
                    yield recordset.rowdicts, resource
                else:
                    print "Warning: Skipping %s due to unmet dependencies ..." % resource
            else:
                continue

    def calcstats(self):
        total = StatDict()
        total["ratios"]["terms/id"] = Counter()
        total["ratios"]["ids/term"] = Counter()
        for recordset, stats in self.stats.iteritems():

            if self.ambig_units[recordset] == "terms":
                try:
                    self.stats[recordset]['avg. terms/id'] = stats["terms"]/stats["ids"]
                except ZeroDivisionError:
                    self.stats[recordset]['avg. terms/id'] = 0

                total["ratios"]["terms/id"] += self.stats[recordset]["ratios"]

            elif self.ambig_units[recordset] == "ids":
                try:
                    self.stats[recordset]['avg. ids/term'] = stats["ids"]/stats["terms"]
                except ZeroDivisionError:
                    self.stats[recordset]['avg. ids/term'] = 0

                total["ratios"]["ids/term"] += self.stats[recordset]["ratios"]

            total["terms"] += stats["terms"]
            total["ids"] += stats["ids"]
            total["avg. terms/id"] += self.stats[recordset]["avg. terms/id"]
            total["avg. ids/term"] += self.stats[recordset]["avg. ids/term"]

        try:
            total["avg. terms/id"] /= len(self.stats.keys())
        except ZeroDivisionError:
            total["avg. terms/id"] = 0
        try:
            total["avg. ids/term"] /= len(self.stats.keys())
        except ZeroDivisionError:
            total["avg. ids/term"] = 0

        self.stats["total"] = total

        
class UnifiedBuilder(dict):
    def __init__(self, rsc, filename, compile_hash = False, pickle_hash = False):
        dict.__init__(self)
        csv_file = codecs.open(filename, 'wt', 'utf-8')
        fieldnames = ["oid", "resource", "original_id", "term", "preferred_term", "entity_type"]
        writer = UnicodeDictWriter(csv_file, dialect= csv.excel_tab, fieldnames=fieldnames, quotechar=str("\""), quoting= csv.QUOTE_NONE, restval='__')
        writer.writeheader()
       
        if pickle_hash:
           self.unpickle_bidicts(rsc)

        for rsc_rowlist, resource in rsc.recordsets():
            clookup = rsc.check_cross_lookup(resource)
            for row in rsc_rowlist:
                # Cross-lookup handling
                if clookup:
                    # If reference, add id to set
                    rsc.cross_lookup[resource].add(row['original_id'])
                writer.writerow(row)
                if compile_hash:
                    # One oid may have multiple original_ids (e.g. uniprot), one original_id has always one oid
                    rsc.bidict_originalid_oid[row["oid"]] = row["original_id"]
                    # One original_id may have multiple terms (most resources), one term may have multiple original_ids (uniprot only)
                    if resource == "uniprot":
                        rsc.bidict_originalid_term[row["original_id"]] = row["term"]
                    else:
                        rsc.bidict_originalid_term[row["term"]] = row["original_id"]

        if pickle_hash:
            self.pickle_bidicts(rsc)

    def pickle_bidicts(self, rsc):
        with open('data/originalid_oid.pkl', 'wb') as o_originalid_oid:
            cPickle.dump(rsc.bidict_originalid_oid, o_originalid_oid, -1)

        with open('data/originalid_term.pkl', 'wb') as o_originalid_term:
            cPickle.dump(rsc.bidict_originalid_term, o_originalid_term, -1)

    def unpickle_bidicts(self, rsc):
        if rsc.pickles_exist:
            with open('data/originalid_oid.pkl', 'rb') as o_originalid_oid:
                rsc.bidict_originalid_oid = cPickle.load(o_originalid_oid)

            with open('data/originalid_term.pkl', 'rb') as o_originalid_term:
                rsc.bidict_originalid_term = cPickle.load(o_originalid_term)