from __future__ import division
import codecs
import csv
import sys
from collections import defaultdict, OrderedDict, Counter
sys.path.insert(0, '../lib')

import biogrid_parser
import uniprot_cellosaurus_parser
import entrezgene_n2o3_wrapper
import mesh_wrapper
import taxdump_parser
import bdict
import pickle

from unicode_csv import UnicodeDictWriter

class RecordSetContainer(object):
    def __init__(self, **kwargs):
        
        # Convert kwargs to defaultdict to create calls dictionary and to sorted OrderedDict
        # to maintain order of resources in output
        self.dkwargs = defaultdict(bool, kwargs)
        self.okwargs = OrderedDict(sorted(kwargs.items(), key= lambda x: x[0]))
        self.calls = {"uniprot": {"module":uniprot_cellosaurus_parser, "arguments":(self.dkwargs["uniprot"], uniprot_cellosaurus_parser.UniProtRecTypes)},
                      "cellosaurus":{"module":uniprot_cellosaurus_parser, "arguments":(self.dkwargs["cellosaurus"], uniprot_cellosaurus_parser.CellosaurusRecTypes)},
                      "entrezgene":{"module":entrezgene_n2o3_wrapper, "arguments":(self.dkwargs["entrezgene"], )},
                      "mesh":{"module":mesh_wrapper, "arguments":self.dkwargs["mesh"]},
                      "taxdump":{"module":taxdump_parser, "arguments":(self.dkwargs["taxdump"], )}
                      }
        self.stats = {}
        self.bidict_originalid_oid = bdict.bidict()
        self.bidict_originalid_term = bdict.defaultbidict(set)
    
    def recordsets(self):
        for resource, infile in self.okwargs.iteritems():
            recordset = self.calls[resource]["module"].RecordSet(*self.calls[resource]["arguments"])
            self.stats[resource] = recordset.stats
            yield recordset.rowdicts
            
    def calcstats(self):
        total = Counter({"terms":0, "ids":0})
        for recordset, stats in self.stats.iteritems():
            try:
                self.stats[recordset]['term_per_id'] = stats["terms"]/stats["ids"]
            except ZeroDivisionError:
                self.stats[recordset]['term_per_id'] = 0
            total["terms"] += stats["terms"]
            total["ids"] += stats["ids"]
            total['term_per_id'] += self.stats[recordset]['term_per_id']
        try:
            total['term_per_id'] /= len(self.stats.keys())
        except ZeroDivisionError:
            total['term_per_id'] = 0
        self.stats["total"] = total
        
    def pickle_bidicts(outfile):
        
        
class UnifiedBuilder(dict):
    def __init__(self, rsc, filename, write_hash = False):
        dict.__init__(self)
        csv_file = codecs.open(filename, 'wt', 'utf-8')
        fieldnames = ["oid", "resource", "original_id", "term", "preferred_term", "entity_type"]
        writer = UnicodeDictWriter(csv_file, dialect= csv.excel_tab, fieldnames=fieldnames, quotechar=str("\""), quoting= csv.QUOTE_NONE, restval='__')
        writer.writeheader()

        for rsc_rowlist in rsc.recordsets():
            for row in rsc_rowlist:
                writer.writerow(row)
                if write_hash:
                    rsc.bidict_originalid_oid[row["original_id"]] = row["oid"]
                    rsc.bidict_originalid_term.add(row["original_id"], row["term"])
            del rsc_rowlist
