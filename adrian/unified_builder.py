import codecs
import csv
import sys
sys.path.insert(0, '../lib')

import biogrid_parser
import uniprot_parser
import entrezgene_n2o3_wrapper
from unicode_csv import UnicodeDictWriter

class RecordSetContainer(object):
    def __init__(self, 
                 biogrid_idents, 
                 uniprot_records, 
                 entrezgene_records):
        
        self.uniprot_records = None
        self.entrezgene_records = None
        self.biogrid_idents = None
        self.rsc_list = []
        
        if uniprot_records:
            self.uniprot_records = \
                uniprot_parser.RecordSet(uniprot_records)
            self.uniprot_records_rowlist = \
                self.uniprot_records.get_rowlist()
            self.rsc_list.append(self.uniprot_records_rowlist)
                
        if entrezgene_records:
            self.entrezgene_records = \
                entrezgene_n2o3_wrapper.RecordSet(entrezgene_records)
            self.entrezgene_records_rowlist = \
                self.entrezgene_records.get_rowlist()
            self.rsc_list.append(self.entrezgene_records_rowlist)
        
        # Feed uniprot and entrezgene into the biogrid parser
        if biogrid_idents:
            self.biogrid_idents = \
                biogrid_parser.IdentifierSet(biogrid_idents)
            self.biogrid_idents_rowlist = \
                self.biogrid_idents.get_rowlist()
            self.rsc_list.append(self.biogrid_idents_rowlist)

class UnifiedBuilder(dict):
    def __init__(self, rsc, filename):
        dict.__init__(self)
        csv_file = codecs.open(filename, 'wt', 'utf-8')
        fieldnames = ["oid", "resource", "original_id", "term", "preferred_term", "entity_type"]
        writer = UnicodeDictWriter(csv_file, dialect= csv.excel_tab, fieldnames=fieldnames, quotechar=str("\""), quoting= csv.QUOTE_NONE, restval='__')
        writer.writeheader()

        for rsc_rowlist in rsc.rsc_list:
            if rsc_rowlist:
                for row in rsc_rowlist:
                    writer.writerow(row)
    
