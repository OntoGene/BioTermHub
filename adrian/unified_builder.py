import codecs
import csv
import sys
sys.path.insert(0, '../lib')

import biogrid_parser
import uniprot_cellosaurus_parser
import entrezgene_n2o3_wrapper
import mesh_wrapper
from unicode_csv import UnicodeDictWriter

class RecordSetContainer(object):
    def __init__(self, 
                 #biogrid_idents, 
                 uniprot_records,
                 cellosaurus_records, 
                 entrezgene_records,
                 mesh_records):
        
        self.uniprot_records = None
        self.cellosaurus = None
        self.entrezgene_records = None
        self.mesh_records = None
        #self.biogrid_idents = None
        self.rsc_list = []
        
        if uniprot_records:
            self.uniprot_records = \
                uniprot_cellosaurus_parser.RecordSet(uniprot_records, uniprot_cellosaurus_parser.UniProtRecTypes)
            self.uniprot_records_rowlist = \
                self.uniprot_records.rowdicts
            self.rsc_list.append(self.uniprot_records_rowlist)
            
        if cellosaurus_records:
            self.cellosaurus_records = \
                uniprot_cellosaurus_parser.RecordSet(cellosaurus_records, uniprot_cellosaurus_parser.CellosaurusRecTypes)
            self.cellosaurus_records_rowlist = \
                self.cellosaurus_records.rowdicts
            self.rsc_list.append(self.cellosaurus_records_rowlist)
                
        if entrezgene_records:
            self.entrezgene_records = \
                entrezgene_n2o3_wrapper.RecordSet(entrezgene_records)
            self.entrezgene_records_rowlist = \
                self.entrezgene_records.rowdicts
            self.rsc_list.append(self.entrezgene_records_rowlist)
        
        if mesh_records:
            self.mesh_records = \
                mesh_wrapper.RecordSet(mesh_records[0], mesh_records[1])
            self.mesh_records_rowlist = \
                self.mesh_records.rowdicts
            self.rsc_list.append(self.mesh_records_rowlist)
        
        #~ if biogrid_idents:
            #~ self.biogrid_idents = \
                #~ biogrid_parser.IdentifierSet(biogrid_idents)
            #~ self.biogrid_idents_rowlist = \
                #~ self.biogrid_idents.get_rowlist()
            #~ self.rsc_list.append(self.biogrid_idents_rowlist)

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
