import biogrid_parser
import uniprot_parser
import entrezgene_n2o3_wrapper

class RecordSetContainer(object):
    def __init__(self, 
                 biogrid_idents, 
                 biogrid_records,
                 uniprot_records, 
                 entrezgene_records):
                     
        self.biogrid_idents = \
            biogrid_parser.IdentifierSet(biogrid_idents)
        self.biogrid_records = \
            biogrid_parser.RecordSet(biogrid_records)
        self.uniprot_records = \
            uniprot_parser.RecordSet(uniprot_records)
        self.entrezgene_records = \
            entrezgene_n2o3_wrapper.RecordSet(entrezgene_records)

class UnifiedBuilder(object):
    def __init__(self, rsc):
        pass
