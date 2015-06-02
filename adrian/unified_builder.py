import biogrid_parser
import uniprot_parser

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
            None # TODO()

class UnifiedBuilder(object):
    def __init__(self, rsc):
        pass
