import biodb_wrapper
from unified_builder import UnifiedBuilder

rsc = biodb_wrapper.ub_wrapper('cellosaurus', 'chebi', 'ctd_chem', 'ctd_disease', 'mesh', 'uniprot', 'entrezgene', 'taxdump', 'ctd_lookup')


mapping = mapdict = {'resource':
              {'mesh desc.*':'MESH Descriptor',
               'mesh supp.*':'MESH Supplementary',
		'CTD:*':'CTD'},
		'entity_type':{}}

ub = UnifiedBuilder(rsc, 'output_test.csv', mapping=mapping)
