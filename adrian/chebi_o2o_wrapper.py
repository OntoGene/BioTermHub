from chebi_obo2ontogene import parse_obo, obodict2ontogene_headers
from collections import Counter

class RecordSet(object):
    """
    Uses the external parser ncbi2ontogene3 to process NCBI/EntrezGene records 
    into dictionaries and stores these dictionaries in a dictionary with the key-value structure 
    record[ncbi_id]:record.
    """
    
    def __init__(self, infile, ontogene = True):
        self.infile = infile
        self.ontogene = ontogene
        self.stats = Counter({"ids":0, "terms":0})

    @property
    def rowdicts(self):

        # Using chebi_obo2ontogene to retrieve a dictionary for each row
        processed_input = parse_obo(self.infile)

        for row in obodict2ontogene_headers(processed_input):
            self.stats["terms"] += 1
            # TODO: IDs
            yield row

def mapping(rectype, ontogene):
        if ontogene and rectype in RecType.og_mapping:
            return RecType.og_mapping[rectype]
        else:
            return rectype