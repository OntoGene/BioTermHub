from tools import DefaultOrderedDict, StatDict, UnmetDependenciesError

try:
    from chebi_obo2ontogene import parse_obo, obodict2ontogene_headers
except ImportError:
    print "Warning: Python dependencies for CHEBI OBO parser not met: Orange and/or Orange-Informatics"
    raise UnmetDependenciesError

class RecordSet(object):
    """
    Uses the external parser ncbi2ontogene3 to process NCBI/EntrezGene records 
    into dictionaries and stores these dictionaries in a dictionary with the key-value structure 
    record[ncbi_id]:record.
    """
    
    def __init__(self, infile, ontogene = True):
        self.infile = infile
        self.ontogene = ontogene
        self.stats = StatDict()
        self.ambig_unit = "terms"

    @property
    def rowdicts(self):

        # Using chebi_obo2ontogene to retrieve a dictionary for each row
        processed_input = parse_obo(self.infile)

        terms_per_id = 0
        for row, new_id in obodict2ontogene_headers(processed_input):
            self.stats["terms"] += 1
            terms_per_id += 1

            if new_id:
                self.stats["ids"] += 1
                statkey = (terms_per_id, "terms/id")
                self.stats["ratios"][statkey] += 1
                terms_per_id = 1

            yield row

def mapping(rectype, ontogene):
        if ontogene and rectype in RecType.og_mapping:
            return RecType.og_mapping[rectype]
        else:
            return rectype