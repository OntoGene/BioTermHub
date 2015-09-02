import sys
sys.path.insert(0, '../lib')
from unicode_csv import UnicodeCsvReader
from oid_generator import OID
from tools import StatDict, CrossLookupTuple

class RecordSet(object):
    """
    Parses BioGRID records into dictionaries and stores these dictionaries
    in a dictionary with the key-value structure record[BIOGRID_INTERACTION_ID]:record.
    """
    def __init__(self, infile, mesh_ids = None):
        self.infile_name = infile.split('.')[0]
        self.entity_type = self.infile_name.split('_')[1]
        self.csv_file = infile_csv = open(infile, "r")
        self.mesh_ids = mesh_ids
        self.ambig_unit = "terms"
        self.stats = StatDict()
        self.mesh_ids = mesh_ids
        self.csv_object = UnicodeCsvReader(infile_csv)

    @property
    def rowdicts(self):
        for line in self.csv_object:
            if line[0][0] == "#":
                continue
            term_and_synonyms = [line[0]]
            term_and_synonyms.extend(line[7].split('|'))
            for term in term_and_synonyms:
                if term:
                    ns, id = line[1].split(':')
                    if self.mesh_ids:
                        clookup_tuple = CrossLookupTuple(id=id, term=term)
                        if clookup_tuple in self.mesh_ids:
                            continue
                    resource = 'CTD (%s)' % ns
                    row_dict = {'oid': OID.get(),
                                'resource':resource,
                                'term':term,
                                'preferred_term':line[0],
                                'original_id': id, # Strip namespace
                                'entity_type':self.entity_type}
                    yield row_dict