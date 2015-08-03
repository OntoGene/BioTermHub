sys.path.insert(0, '../lib')
from unicode_csv import UnicodeCsvReader
from oid_generator import OID

class RecordSet(list):
    """
    Parses BioGRID records into dictionaries and stores these dictionaries
    in a dictionary with the key-value structure record[BIOGRID_INTERACTION_ID]:record.
    """
    def __init__(self, infile, rowdicts, mesh_ids = None):
        dict.__init__(self)
        self.infile_name = infile.split('.')[0]
        self.entity_type = self.infile_name.split('_')[1]
        self.csv_file = infile_csv = open(infile, "r")
        self._set_col_labels()
        self.mesh_ids = mesh_ids

        fields = [self.name_col, self.id_col, 'Synonyms']

        self.csv_object = UnicodeCsvReader(infile_csv)

        if not rowdicts:
            self.build_dict()

        @property
        def rowdicts(self):
            for line in self.csv_object:
                if mesh_ids and line[0] in mesh_ids:
                    continue
                term_and_synonyms = [line[0]].extend(line[7].split('|'))
                for term in term_and_synonyms:
                    row_dict = {'oid': OID.get()
                                'resource':'CTD',
                                'term':term,
                                'preferred_term':line[0],
                                'original_id': line[1],
                                'entity_type':self.entity_type}
                    yield row_dict