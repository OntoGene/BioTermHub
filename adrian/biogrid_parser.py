from csv import DictReader, QUOTE_NONE, Dialect
from oid_generator import OID
from copy import deepcopy

##
## Label constants and constants lists
##

# Records
BIOGRID_INTERACTION_ID = "#BioGRID Interaction ID"
ENTREZ_GENE_INTERACTOR_A = "Entrez Gene Interactor A"
ENTREZ_GENE_INTERACTOR_B = "Entrez Gene Interactor B"
BIOGRID_ID_INTERACTOR_A = "BioGRID ID Interactor A"
BIOGRID_ID_INTERACTOR_B = "BioGRID ID Interactor B"
SYNONYMS_INTERACTOR_A = "Synonyms Interactor A"
SYNONYMS_INTERACTOR_B = "Synonyms Interactor B"

BIOGRID_ID = "BIOGRID_ID"
IDENTIFIER_VALUE = "IDENTIFIER_VALUE"
IDENTIFIER_TYPE = "IDENTIFIER_TYPE"
ORGANISM_OFFICIAL_NAME = "ORGANISM_OFFICIAL_NAME"

RECORD_COLUMNS = [ 
             ENTREZ_GENE_INTERACTOR_A,
             ENTREZ_GENE_INTERACTOR_B,
             BIOGRID_ID_INTERACTOR_A,
             BIOGRID_ID_INTERACTOR_B,
             SYNONYMS_INTERACTOR_A,
             SYNONYMS_INTERACTOR_B
            ]
SYNONYM_COLUMNS = [SYNONYMS_INTERACTOR_A, SYNONYMS_INTERACTOR_B]

# Identifiers
IDENTIFIER_COLUMNS = [
       IDENTIFIER_VALUE,
       IDENTIFIER_TYPE,
       ORGANISM_OFFICIAL_NAME,
      ]

class BioGRID_Dialect(Dialect):
    delimiter = "\t"
    doublequote = False
    quoting = QUOTE_NONE
    lineterminator = "\r\n"

class RecordSet(dict):
    """
    Parses BioGRID records into dictionaries and stores these dictionaries
    in a dictionary with the key-value structure record[BIOGRID_INTERACTION_ID]:record.
    """
    def __init__(self, infile):
        dict.__init__(self)
        self.csv_file = infile_csv = open(infile, "r")
        self.csv_object = DictReader(infile_csv, dialect=BioGRID_Dialect)
        self.build_dict()
        
    def build_dict(self):
        for row in self.csv_object:
            record = {}
            numerical_id = int(row[BIOGRID_INTERACTION_ID])
            self[numerical_id] = record
            record["OID"] = OID.get()
            for column in RECORD_COLUMNS:
                if column in SYNONYM_COLUMNS:
                    # Split synonyms into lists. Insert empty lists for empty fields
                    # (Placeholder `-`)
                    if not row[column] == "-":
                        record[column] = row[column].split("|")
                    else:
                        record[column] = []

                elif row[column].isdigit():
                    record[column] = int(row[column])
    
                else:
                    record[column] = row[column]
            
        
class IdentifierSet(dict):
    """
    Parses BioGRID identifier records into dictionaries and stores these dictionaries
    in a dictionary with the key-value structure record[BIOGRID_ID]:record.
    
    A record is considered any number of rows sharing a BIOGRID_ID and collapsed into a single
    dictionary.
    
    Note: Because of this reduction, the column heads "IDENTIFIER_TYPE" and "IDENTIFIER_VALUE"
    are not used as keys.  The key-value-pair for those columns has the following format format :
    <actual_identifier_type>:<actual_identifier_value>
    """
    def __init__(self, infile):
        dict.__init__(self)
        self.csv_file = infile_csv = open(infile, "r")
        header = [BIOGRID_ID] + IDENTIFIER_COLUMNS
        
        filerow = None
        # Forward file generator to TSV header row
        while filerow != "\t".join(header) + "\n":
            filerow = self.csv_file.next()
        self.csv_object = DictReader(infile_csv, dialect=BioGRID_Dialect, fieldnames=header)
        self.build_dict()

    def build_dict(self):
        identifier_dict = {}
        previous_id = None
        for row in self.csv_object:
            numerical_id = int(row[BIOGRID_ID])
            
            if not previous_id == numerical_id:
                record = {}
                record["OID"] = OID.get()
                self[numerical_id] = record
                
            record[ORGANISM_OFFICIAL_NAME] = row[ORGANISM_OFFICIAL_NAME]
            
            if row[IDENTIFIER_TYPE] == "SYNONYM" and row[IDENTIFIER_TYPE] in record:
                try:
                    # value is already a list
                    record[row[IDENTIFIER_TYPE]].append(row[IDENTIFIER_VALUE])
                except AttributeError:
                    # encapsulate first rowvalue_dict in list, append second rowvalue_dict
                    rowvalue_list = [record[row[IDENTIFIER_TYPE]]]
                    rowvalue_list.append(row[IDENTIFIER_VALUE])
                    record[row[IDENTIFIER_TYPE]] = rowvalue_list
                
            elif row[IDENTIFIER_VALUE].isdigit():
                record[row[IDENTIFIER_TYPE]] = int(row[IDENTIFIER_VALUE])
            else:
                record[row[IDENTIFIER_TYPE]] = row[IDENTIFIER_VALUE]
            previous_id = numerical_id
    
    def get_rowlist(self):
        row_list = []
        for k, v in self.iteritems():
            row_dict = {key:value for key,value in v.iteritems() if key not in ["OFFICIAL SYMBOL", "SYNONYM"]}
            common_row_dict = deepcopy(row_dict)
            row_dict["SYMBOL"] = v["OFFICIAL SYMBOL"]
            row_list.append(row_dict)
            if "SYNONYM" in v:
                for synonym in v["SYNONYM"]:
                    row_dict = deepcopy(common_row_dict)
                    row_dict["SYMBOL"] = synonym
                    row_list.append(row_dict)
        
        return row_list
