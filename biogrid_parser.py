from csv import DictReader, QUOTE_NONE, Dialect

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


RECORD_COLUMN_CONSTANTS = [ 
             ENTREZ_GENE_INTERACTOR_A,
             ENTREZ_GENE_INTERACTOR_B,
             BIOGRID_ID_INTERACTOR_A,
             BIOGRID_ID_INTERACTOR_B,
             SYNONYMS_INTERACTOR_A,
             SYNONYMS_INTERACTOR_B
            ]

IDENTIFIER_COLUMN_CONSTANTS = [
       IDENTIFIER_VALUE,
       IDENTIFIER_TYPE,
       ORGANISM_OFFICIAL_NAME,
      ]

class BioGRID_Dialect(Dialect):
    delimiter = "\t"
    doublequote = False
    quoting = QUOTE_NONE
    lineterminator = "\r\n"

class RecordSet(object):
    def __init__(self, infile):
        self.csv_file = infile_csv = open(infile, "r")
        self.csv_object = DictReader(infile_csv, dialect=BioGRID_Dialect
                )
    def build_dict(self):
        self.term_dict = {}
        for row in self.csv_object:
            record = {}
            self.term_dict[row[BIOGRID_INTERACTION_ID]] = record
            for column in RECORD_COLUMN_CONSTANTS:
                if not column in [SYNONYMS_INTERACTOR_A, SYNONYMS_INTERACTOR_B]:
                    record[column] = row[column]
                else:
                    if not row[column] == "-":
                        record[column] = row[column].split("|")
                    else:
                        record[column] = []
        print self.term_dict.items()[:10]

class IdentifierSet(object):
    def __init__(self, infile):
        self.csv_file = infile_csv = open(infile, "r")
        header = [BIOGRID_ID] + IDENTIFIER_COLUMN_CONSTANTS
        filerow = None
        while filerow != "\t".join(header) + "\n":
            filerow = self.csv_file.next()
        self.csv_object = DictReader(infile_csv, dialect=BioGRID_Dialect, fieldnames=header)

    def build_dict(self):
        self.identifier_dict = {}
        previous_id = None
        for row in self.csv_object:
            if not previous_id == row[BIOGRID_ID]:
                record = {}
                self.identifier_dict[row[BIOGRID_ID]] = record
            record[ORGANISM_OFFICIAL_NAME] = row[ORGANISM_OFFICIAL_NAME]
            record[row[IDENTIFIER_TYPE]] = row[IDENTIFIER_VALUE]
            previous_id = row[BIOGRID_ID]
        print self.identifier_dict.items()[:10]
