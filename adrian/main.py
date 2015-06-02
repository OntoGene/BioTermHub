from pprint import pprint
from uniprot_parser import parse, build_indices
from csv import writer
from dependencies import *

def main():
    handle = open("uniprot_sprot.dat", "r")
    getdeps()
    sec_ac_dict, acindex_dict = build_indices()
    records = parse(handle) 
    
    with open("uniprot_sprot.csv", "w") as csvfile: 
        csvwriter = writer(csvfile, delimiter = '\t')
        for idx, record in enumerate(records):
            csvwriter.writerow([record["AC"], record["ID"], record["OS"]])
            if (idx + 1) % 50000 == 0:
                print "Records processed:", idx + 1

    handle.close()

if __name__ == "__main__":
    main()
