from pprint import pprint
from uniprot_keylist_parser import *
from csv import writer 
handle = open("uniprot_sprot.dat", "r")
getdeps()

with open("sec_ac.txt", "r") as sec_ac:
    sec_ac_dict = build_dictionary(sec_ac)
with open("acindex.txt", "r") as acindex:
    acindex_dict = build_dictionary(acindex)

#
# Build dictionary
#

records = parse(handle) # Uses the function 'parse' from the module. 
with open("uniprot_sprot.csv", "w") as csvfile: 
    csvwriter = writer(csvfile, delimiter = '\t')
    for record in records:
        csvwriter.writerow([record["AC"], record["ID"], record["OS"]])
        # print record["AC"], record["ID"], record["OS"]
        # pprint(record)
