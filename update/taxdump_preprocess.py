#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015
# Modified: Lenz Furrer, 2016


'''
Join the relevant Taxonomy names and nodes into a single file.
'''


def main():
    '''
    Run as script.
    '''
    preprocess("data/names.dmp", "data/nodes.dmp", "data/names.trunc.dmp")




def preprocess(names_in, nodes_in, names_out):
    '''
    Join names and nodes.
    '''
    with open(names_in, "r") as names, \
         open(nodes_in, "r") as nodes, \
         open(names_out, "w") as outfile:
        header_fields = [
            "tax_id", "parent tax_id", "rank", "embl code", "division id",
            "inherited div flag", "genetic code id", "inherited GC  flag",
            "mitochondrial genetic code ", "inherited MGC flag",
            "GenBank hidden flag", "hidden subtree root flag", "comments"]
        nodes_reader = (dict(zip(header_fields,
                                 line.rstrip("\t|\n").split("\t|\t")))
                        for line in nodes)
        nodes_id = None
        for names_row in names:
            names_id = names_row.split("\t", 1)[0]
            while nodes_id != names_id:
                # Skip nodes until it matches names.
                node_row = next(nodes_reader)
                nodes_id = node_row["tax_id"]
            if node_row["rank"] == "species":
                outfile.write(names_row)


if __name__ == '__main__':
    main()
