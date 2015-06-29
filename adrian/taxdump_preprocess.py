def preprocess(names_in, nodes, names_out):
    with open(names_in, "r") as names, \
         open(nodes, "r") as nodes, \
         open(names_out, "w") as outfile:
         header_fields = ["tax_id", "parent tax_id", "rank", "embl code", "division id", "inherited div flag",
                   "genetic code id", "inherited GC  flag", "mitochondrial genetic code ", "inherited MGC flag",
                   "GenBank hidden flag", "hidden subtree root flag", "comments"]
         nodes_reader = (dict(zip(header_fields, line.rstrip("\t|\n").split("\t|\t"))) for line in nodes)
         nodes_id = None
         for names_row in names:
             names_id = names_row.split("\t")[0]
             while nodes_id != names_id:
                 node_row = nodes_reader.next()
                 nodes_id = node_row["tax_id"]
             if node_row["rank"] == "species":
                 outfile.write(names_row)

if __name__ == "__main__":
    preprocess("data/names.dmp", "data/nodes.dmp", "data/names.trunc.dmp")