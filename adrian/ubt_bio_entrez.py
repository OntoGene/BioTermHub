from unified_builder import RecordSetContainer, UnifiedBuilder

test_rsc = RecordSetContainer("data/BIOGRID-IDENTIFIERS-3.4.125.tab.txt",
                              None,
                              None,
                              "data/gene_info"
                              )

#~ test_rsc = RecordSetContainer("1k_snippets/BIOGRID-IDENTIFIERS-1k",
                              #~ "1k_snippets/uniprot_sprot-1k",
                              #~ "1k_snippets/cellosaurus-1k",
                              #~ "1k_snippets/gene_info_10k"
                              #~ )

biogrid_idents = test_rsc.biogrid_idents
entrezgene_records = test_rsc.entrezgene_records

counter_ent = 0

missing_ENTREZ_GENE = []

for ident in biogrid_idents:
    if "ENTREZ_GENE" in biogrid_idents[ident]:
        counter_ent += 1
        if not biogrid_idents[ident]["ENTREZ_GENE"] in entrezgene_records:
            missing_ENTREZ_GENE.append(biogrid_idents[ident]["ENTREZ_GENE"])

print "Entrez gene"
print len(missing_ENTREZ_GENE)
print counter_ent

import codecs

missing_e = codecs.open("missing_e", "w", "utf-8")

missing_e.writelines(missing_ENTREZ_GENE)

missing_e.close()

print "\nBIOGRID IDs"
print biogrid_idents.items()[0:5]

print "\nEntrezgene Records"
print entrezgene_records.items()[0:5]
