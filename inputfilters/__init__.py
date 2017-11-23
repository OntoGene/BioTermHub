#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Input filter classes.

How to add a new filter:
- Write a new module in this directory, following the
  example of the existing filters.
- Import the module here.
- Add an entry to the `FILTERS` module constant.

The new filter will then be accessible to core.aggregate
as well as www.index, thus it will be listed in the web GUI.
'''


from termhub.inputfilters import cl, cellosaurus, chebi, ctd, entrezgene
from termhub.inputfilters import go, mesh, ncbitax, pro, so, uberon, uniprot


FILTERS = {
    'cellosaurus': cellosaurus.RecordSet,
    'cl': cl.RecordSet,
    'chebi': chebi.RecordSet,
    'ctd_chem': ctd.ChemRecordSet,
    'ctd_disease': ctd.DiseaseRecordSet,
    'entrezgene': entrezgene.RecordSet,
    'go': go.RecordSet,
    'mesh': mesh.RecordSet,
    'ncbitax': ncbitax.RecordSet,
    'pro': pro.RecordSet,
    'so': so.RecordSet,
    'uberon': uberon.RecordSet,
    'uniprot': uniprot.RecordSet,
}
