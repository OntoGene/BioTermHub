#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016
#         Nico Colic, 2018


'''
Parse RxNorm Current Prescribable Content ("RXNCONSO.RFF").
'''

# https://www.nlm.nih.gov/research/umls/rxnorm/docs/2018/rxnorm_doco_full_2018-1.html#s12_4

# RXNSAT.RFF : 
# RXNREL.RFF : atoms and concept links, links RXAUI and RXCUI 


import io

from termhub.inputfilters._base import IterConceptRecordSet


class RecordSet(IterConceptRecordSet):
    '''
    Record collector for RxNorm RFF.
    '''

    resource = 'RX Norm'
    entity_type = 'chemical' # could be 'drug'?

    dump_fn = 'RXNCONSO.RFF'
    
    # test this in June again to see if this gives the new file, too?
    import os, ssl
    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
        getattr(ssl, '_create_unverified_context', None)): 
        ssl._create_default_https_context = ssl._create_unverified_context
    
    remote = 'http://download.nlm.nih.gov/rxnorm/RxNorm_full_prescribe_current.zip'
    source_ref = 'http://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html'

    @classmethod
    def _update_steps(cls):
        return ('zip', cls.preprocess)
    
    @staticmethod
    def preprocess(stream):
        '''
        Save some space by removing unused data right away.
        '''
        lines = io.TextIOWrapper(stream, encoding='utf-8')
        for line in lines:
            yield line.encode('utf-8')
