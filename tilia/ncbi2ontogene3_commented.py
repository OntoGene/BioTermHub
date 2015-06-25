#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Python Program Description: Script for converting a database (ncbi gene_info) into the ontogene term matcher format. Original program: gene_families3.py

"""
from optparse import OptionParser
import codecs
import sys
import os
#from Bio import Entrez
import codecs
import shutil
import random
import nltk
import collections
import csv
import re
import cStringIO
import difflib


# Prevent Encoding exceptions in Python 2.x
sys.stdout = codecs.getwriter('utf-8')(sys.__stdout__)
sys.stderr = codecs.getwriter('utf-8')(sys.__stderr__)
sys.stdin = codecs.getreader('utf-8')(sys.__stdin__)


class UnicodeCsvReader(object):
    def __init__(self, f, encoding="utf-8", **kwargs):
        self.csv_reader = csv.reader(f, **kwargs)
        self.encoding = encoding

    def __iter__(self):
        return self

    def next(self):
        # read and split the csv row into fields
        row = self.csv_reader.next()
        # now decode
        return [unicode(cell, self.encoding) for cell in row]

    @property
    def line_num(self):
        return self.csv_reader.line_num

class UnicodeDictReader(csv.DictReader):
    def __init__(self, f, encoding="utf-8", fieldnames=None, **kwds):
        csv.DictReader.__init__(self, f, fieldnames=fieldnames, **kwds)
        self.reader = UnicodeCsvReader(f, encoding=encoding, **kwds)


class UnicodeDictWriter(object):

    def __init__(self, f, fieldnames, dialect=csv.excel_tab, quoting= csv.QUOTE_NONE, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.DictWriter(self.queue, fieldnames, quoting=quoting, escapechar = chr(92),dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, D):
        self.writer.writerow({unicode(k):unicode(v).encode("utf-8") for k,v in D.items()})
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data.decode("utf-8"))
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for D in rows:
            self.writerow(D)

    def writeheader(self):
        self.writer.writeheader()



def process_file(csv_file, options=None, args=None):
    '''Reads a csv file and produces a list of dictionaries with one dictionary per line using UnicodeDictReader; Headers are used as keys.'''


    reader = UnicodeDictReader(codecs.open(csv_file, 'r'), dialect=csv.excel_tab, quoting=csv.QUOTE_NONE,quotechar=str("\""))

    if options.id_list:
        #Only applies if a list of ids is given; only take ids in list into account 
        
        id_list_file = open(options.id_list, 'r')
        id_list = [unicode(id.rstrip('\n')) for id in id_list_file.readlines()]
        # print 'id_list'
#         print id_list


        file_list = []

        for row in reader:
            #print row
            gene_id = row['GeneID']
            #print gene_id
            if gene_id in id_list:
                file_list.append(row)

        #file_list = [row for row in reader if row['GeneID'] in id_list]

    else: 
        file_list = [row for row in reader]
        #Use all lines of the file
        
        #print sys.stderr,'#',file_list[0]

    return file_list


def dict_to_file(dict_list, output_file):
    ''' Takes a list of dictionaries and writes it to a csv file using UnicodeDictWriter'''

    csv_file = codecs.open(output_file, 'wt', 'utf-8')

    fieldnames = ['ncbi_id', 'term', 'type','reference']
    #the names of the headers

    print >> sys.stderr, '# STATUS: fieldnames', " ".join(fieldnames)


    try:

        writer = UnicodeDictWriter(csv_file, dialect= csv.excel_tab, fieldnames=fieldnames, quotechar=str("\""), quoting= csv.QUOTE_NONE, restval='__')

        headers = dict( (n,n) for n in fieldnames )


        #writer.writerow(headers)
        #(only use if headers are wanted)

        for one_dict in dict_list:
            writer.writerow(one_dict)

    finally: csv_file.close()



def transform_input(file_list):
    '''Processes the input in the following ways:
    - Splits lists of synonyms and converts them to different entries; 
    - Processes 'descriptions' fields and 'other designations' fields and finds longest common substrings (additional terms) for high recall (experimental, can be removed)'''

    new_file_list = []

    for line_dict in file_list:

        term_list = []

        synonyms = line_dict['Synonyms']
        description = line_dict['description']
        other_desig = line_dict['Other_designations']

    # Add Symbol itself to term list
        term_list.append(line_dict['Symbol'])

    # Split "Synonyms", "description" and "Other_designations" fields at '|' (and '/')
        if not synonyms == '-':
            synonym_list = synonyms.replace('/', '|').split('|')
            term_list.extend(synonym_list)
        if not description == '-':
            description_list = description.replace('/', '|').split('|')
            term_list.extend(description_list)
        if not other_desig =='-':
            other_desig_list = other_desig.replace('/', '|').split('|')
            term_list.extend(other_desig_list)

    # Find common substrings to terms in termlist

        term_list_copy = term_list[:]

        for i, term in enumerate(term_list_copy):
            common_substrings = []

            for x in range(1, len(term_list_copy)):
                try:
                    s1 = term
                    s2 = term_list_copy[i+x]

                    seq_matcher = difflib.SequenceMatcher(None, s1, s2)
                    seq_match = seq_matcher.find_longest_match(0, len(s1), 0, len(s2))

                    longest_substring = s1[seq_match[0] : seq_match[2]]
                    # find longest common substring between two strings

                    common_substrings.append(longest_substring)

                except IndexError: break

        try:
            frequent_substring = max(set(common_substrings), key=common_substrings.count)
            # most common longest common substring for all items
            if len(frequent_substring) >= 3:
                print 'Frequent Substring:', frequent_substring
                print 'COMMON SUBSTRINGS', common_substrings
                term_list.append(frequent_substring)
        except ValueError: pass

        for term in term_list_copy:
            if re.search("^[A-Za-z]+-?[0-9]+$", term):
                m = re.sub("-?[0-9]+$", '', term)
                #letters_match = term[:m.start()] + term[m.end():]
                print 'LETTERS MATCH', m
                term_list.append(m)


        print 'TERM LIST', term_list

        if not term_list == []:

            for term in list(set(term_list)):
                output_dict = {}

                output_dict['term'] = term
                output_dict['type'] = 'gene'
                output_dict['ncbi_id'] = line_dict['GeneID']
                output_dict['reference'] = line_dict['Symbol']

                print 'OUTPUT_DICT', output_dict

                new_file_list.append(output_dict)


    return new_file_list




def process(options=None, args=None):
    """
    Do the processing.

    Put together all functions.
    """
    #if options.debug:
    #    print >>sys.stderr, options

    #print sys.stdin, 'test'

    print 'OPTIONS:', options

    input_file = args[0]

    output_file = args[1]

    processed_input = process_file(input_file, options=options, args=args)
    # Read input file; (sort out input if list of ids is given); return dictionary structure

    #print processed_input, 'INPUT DICT'

    new_file_list = transform_input(processed_input)
    # Transform input into separate dictionaries for synonyms. 
    # The function transform_input will have to be adapted


    dict_to_file(new_file_list, output_file)
    # Write the new list of dictionaries to the output file

    print len(new_file_list), 'LINES WERE PRODUCED'



def main():
    """
    Invoke this module as a script
    """
    usage = "usage: %prog [options]; args[0]: original (database) file (NCBI); args[1]: output csv file ('Ontogene Format')"
    parser = OptionParser(version='%prog 0.99', usage=usage)

    parser.add_option('-l', '--logfile', dest='logfilename',
                      help='write log to FILE', metavar='FILE')
    parser.add_option('-q', '--quiet',
                      action='store_true', dest='quiet', default=False,
                      help='do not print status messages to stderr')
    parser.add_option('-d', '--debug',
                      action='store_true', dest='debug', default=False,
                      help='print debug information')

    parser.add_option('-i', '--id_list', action="store", type="string", dest='id_list', default=False,
                      help='only keep gene information for genes from an external list', metavar="FILE")




    (options, args) = parser.parse_args()

    if options.debug: print >> sys.stderr, '# Starting processing'

    process(options=options,args=args)




    sys.exit(0) # Everything went ok!

if __name__ == '__main__':
    main()
