#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015


def preprocess(infile, outfile, fields):
    with open(infile, 'r', encoding='utf-8') as infile_obj:
        header_line = infile_obj.readline()
        header_fields = header_line.split(" (")[0].split()[1:]
        output_header_list = [header_fields[i] for i in fields]
        output_header_line = "\t".join(output_header_list) + "\n"
        with open(outfile, 'w', encoding='utf-8') as outfile_obj:
            outfile_obj.write(output_header_line)
            for line in infile_obj:
                outlist = [line.split("\t")[i] for i in fields]
                outline = "\t".join(outlist) + "\n"
                outfile_obj.write(outline)
