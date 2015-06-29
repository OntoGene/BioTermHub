import codecs

def preprocess(infile, outfile, fields):
    with codecs.open(infile, 'r') as infile_obj:
        header_line = infile_obj.readline()
        header_fields = header_line.split(" (")[0].split()[1:]
        output_header_list = [header_fields[i] for i in fields]
        output_header_line = "\t".join(output_header_list) + "\n"
        with codecs.open(outfile, 'w') as outfile_obj:
            outfile_obj.write(output_header_line)
            for line in infile_obj:
                outlist = [line.split("\t")[i] for i in fields]
                outline = "\t".join(outlist) + "\n"
                outfile_obj.write(outline)
