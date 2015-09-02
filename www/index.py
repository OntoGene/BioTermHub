#!/usr/bin/env python
# coding: utf8

# Author: Lenz Furrer, 2015


from __future__ import division, print_function #, unicode_literals

import sys
import os
from cgi import FieldStorage
import multiprocessing as mp
import time
import glob

from lxml import etree
import cgitb
cgitb.enable()

HERE = os.path.dirname(__file__)
libdir = os.path.join(HERE, 'lib')

sys.path.append('/mnt/storage/hex/users/furrer/'
                'tia2015-biomed-term-res/terminology_tool')
sys.path.append(libdir)
import adrian.unified_builder as ub
ub = reload(ub)  # make sure recent changes take effect


# Config globals.
DOWNLOADDIR = os.path.join(HERE, 'downloads')
THISURL = ('http://localhost:8080/hex/users/furrer/'
           'tia2015-biomed-term-res/terminology_tool/www')

RESOURCES = [
    ('Uniprot', 'uniprot', "1k_snippets/uniprot_sprot-3"),
    ('Cellosaurus', 'cellosaurus', "1k_snippets/cellosaurus-2"),
    ('EntrezGene', 'entrezgene', "1k_snippets/gene_info-3"),
    ('MeSH', 'mesh', ("1k_snippets/desc-1k", "1k_snippets/supp-1k")),
    ('Taxdump', 'taxdump', "1k_snippets/names-1k")]

# Some shorthands.
se = etree.SubElement
NBSP = u'\xA0'

snippetpath = ('/mnt/storage/hex/users/furrer/'
                'tia2015-biomed-term-res/terminology_tool/adrian/')

def application(environ, start_response):
    # Build the page.
    html = etree.HTML(PAGE)
    populate_checkboxes(html, RESOURCES)
    resource_keys = {id_: snippetpath+loc for _, id_, loc in RESOURCES}

    # Respond to the user requests.
    fields = FieldStorage(fp=environ['wsgi.input'], environ=environ,
                          keep_blank_values=1)
    download_id = fields.getfirst('dlid')
    creation_request = fields.getlist('resources')
    if download_id is not None:
        # Creation has started already. Check for the resulting CSV.
        result = handle_download_request(download_id)
    elif creation_request:
        # A creation request has been submitted.
        link = start_resource_creation(
            {id_: resource_keys[id_] for id_ in creation_request})
        result = etree.HTML('<p>The resource is being created.<br/>'
                            'A download link will be provided '
                            '<a href="{}" target="blank_">here</a> '
                            'in a few minutes.</p>'.format(link))
    else:
        # Empty form.
        result = etree.HTML('<p>[no pending request]</p>')

    # Hidden functionality: clear the downloads directory with "?del=all".
    if fields.getfirst('del') == 'all':
        delfns = glob.glob('{}/*'.format(DOWNLOADDIR))
        for fn in delfns:
            os.remove(fn)
        msg = 'INFO: removed {} files in {}.'.format(len(delfns), DOWNLOADDIR)
        html.find('.//*[@id="div-msg"]').text = msg

    # Serialise the complete page.
    html.find('.//*[@id="div-result"]').append(result)
    output = etree.tostring(html, method='HTML', encoding='UTF-8',
                            xml_declaration=True, pretty_print=True)

    # WSGI boilerplate: HTTP response.
    status = '200 OK'
    response_headers = [('Content-type', 'text/html;charset=UTF-8'),
                        ('Content-Length', str(len(output)))]
    start_response(status, response_headers)

    return [output]


def populate_checkboxes(doc, resources):
    '''
    Insert a labeled checkbox for every resource.
    '''
    form = doc.find('.//*[@id="form-res"]')
    for label, id_, _ in resources:
        atts = dict(type='checkbox', name='resources', value=id_)
        se(se(form, 'p'), 'input', atts).tail = NBSP + label
    # etree.SubElement appends new elements at the end --
    # now the button is at the second position.
    # Move it to the end:
    form.append(form[1])


def start_resource_creation(resources):
    '''
    Asynchronous initialisation.
    '''
    timestamp = int(time.time())
    download_id = '{}-{}'.format(timestamp, '-'.join(sorted(resources)))
    target_fn = os.path.join(DOWNLOADDIR, '{}.csv'.format(download_id))
    # Start the creation process, but don't wait for its termination.
    p = mp.Process(target=create_resource,
                   args=(resources, target_fn))
    p.start()
    return '{}?dlid={}'.format(THISURL, download_id)


def handle_download_request(download_id):
    '''
    Check if the CSV is ready yet, or if an error occurred.
    '''
    msg = etree.Element('p')
    fn = '{}.csv'.format(download_id)
    path = os.path.join(DOWNLOADDIR, fn)
    if os.path.exists(path):
        address = '/'.join((THISURL, 'downloads', fn))
        se(msg, 'a', {'href': address}).text = fn
    elif os.path.exists(path + '.log'):
        with open(path + '.log') as f:
            msg.text = 'Runtime error: {}'.format(f.read())
    else:
        msg.text = '[not ready yet]'
    return msg


def create_resource(resources, target_fn):
    '''
    Call the creation pipeline.

    Since exceptions are not raised to the parent process,
    write them to a log file.
    '''
    try:
        rsc = ub.RecordSetContainer(**resources)
        ub.UnifiedBuilder(rsc, target_fn + '.tmp')
    except StandardError as e:
        with open(target_fn + '.log', 'w') as f:
            f.write(str(e))
    else:
        os.rename(target_fn + '.tmp', target_fn)


PAGE = '''<!doctype html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Biomedical Terminology Resource</title>
  <link rel='stylesheet' type='text/css'
    href='https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css'>
  <style>
    body { background:WhiteSmoke; }
    td { padding: 0cm 0.2cm 0cm 0.2cm;
         vertical-align: top; }
    #div-result { padding-bottom: 1cm }
  </style>
</head>
<body>
  <center>
    <h1 class="page-header">
      Biomedical Terminology Resource
    </h1>
  </center>
  <center>
    <div id="div-msg"></div>
    <div style="width: 80%;">
      <div style="float: left; width: 50%;">
        <h3>Resource Selection</h3>
        <div style="text-align: left">
          <form id="form-res" role="form" action="." method="get"
                accept-charset="UTF-8">
            <label>Please select the resources to be included:</label>
            <input type="submit"/>
          </form>
        </div>
      </div>
      <div style="float: right; width: 50%;">
        <center>
          <h3>Download</h3>
          <div id="div-result"></div>
        </center>
      </div>
    </div>
    <div style="clear: both"></div>
  </center>
</body>
</html>
'''
