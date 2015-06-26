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


DOWNLOADDIR = os.path.join(HERE, 'downloads')
THISURL = ('http://localhost:8080/hex/users/furrer/'
           'tia2015-biomed-term-res/terminology_tool/www')

RESOURCES = [
    ('Uniprot', 'uniprot', "1k_snippets/uniprot_sprot-3"),
    ('Cellosaurus', 'cellosaurus', "1k_snippets/cellosaurus-2"),
    ('EntrezGene', 'entrezgene', "1k_snippets/gene_info-3"),
    ('MeSH', 'mesh', ("1k_snippets/desc-1k", "1k_snippets/supp-1k")),
    ('Taxdump', 'taxdump', "1k_snippets/names-1k)")]

# Some shorthands.
se = etree.SubElement
NBSP = u'\xA0'


def application(environ, start_response):
    html = etree.HTML(PAGE)

    populate_checkboxes(html, RESOURCES)
    resource_keys = {id_: loc for _, id_, loc in RESOURCES}

    fields = FieldStorage(fp=environ['wsgi.input'], environ=environ,
                          keep_blank_values=1)
    download_id = fields.getfirst('dlid')
    if download_id is not None:
        result = handle_download_request(download_id)
    else:
        requested = fields.getlist('resources')
        if requested:
            link = start_resource_creation(
                {id_: resource_keys[id_] for id_ in requested})
            result = etree.HTML('<p>The resource is being created.<br/>'
                                'A download link will be provided '
                                '<a href="{}" target="blank_">here</a> '
                                'in a few minutes.</p>'.format(link))
        else:
            result = etree.HTML('<p>[no pending request]</p>')

    if fields.getfirst('del') == 'all':
        for fn in glob.iglob('{}/*'.format(DOWNLOADDIR)):
            os.remove(fn)

    html.find('.//*[@id="div-result"]').append(result)

    # msg = repr(fields.getlist('resources'))
    # html.find('.//*[@id="div-msg"]').text = msg

    output = etree.tostring(html, method='HTML', encoding='UTF-8',
                            xml_declaration=True, pretty_print=True)
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
    timestamp = int(time.time())
    download_id = '{}-{}'.format(timestamp, '-'.join(sorted(resources)))
    target_fn = os.path.join(DOWNLOADDIR, '{}.csv'.format(download_id))
    # Start the categorisation process, but don't wait for its termination.
    p = mp.Process(target=create_resource,
                   args=(resources, target_fn))
    p.start()
    return '{}?dlid={}'.format(THISURL, download_id)


def handle_download_request(download_id):
    fn = '{}.csv'.format(download_id)
    if os.path.exists(os.path.join(DOWNLOADDIR, fn)):
        address = '/'.join((THISURL, 'downloads', fn))
        link = etree.Element('a', {'href': address})
        link.text = fn
        return link
    else:
        return etree.HTML('<p>[not ready yet]</p>')


def create_resource(resources, target_fn):
    rsc = ub.RecordSetContainer(**resources)
    ub.UnifiedBuilder(rsc, target_fn + '.tmp')
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
