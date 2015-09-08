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
from collections import OrderedDict
from contextlib import contextmanager

from lxml import etree
import cgitb
cgitb.enable()

HERE = os.path.dirname(__file__)

# Config globals.
DOWNLOADDIR = os.path.join(HERE, 'downloads')
THISURL = ('http://kitt.cl.uzh.ch/kitt/biodb/')
SCRIPT_NAME = os.path.basename(__file__)

RESOURCES = OrderedDict((
    ('cellosaurus', 'Cellosaurus'),
    ('chebi', 'ChEBI'),
    ('ctd_chem', 'CTD chemicals'),
    ('ctd_disease', 'CTD diseases'),
    ('entrezgene', 'EntrezGene'),
    ('mesh_desc', 'MeSH description'),
    ('mesh_supp', 'MeSH supplement'),
    ('taxdump', 'Taxdump'),
    ('uniprot', 'Uniprot'),
))

# Some shorthands.
se = etree.SubElement
NBSP = u'\xA0'

PROJECTPATH = ('/mnt/storage/hex/users/furrer/'
               'tia2015-biomed-term-res/terminology_tool/adrian/')


def application(environ, start_response):
    # Build the page.
    html = etree.HTML(PAGE)
    populate_checkboxes(html, RESOURCES.iteritems())

    # Respond to the user requests.
    fields = FieldStorage(fp=environ['wsgi.input'], environ=environ)
                          #keep_blank_values=1)
    download_id = fields.getfirst('dlid')
    creation_request = fields.getlist('resources')

    if creation_request:
        # A creation request has been submitted.
        renaming = parse_renaming(fields)
        download_id = start_resource_creation(creation_request, renaming)

    if download_id is None:
        # Empty form.
        result = etree.HTML('<p>[no pending request]</p>')
    else:
        # Creation has started already. Check for the resulting CSV.
        result = handle_download_request(download_id)
        if result.text.startswith('[Please wait'):
            # Add auto-refresh to the page.
            head = html.find('head')
            link = '{}?dlid={}'.format(THISURL, download_id)
            se(head, 'meta', {'http-equiv': "refresh",
                              'content': "15; url={}".format(link)})

    # Hidden functionality: clear the downloads directory with "?del=all".
    if fields.getfirst('del') == 'all':
        delfns = glob.glob('{}/*'.format(DOWNLOADDIR))
        for fn in delfns:
            os.remove(fn)
        msg = 'INFO: removed {} files in {}.'.format(len(delfns), DOWNLOADDIR)
        html.find('.//*[@id="div-msg"]').text = msg

    # TODO: add automatic download-dir clean-up.

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
    div = doc.find('.//*[@id="div-checkboxes"]')
    for id_, label in resources:
        atts = dict(type='checkbox', name='resources', value=id_)
        se(se(div, 'p'), 'input', atts).tail = NBSP + label


def parse_renaming(fields):
    '''
    Get any user-specified renaming entries.
    '''
    m = {}
    for level in ('resource', 'entity_type'):
        m[level] = {}
        entries = [fields.getfirst('{}-{}'.format(level, n), '').split('\n')
                   for n in ('std', 'custom')]
        for std, custom in zip(*entries):
            if std and custom:
                m[level][std] = custom
    return m


def start_resource_creation(resources, renaming):
    '''
    Asynchronous initialisation.
    '''
    timestamp = int(time.time())
    download_id = '{}-{}'.format(timestamp, '-'.join(sorted(resources)))
    target_fn = os.path.join(DOWNLOADDIR, '{}.csv'.format(download_id))
    # Start the creation process, but don't wait for its termination.
    p = mp.Process(target=create_resource,
                   args=(resources, renaming, target_fn))
    p.start()
    return download_id


def handle_download_request(download_id):
    '''
    Check if the CSV is ready yet, or if an error occurred.
    '''
    msg = etree.Element('p')
    fn = '{}.csv'.format(download_id)
    path = os.path.join(DOWNLOADDIR, fn)
    if os.path.exists(path):
        msg.text = 'Download resource: '
        address = '/'.join((THISURL, 'downloads', fn))
        se(msg, 'a', href=address).text = fn
    elif os.path.exists(path + '.log'):
        with open(path + '.log') as f:
            msg.text = 'Runtime error: {}'.format(f.read())
    else:
        msg.text = '[Please wait while the resource is being created...]'
    return msg


def create_resource(resources, renaming, target_fn):
    '''
    Call the creation pipeline.

    Since exceptions are not raised to the parent process,
    write them to a log file.
    '''
    try:
        with cd(PROJECTPATH):
            if PROJECTPATH not in sys.path:
                sys.path.append(PROJECTPATH)
            import unified_builder as ub
            import biodb_wrapper
            # ub = reload(ub)  # make sure recent changes take effect
            rsc = biodb_wrapper.ub_wrapper(*resources)
            ub.UnifiedBuilder(rsc, target_fn + '.tmp', mapping=renaming)
            # TODO: read back rsc.resources and rsc.entity_types
            # and store them somewhere useful.
    except StandardError as e:
        with open(target_fn + '.log', 'w') as f:
            f.write('{}: {}\n'.format(e.__class__.__name__, e))
    else:
        os.rename(target_fn + '.tmp', target_fn)


@contextmanager
def cd(newdir):
    '''
    Temporarily change the working directory.
    '''
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)


PAGE = '''<!doctype html>
<html>
<head>
  <meta charset="UTF-8"/>
  <title>Biomedical Terminology Resource</title>
  <link rel='stylesheet' type='text/css'
    href='https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css'/>
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
            <div id="div-checkboxes">
              <label>Please select the resources to be included:</label>
            </div>
            <hr/>
            <div id="div-renaming">
              <p>Use the following text boxes to change the labeling of resources and entity types. Unix-style regex (eg. "mesh desc.*") are allowed.</p>
              <label>Resources:</label>
              <table>
                <tr>
                  <td><textarea rows="3" cols="35" name="resource-std"></td>
                  <td><textarea rows="3" cols="35" name="resource-custom"></td>
                </tr>
              </table>
              <label>Entity types:</label>
              <table>
                <tr>
                  <td><textarea rows="3" cols="35" name="entity_type-std"></td>
                  <td><textarea rows="3" cols="35" name="entity_type-custom"></td>
                </tr>
              </table>
            </div>
            <hr/>
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
