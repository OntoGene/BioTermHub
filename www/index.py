#!/usr/bin/env python
# coding: utf8

# Author: Lenz Furrer, 2015


from __future__ import division, print_function #, unicode_literals

import os
import cgi
import subprocess as sp
import time
import glob
import codecs
from collections import OrderedDict

from lxml import etree
import cgitb
cgitb.enable()

HERE = os.path.dirname(__file__)

# Config globals.
DOWNLOADDIR = os.path.join(HERE, 'downloads')
SCRIPT_NAME = os.path.basename(__file__)
BUILDERPATH = os.path.realpath(os.path.join(HERE, '..', 'adrian'))

RESOURCES = OrderedDict((
    ('cellosaurus', 'Cellosaurus'),
    ('chebi', 'ChEBI'),
    ('ctd_chem', 'CTD chemicals'),
    ('ctd_disease', 'CTD diseases'),
    ('entrezgene', 'EntrezGene'),
    ('mesh', 'MeSH'),
    ('taxdump', 'Taxdump'),
    ('uniprot', 'Uniprot'),
))

# Some shorthands.
se = etree.SubElement
NBSP = u'\xA0'


def main():
    '''
    Run this as a CGI script.
    '''
    fields = cgi.FieldStorage()

    url = 'http://kitt.cl.uzh.ch/kitt/cgi-bin/biodb/index.py'
    output, response_headers = main_handler(fields, url)

    # HTTP response.
    for entry in response_headers:
        print('{}: {}'.format(*entry))
    print()
    print(output)


def application(environ, start_response):
    '''
    Run this as a WSGI script.
    '''
    fields = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ)

    url = 'http://kitt.cl.uzh.ch/kitt/biodb/'
    output, response_headers = main_handler(fields, url)

    # HTTP response.
    status = '200 OK'
    start_response(status, response_headers)

    return [output]


def main_handler(fields, self_url):
    '''
    Main program logic, used in both WSGI and CGI mode.
    '''
    # Build the page.
    html = etree.HTML(PAGE)
    populate_checkboxes(html, RESOURCES.iteritems())
    add_resource_labels(html, HERE)
    clean_up_dir(DOWNLOADDIR, html, fields.getfirst('del') == 'all')

    # Respond to the user requests.
    creation_request = fields.getlist('resources')
    download_id = fields.getfirst('dlid')
    download_ready = fields.getfirst('download')

    if download_ready:
        try:
            return deliver_download(download_ready)
        except StandardError:
            pass

    if creation_request:
        # A creation request has been submitted.
        renaming = parse_renaming(fields)
        # rb: read back identifiers if all resources are selected.
        rb = set(RESOURCES).issubset(creation_request)
        download_id = start_resource_creation(creation_request, renaming, rb)

    if download_id is None:
        # Empty form.
        result = etree.XML('<p>[no pending request]</p>')
    else:
        # Creation has started already. Check for the resulting CSV.
        result = handle_download_request(download_id, self_url)
        if result.text.startswith('[Please wait'):
            # Add auto-refresh to the page.
            link = '{}?dlid={}'.format(self_url, download_id)
            se(html.find('head'), 'meta',
                {'http-equiv': "refresh",
                 'content': "15; url={}".format(link)})

    # Serialise the complete page.
    html.find('.//*[@id="div-result"]').append(result)
    output = etree.tostring(html, method='HTML', encoding='UTF-8',
                            xml_declaration=True, pretty_print=True,
                            doctype='<!doctype html>')
    # HTTP boilerplate.
    response_headers = [('Content-Type', 'text/html;charset=UTF-8'),
                        ('Content-Length', str(len(output)))]

    return output, response_headers


def populate_checkboxes(doc, resources):
    '''
    Insert a labeled checkbox for every resource.
    '''
    tbl = doc.find('.//table[@id="tbl-checkboxes"]')
    atts = dict(type='checkbox', name='resources')
    for id_, label in resources:
        atts['value'] = id_
        se(se(se(se(tbl, 'tr'), 'td'), 'p'), 'input', atts).tail = NBSP + label
        if id_ == 'ctd_chem':
            cell = se(se(tbl[-1], 'td', rowspan='2'), 'p', font='-1')
            atts['value'] = 'ctd_lookup'
            label = 'avoid duplicates found in MeSH also'
            se(cell, 'input', atts).tail = NBSP + label
            se(cell, 'br').tail = ('(has no effect unless both CTD and MeSH '
                                   'are selected)')


def add_resource_labels(doc, path):
    '''
    Add a list of existing resource/entity type identifiers.
    '''
    for level in ('resources', 'entity_types'):
        fn = os.path.join(path, '{}.identifiers'.format(level))
        with codecs.open(fn, 'r', 'utf8') as f:
            names = f.read().strip().split('\n')
        if names:
            cell = doc.find('.//td[@id="td-{}-ids"]'.format(level[:3]))
            cell.text = names[0]
            for n in names[1:]:
                se(cell, 'br').tail = n


def clean_up_dir(dirpath, doc, clear_all=False):
    '''
    Remove old (or all) files under this directory.
    '''
    fns = glob.iglob('{}/*'.format(dirpath))
    if clear_all:
        # Hidden functionality: clear the downloads directory with "?del=all".
        # Report this when it happens.
        del_fns = list(fns)
        msg = 'INFO: removed {} files in {}.'.format(len(del_fns), dirpath)
        doc.find('.//*[@id="div-msg"]').text = msg
    else:
        # Automatic clean-up of files older than 35 days.
        deadline = time.time() - 3024000  # 35 * 24 * 3600
        del_fns = [fn for fn in fns if os.path.getmtime(fn) < deadline]
    for fn in del_fns:
        os.remove(fn)


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


def start_resource_creation(resources, renaming, read_back):
    '''
    Asynchronous initialisation as a subprocess.
    '''
    args = ['python', 'create_resource.py']

    timestamp = int(time.time())
    download_id = '{}-{}'.format(timestamp, '-'.join(sorted(resources)))
    target_fn = os.path.join(DOWNLOADDIR, '{}.csv'.format(download_id))
    args.extend(['-t', target_fn])

    args.append('-r')
    args.extend(resources)

    flag = {'resource': '-n', 'entity_type': '-e'}
    for level, mapping in renaming.iteritems():
        if mapping:
            args.append(flag[level])
            args.extend(i for item in mapping.iteritems() for i in item)

    if read_back:
        args.append('-b')

    # Start the creation process, but don't wait for its termination.
    sp.Popen(args)

    return download_id


def handle_download_request(download_id, self_url):
    '''
    Check if the CSV is ready yet, or if an error occurred.
    '''
    msg = etree.Element('p')
    fn = '{}.csv'.format(download_id)
    path = os.path.join(DOWNLOADDIR, fn)
    if os.path.exists(path):
        msg.text = 'Download resource: '
        address = '{}?download={}'.format(self_url, fn)
        se(msg, 'a', href=address).text = fn
    elif os.path.exists(path + '.log'):
        with codecs.open(path + '.log', 'r', 'utf8') as f:
            msg.text = 'Runtime error: {}'.format(f.read())
    elif os.path.exists(path + '.tmp') or is_recent(download_id, 10):
        msg.text = '[Please wait while the resource is being created...]'
    else:
        msg.text = 'The requested resource seems not to exist.'
    return msg


def deliver_download(fn):
    '''
    Return the requested file to the user.
    '''
    # TODO: prevent reading a huge file into memory too early.
    path = os.path.join(DOWNLOADDIR, fn)
    with open(path) as f:
        output = f.read()
    response_headers = [
        ('Content-Type', 'application/octet-stream; name="{}"'.format(fn)),
        ('Content-Disposition', 'attachment; filename="{}"'.format(fn)),
        ('Content-Length', str(len(output)))]
    return output, response_headers


def is_recent(download_id, seconds):
    '''
    Determine if download_id was created no more than n seconds ago.
    '''
    try:
        timestamp = int(download_id.split('-')[0])
    except ValueError:
        # Invalid download ID.
        return False
    return time.time() - timestamp <= seconds


PAGE = '''<!doctype html>
<html>
<head>
  <meta charset="UTF-8"/>
  <title>Biomedical Terminology Resource</title>
  <link rel='stylesheet' type='text/css'
    href='https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css'/>
  <style>
    body { background:WhiteSmoke; }
    td { padding: 0cm 0.2cm 0cm 0.2cm; }
  </style>
  <script type="text/javascript">
    function checkAll(bx) {
      var cbs = document.getElementsByTagName('input');
      for (var i=cbs.length; i--;) {
        if(cbs[i].type == 'checkbox') {
          cbs[i].checked = bx.checked;
        }
      }
    }
  </script>
</head>
<body>
  <center>
    <h1 class="page-header">
      Biomedical Terminology Resource
    </h1>
  </center>
  <center>
    <div id="div-msg"></div>
    <div style="width: 80%; padding-bottom: 1cm;">
      <div style="float: left; width: 50%;">
        <h3>Resource Selection</h3>
        <div style="text-align: left">
          <form id="form-res" role="form" method="post"
                accept-charset="UTF-8">
            <div id="div-checkboxes">
              <label>Please select the resources to be included:</label>
              <p><input type="checkbox" name="all" onclick="checkAll(this)"/> select all</p>
              <table id="tbl-checkboxes"></table>
            </div>
            <hr/>
            <div id="div-renaming">
              <p>Use the following text boxes to change the labeling of resources and entity types.
                Unix-style regex (eg. "mesh desc.*") are allowed.</p>
              <p>You can use define multiple pattern-replacement pairs
                by using corresponding lines in the left/right box.</p>
              <label>Resources:</label>
              <table>
                <tr>
                  <td><textarea rows="3" cols="35" name="resource-std" placeholder="[pattern]"></td>
                  <td><textarea rows="3" cols="35" name="resource-custom" placeholder="[replacement]"></td>
                </tr>
              </table>
              <label>Entity types:</label>
              <table>
                <tr>
                  <td><textarea rows="3" cols="35" name="entity_type-std" placeholder="[pattern]"></td>
                  <td><textarea rows="3" cols="35" name="entity_type-custom" placeholder="[replacement]"></td>
                </tr>
              </table>
            </div>
            <div style="padding-top: .6cm;">
              <input type="submit"/>
            </div>
          </form>
        </div>
      </div>
      <div style="float: right; width: 50%;">
        <center>
          <h3>Download</h3>
          <div id="div-result"></div>
        </center>
      </div>
      <div style="clear: both">
        <hr/>
      </div>
      <table>
        <tr>
          <th>Existing resource labels:</th>
          <th>Existing entity-type labels:</th>
        </tr>
        <tr style="vertical-align: top;">
          <td id="td-res-ids"></td>
          <td id="td-ent-ids"></td>
        </tr>
      </table>
    </div>
  </center>
</body>
</html>
'''


if __name__ == '__main__':
    main()
