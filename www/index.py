#!/usr/bin/env python
# coding: utf8

# Author: Lenz Furrer, 2015


from __future__ import division, print_function #, unicode_literals

import sys
import os
import cgi
import multiprocessing as mp
import time
import glob
import codecs
from collections import OrderedDict
from contextlib import contextmanager

from lxml import etree
import cgitb
cgitb.enable()

HERE = os.path.dirname(__file__)

# Config globals.
DOWNLOADDIR = os.path.join(HERE, 'downloads')
SCRIPT_NAME = os.path.basename(__file__)
BUILDERPATH = os.path.realpath(os.path.join(HERE, '..', 'adrian'))
DL_URL = 'http://kitt.cl.uzh.ch/kitt/biodb/downloads/'

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
    job_id = fields.getfirst('dlid')

    if creation_request:
        # A creation request has been submitted.
        timestamp = int(time.time())
        job_id = '{}-{}'.format(timestamp, '-'.join(sorted(creation_request)))

        renaming = parse_renaming(fields)
        # rb: read back identifiers if all resources are selected.
        rb = set(RESOURCES).issubset(creation_request)

        if fields.getfirst('requested-through') == 'ajax':
            # If AJAX is possible, return only a download link to be included.
            return ajax_response(creation_request, renaming, job_id, rb)

        # Without AJAX, proceed with the dumb auto-refresh mode.
        start_resource_creation(creation_request, renaming, job_id, rb)


    if job_id is None:
        # Empty form.
        result = etree.XML('<p>[no pending request]</p>')
    else:
        # Creation has started already. Check for the resulting CSV.
        result = handle_download_request(job_id)
        if result.text.startswith('[Please wait'):
            # Add auto-refresh to the page.
            link = '{}?dlid={}'.format(self_url, job_id)
            se(html.find('head'), 'meta',
                {'http-equiv': "refresh",
                 'content': "5; url={}".format(link)})

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
    cell = se(tbl.getparent(), 'p')
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
                m[level][std.strip()] = custom.strip()
    return m


def ajax_response(resources, renaming, job_id, read_back):
    '''
    Run the pipeline and return an HTML fragment when finished.
    '''
    create_resource(resources, renaming, job_id, read_back)
    outcome = handle_download_request(job_id)

    output = etree.tostring(outcome, method='HTML', encoding='UTF-8')
    response_headers = [('Content-Type', 'text/html;charset=UTF-8'),
                        ('Content-Length', str(len(output)))]

    return output, response_headers


def start_resource_creation(resources, renaming, job_id, read_back):
    '''
    Asynchronous initialisation.
    '''
    # Start the creation process, but don't wait for its termination.
    p = mp.Process(target=create_resource,
                   args=(resources, renaming, job_id, read_back))
    p.start()

    return job_id


def handle_download_request(job_id):
    '''
    Check if the CSV is ready yet, or if an error occurred.
    '''
    msg = etree.Element('p')
    fn = '{}.csv'.format(job_id)
    path = os.path.join(DOWNLOADDIR, fn)
    if os.path.exists(path):
        msg.text = 'Download resource: '
        se(msg, 'a', href=DL_URL+fn).text = fn
    elif os.path.exists(path + '.log'):
        with codecs.open(path + '.log', 'r', 'utf8') as f:
            msg.text = 'Runtime error: {}'.format(f.read())
    elif os.path.exists(path + '.tmp') or is_recent(job_id, 10):
        msg.text = '[Please wait while the resource is being created...]'
    else:
        msg.text = 'The requested resource seems not to exist.'
    return msg


def create_resource(resources, renaming, job_id, read_back=False):
    '''
    Run the BioDB resource creation pipeline.
    '''
    try:
        target_fn = os.path.join(DOWNLOADDIR, '{}.csv'.format(job_id))
        target_fn = os.path.abspath(target_fn)
        with cd(BUILDERPATH):
            if BUILDERPATH not in sys.path:
                sys.path.append(BUILDERPATH)
            import unified_builder as ub
            import biodb_wrapper
            rsc = biodb_wrapper.ub_wrapper(*resources)
            ub.UnifiedBuilder(rsc, target_fn + '.tmp', mapping=renaming)
    except StandardError as e:
        with codecs.open(target_fn + '.log', 'w', 'utf8') as f:
            f.write('{}: {}\n'.format(e.__class__.__name__, e))
    else:
        os.rename(target_fn + '.tmp', target_fn)
        if read_back:
            # Read back resource and entity type names.
            for level in ('resources', 'entity_types'):
                names = sorted(rsc.__getattribute__(level))
                fn = os.path.join(HERE, '{}.identifiers'.format(level))
                with codecs.open(fn, 'w', 'utf8') as f:
                    f.write('\n'.join(names) + '\n')


def is_recent(job_id, seconds):
    '''
    Determine if job_id was created no more than n seconds ago.
    '''
    try:
        timestamp = int(job_id.split('-')[0])
    except ValueError:
        # Invalid download ID.
        return False
    return time.time() - timestamp <= seconds


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
    td { padding: 0cm 0.2cm 0cm 0.2cm; }
  </style>
  <script type="text/javascript">
    // "select all" checkbox.
    function checkAll(bx) {
      var cbs = document.getElementsByTagName('input');
      for (var i=cbs.length; i--;) {
        if(cbs[i].type == 'checkbox') {
          cbs[i].checked = bx.checked;
        }
      }
    }

    // AJAX stuff.
    onload = function() {
      var form = document.getElementById('form-res');
      form.addEventListener('submit', function(ev) {

        var xmlhttp = new XMLHttpRequest(),
            fdata = new FormData(form),
            res_div = document.getElementById('div-result');

        fdata.append("requested-through", "ajax");

        xmlhttp.onreadystatechange = function() {
          if (xmlhttp.readyState == 4) {
            if (xmlhttp.status == 200) {
              res_div.innerHTML = xmlhttp.responseText;
            } else {
              res_div.innerHTML = "Error " + xmlhttp.status + " occurred";
            }
          } else {
            res_div.innerHTML = "Please wait while the resource is being created...";
          }
        }

        xmlhttp.open("POST", 'index.py', true);
        xmlhttp.send(fdata);
        ev.preventDefault();

        // Disable the checkboxes.
        var bx = document.getElementById('inp-select-all');
        bx.checked = false;
        checkAll(bx);

      }, false);
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
              <p><input type="checkbox" id="inp-select-all" name="all" onclick="checkAll(this)"/> select all</p>
              <table id="tbl-checkboxes"></table>
            </div>
            <hr/>
            <div id="div-renaming">
              <p>Use the following text boxes to change the labeling of resources and entity types.
                You may use regular expressions (eg. "mesh desc.*").</p>
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
            <div style="padding-top: .5cm; padding-bottom: .3cm;">
              <input type="submit" value="Create resource" />
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
