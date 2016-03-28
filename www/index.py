#!/usr/bin/env python
# coding: utf8

# Author: Lenz Furrer, 2015


from __future__ import division, print_function #, unicode_literals

import sys
import os
import re
import cgi
import multiprocessing as mp
import time
import glob
import codecs
import zipfile
import hashlib
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
CGI_URL = 'http://kitt.cl.uzh.ch/kitt/cgi-bin/biodb/index.py'
WSGI_URL = 'http://kitt.cl.uzh.ch/kitt/biodb/'

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

if BUILDERPATH not in sys.path:
    sys.path.append(BUILDERPATH)
import biodb_wrapper
import settings


# Some shorthands.
se = etree.SubElement
NBSP = u'\xA0'
WAIT_MESSAGE = ('Please wait while the resource is being created '
                '(this may take up to 30 minutes, '
                'depending on the size of the resource).')


def main():
    '''
    Run this as a CGI script.
    '''
    fields = cgi.FieldStorage()

    url = CGI_URL
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

    url = WSGI_URL
    output, response_headers = main_handler(fields, url)

    # HTTP response.
    status = '200 OK'
    start_response(status, response_headers)

    return [output]


def main_handler(fields, self_url):
    '''
    Main program logic, used in both WSGI and CGI mode.
    '''
    # Respond to the user requests.
    creation_request = fields.getlist('resources')
    job_id = fields.getfirst('dlid')
    zipped = fields.getfirst('zipped')

    if creation_request:
        # A creation request has been submitted.
        resources = biodb_wrapper.resource_paths(*creation_request)
        renaming = parse_renaming(fields)
        job_id = job_hash(resources, renaming)

        # rb: read back identifiers if all resources are selected.
        rb = set(RESOURCES).issubset(creation_request)
        plot_email = fields.getfirst('plot-email')
        params = (resources, renaming, zipped, plot_email, rb, job_id, True)

        if fields.getfirst('requested-through') == 'ajax':
            # If AJAX is possible, return only a download link to be included.
            return ajax_response(params)

        # Without AJAX, proceed with the dumb auto-refresh mode.
        start_resource_creation(params)

    if job_id is None:
        # Empty form.
        html = input_page()
        if fields.getfirst('del') == 'all':
            clean_up_dir(DOWNLOADDIR, html, clear_all=True)
    else:
        # Creation has started already. Check for the resulting CSV.
        html = response_page()
        result = handle_download_request(job_id, zipped, bool(creation_request))
        html.find('.//*[@id="div-result"]').append(result)
        if result.text == WAIT_MESSAGE:
            # Add auto-refresh to the page.
            link = '{}?dlid={}'.format(self_url, job_id)
            if zipped:
                link += '&zipped=true'
            se(html.find('head'), 'meta',
                {'http-equiv': "refresh",
                 'content': "5; url={}".format(link)})

    # Serialise the complete page.
    html.find('.//a[@id="anchor-title"]').set('href', self_url)
    html.find('.//a[@id="anchor-reset"]').set('href', self_url)
    output = etree.tostring(html, method='HTML', encoding='UTF-8',
                            xml_declaration=True, pretty_print=True,
                            doctype='<!doctype html>')
    # HTTP boilerplate.
    response_headers = [('Content-Type', 'text/html;charset=UTF-8'),
                        ('Content-Length', str(len(output)))]

    return output, response_headers


def input_page():
    '''
    Page with the input forms.
    '''
    html = etree.HTML(PAGE)
    html.find('.//div[@id="div-download-page"]').set('class', 'hidden')
    populate_checkboxes(html, RESOURCES.iteritems())
    add_resource_labels(html, HERE)
    return html


def response_page():
    '''
    Page with download information.
    '''
    html = etree.HTML(PAGE)
    html.find('.//div[@id="div-input-form"]').set('class', 'hidden')
    return html


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
    label = 'skip CTD entries that are MeSH duplicates'
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


def job_hash(resources, renaming):
    '''
    Create a hash value considering all options for this job.
    '''
    m = hashlib.sha1()
    for r in sorted(resources):
        # Update with the resource selection (inlucding skip option).
        m.update(r)
        p = resources[r]
        # p is either a boolean (flag for skipping redundancy),
        # a pair of str (paths) in the case of MeSH,
        # or a str (one path), in all other cases.
        # If there are paths, the last-modified time is added to the hash.
        if isinstance(p, basestring):
            p = (p,)
        if isinstance(p, tuple):
            for pp in p:
                # Update with the timestamps (whole-second precision is enough).
                m.update(str(int(os.path.getmtime(pp))))
    for level in sorted(renaming):
        for entry in sorted(renaming[level].iteritems()):
            for e in entry:
                # Update with any renaming rules.
                m.update(e.encode('utf8'))
    return m.hexdigest()


def ajax_response(params):
    '''
    Run the pipeline and return an HTML fragment when finished.
    '''
    create_resource(*params)
    job_id, zipped = params[5], params[2]
    outcome = handle_download_request(job_id, zipped, False)

    output = etree.tostring(outcome, method='HTML', encoding='UTF-8')
    response_headers = [('Content-Type', 'text/html;charset=UTF-8'),
                        ('Content-Length', str(len(output)))]

    return output, response_headers


def start_resource_creation(params):
    '''
    Asynchronous initialisation.
    '''
    # Start the creation process, but don't wait for its termination.
    p = mp.Process(target=create_resource,
                   args=params)
    p.start()


def handle_download_request(job_id, zipped, is_recent):
    '''
    Check if the CSV is ready yet, or if an error occurred.
    '''
    msg = etree.Element('p')
    fn = job_id + '.csv'
    path = os.path.join(DOWNLOADDIR, fn)
    if zipped and os.path.exists(zipname(path)):
        success_msg(msg, zipname(fn), zipname(path))
    elif not zipped and os.path.exists(path):
        success_msg(msg, fn, path)
    elif os.path.exists(path + '.log'):
        with codecs.open(path + '.log', 'r', 'utf8') as f:
            msg.text = 'Runtime error: {}'.format(f.read())
    elif os.path.exists(path + '.tmp') or is_recent:
        msg.text = WAIT_MESSAGE
    else:
        msg.text = 'Cannot find the requested resource.'
    return msg


def success_msg(msg, fn, path):
    '''
    Create a download link.
    '''
    msg.text = 'Download resource: '
    link = se(msg, 'a', href=DL_URL+fn)
    link.text = fn
    link.tail = size_fmt(os.path.getsize(path), ' ({:.1f} {}B)')


def size_fmt(num, fmt='{:.1f} {}B'):
    '''
    Convert file size to a human-readable format.
    '''
    for prefix in ('', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi'):
        if abs(num) < 1024:
            return fmt.format(num, prefix)
        num /= 1024
    return fmt.format(num, 'Yi')


def zipname(fn):
    '''
    Replace the last 4 characters with ".zip".
    '''
    return fn[:-4] + '.zip'


def create_resource(resources, renaming,
                    zipped=False, plot_email=None, read_back=False,
                    job_id=None, log_exception=False):
    '''
    Run the Bio Term Hub resource creation pipeline, if necessary.
    '''
    if job_id is None:
        job_id = job_hash(resources, renaming)
    target_fn = os.path.join(DOWNLOADDIR, job_id + '.csv')
    target_fn = os.path.abspath(target_fn)
    r_value = target_fn

    # Check if we really have to create this resource
    # (it might already exist from an earlier job).
    if os.path.exists(target_fn):
        # Touch this file to keep it from being cleaned away.
        os.utime(target_fn, None)
    else:
        try:
            _create_resource(target_fn, resources, renaming, read_back)
        except StandardError as e:
            if log_exception:
                with codecs.open(target_fn + '.log', 'w', 'utf8') as f:
                    f.write('{}: {}\n'.format(e.__class__.__name__, e))
                return
            else:
                raise

    if zipped:
        zipfn = zipname(target_fn)
        r_value = zipfn
        if os.path.exists(zipfn):
            os.utime(zipfn, None)  # touch as well
        else:
            with zipfile.ZipFile(zipfn, 'w', zipfile.ZIP_DEFLATED) as f:
                f.write(target_fn, job_id + '.csv')
    if plot_email:
        pending_fn = os.path.join(settings.path_batch, 'pending')
        plot_email = re.sub(r'\s+', '', plot_email)
        with codecs.open(pending_fn, 'a', 'utf8') as f:
            f.write('{} {}\n'.format(plot_email, target_fn))

    # Remove old, unused files.
    clean_up_dir(DOWNLOADDIR, None)

    return r_value


def _create_resource(target_fn, resources, renaming, read_back=False):
    '''
    Run the Bio Term Hub resource creation pipeline.
    '''
    with cd(BUILDERPATH):
        import unified_builder as ub
        rsc = ub.RecordSetContainer(**resources)
        ub.UnifiedBuilder(rsc, target_fn + '.tmp', mapping=renaming)
    os.rename(target_fn + '.tmp', target_fn)
    if read_back:
        # Read back resource and entity type names.
        for level in ('resources', 'entity_types'):
            names = sorted(rsc.__getattribute__(level))
            fn = os.path.join(HERE, '{}.identifiers'.format(level))
            with codecs.open(fn, 'w', 'utf8') as f:
                f.write('\n'.join(names) + '\n')


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


PAGE = u'''<!doctype html>
<html>
<head>
  <meta charset="UTF-8"/>
  <title>Biomedical Terminology Resource</title>
  <link rel='stylesheet' type='text/css'
    href='https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css'/>
  <style>
    body { background:WhiteSmoke; }
    td { padding: 0cm 0.2cm 0cm 0.2cm; }
    .hidden { display: none; }
    .visible {}
  </style>
  <script type="text/javascript">
    // The "select all" checkbox.
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
            result_div = document.getElementById('div-result'),
            inp_div = document.getElementById('div-input-form'),
            resp_div = document.getElementById('div-download-page');

        // Replace the form div with the response div.
        inp_div.setAttribute('class', 'hidden');
        resp_div.setAttribute('class', 'visible');

        // Mark this request as originating from AJAX.
        fdata.append("requested-through", "ajax");

        // Status handling.
        xmlhttp.onreadystatechange = function() {
          if (xmlhttp.readyState == 4) {
            if (xmlhttp.status == 200) {
              result_div.innerHTML = xmlhttp.responseText;
            } else {
              result_div.innerHTML = "Error " + xmlhttp.status + " occurred";
            }
          } else {
            result_div.innerHTML = WAIT_MESSAGE;
          }
        }

        // Data transmission.
        xmlhttp.open("POST", 'index.py', true);
        xmlhttp.timeout = 1800000;  // 30 minutes
        xmlhttp.send(fdata);

        // Prevent reloading the page on submit.
        ev.preventDefault();
      }, false);
    }
  </script>
</head>
<body>
  <center>
    <h1 class="page-header">
      <a id="anchor-title" style="color: black; text-decoration: none;">
        Biomedical Terminology Resource
      </a>
    </h1>
  </center>
  <center>
    <div id="div-msg"></div>
    <div style="width: 80%; padding-bottom: 1cm;">
      <div id="div-input-form">
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
                You may use regular expressions (see the examples below).</p>
              <p>You can define multiple pattern-replacement pairs
                by using corresponding lines in the left/right box.</p>
              <label>Resources:</label>
              <table>
                <tr>
                  <td><textarea rows="3" cols="35" name="resource-std" placeholder="[pattern]"></td>
                  <td><textarea rows="3" cols="35" name="resource-custom" placeholder="[replacement]"></td>
                </tr>
                <tr>
                  <td>Example: mesh.* → MESH</td>
                </tr>
              </table>
              <label>Entity types:</label>
              <table>
                <tr>
                  <td><textarea rows="3" cols="35" name="entity_type-std" placeholder="[pattern]"></td>
                  <td><textarea rows="3" cols="35" name="entity_type-custom" placeholder="[replacement]"></td>
                </tr>
                <tr>
                  <td>Example: (organism|species) → organism</td>
                </tr>
              </table>
            </div>
            <hr/>
            <div style="padding-bottom: .3cm;">
              <p>
                <label>Send me an e-mail with resource statistics plots:</label>
                <input type="text" name="plot-email" placeholder="user@example.com" />
              </p>
              <input type="submit" value="Create resource" />&nbsp;&nbsp;&nbsp;
              <input type="checkbox" name="zipped" checked="checked" value="true" />&nbsp;download as Zip archive
            </div>
          </form>
        </div>
        <hr/>
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
      <div id="div-download-page">
        <center>
          <h3>Download</h3>
          <div id="div-result"></div>
          <hr/>
          <p><a id="anchor-reset">Reset</a></p>
        </center>
      </div>
    </div>
  </center>
</body>
</html>
'''
PAGE = PAGE.replace('WAIT_MESSAGE', repr(WAIT_MESSAGE))


if __name__ == '__main__':
    main()
