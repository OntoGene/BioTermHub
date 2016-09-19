#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2015--2016


'''
Web interface for the Bio Term Hub.
'''


import sys
import os
import re
import cgi
import multiprocessing as mp
import time
import datetime as dt
import math
import glob
import logging
import zipfile
import hashlib

from lxml import etree

HERE = os.path.dirname(__file__)
PACKAGEPATH = os.path.join(os.path.realpath(HERE), '..', '..')
if PACKAGEPATH not in sys.path:
    sys.path.append(PACKAGEPATH)
from termhub.core import settings
from termhub.core.aggregate import RecordSetContainer
from termhub.inputfilters import FILTERS
from termhub.update.fetch_remote import RemoteChecker


# Config globals.
LOGFILE = settings.log_file
DOWNLOADDIR = settings.path_download
SCRIPT_NAME = os.path.basename(__file__)
DL_URL = 'http://kitt.cl.uzh.ch/kitt/biodb/downloads/'
CGI_URL = './index.py'
WSGI_URL = '.'


# Some shorthands.
se = etree.SubElement
NBSP = '\xA0'
WAIT_MESSAGE = ('Please wait while the resource is being created '
                '(this may take a few minutes, '
                'depending on the size of the resource).')
with open(os.path.join(HERE, 'template.html'), encoding='utf8') as f:
    PAGE = f.read()
    PAGE = PAGE.replace('WAIT_MESSAGE', repr(WAIT_MESSAGE))
    PAGE = PAGE.replace('RESOURCE_NAMES', repr(list(FILTERS)))


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
    sys.stdout.flush()
    sys.stdout.buffer.write(output)


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
    # Set up the logger.
    logging.basicConfig(
        level=logging.INFO,
        filename=LOGFILE,
        format='%(process)d - %(asctime)s - %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    # Respond to the user requests.
    logging.info('Processing request: %r', fields)
    try:
        if 'check-request' in fields:
            resp = check_request(fields.getfirst('check-request'))
        elif 'update-request' in fields:
            resp = update_request(fields.getfirst('update-request'))
        else:
            resp = general_request(fields, self_url)
    except Exception:
        logging.exception('Runtime error.')
        raise
    else:
        return resp


def general_request(fields, self_url):
    '''
    Respond to a non-specific or creation request.
    '''
    creation_request = fields.getlist('resources')
    job_id = fields.getfirst('dlid')
    zipped = fields.getfirst('zipped')

    if creation_request:
        # A creation request has been submitted.
        rsc = RecordSetContainer(resources=creation_request,
                                 flags=fields.getlist('flags'))
        renaming = parse_renaming(fields)
        job_id = job_hash(rsc, renaming)

        plot_email = fields.getfirst('plot-email')
        log_exception = True
        params = (rsc, renaming, zipped, plot_email, job_id, log_exception)

        if fields.getfirst('requested-through') == 'ajax':
            # If AJAX is possible, return only a download link to be included.
            logging.info('Respond to an AJAX request.')
            # Run the aggregator and return only when finished.
            create_resource(*params)
            outcome = handle_download_request(job_id, zipped, False)
            return response(outcome)

        # Without AJAX, proceed with the dumb auto-refresh mode.
        logging.info('Respond with auto-refresh work-around.')
        start_resource_creation(params)

    return build_page(self_url, fields, creation_request, job_id, zipped)


def check_request(name):
    '''
    Check the last-modified date.
    '''
    # Check request only works with AJAX.
    remote = RemoteChecker(name)
    if remote.has_changed():
        msg = 'Update available.'
    else:
        msg = 'Up-to-date.'
    p = etree.Element('p')
    p.text = msg
    return response(p)


def update_request(name):
    '''
    Update a resource from remote.
    '''
    # Update request also only works with AJAX.
    remote = RemoteChecker(name)
    remote.update()
    p = etree.Element('p')
    p.text = 'Up-to-date.'
    return response(p)


def build_page(self_url, fields, creation_request, job_id, zipped):
    '''
    Complete page needed for initial loading and JS-free creation request.
    '''
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

    return response(html, xml_declaration=True, doctype='<!doctype html>')


def response(node, **kwargs):
    '''
    Serialise HTML and create headers.
    '''
    output = etree.tostring(node, method='HTML', encoding='UTF-8', **kwargs)
    response_headers = [('Content-Type', 'text/html;charset=UTF-8'),
                        ('Content-Length', str(len(output)))]

    return output, response_headers


def input_page():
    '''
    Page with the input forms.
    '''
    html = etree.HTML(PAGE)
    html.find('.//div[@id="div-download-page"]').set('class', 'hidden')
    resources = [RemoteChecker(id_) for id_ in FILTERS]
    resources.sort(key=lambda r: r.resource.dump_label().lower())
    populate_checkboxes(html, resources)
    add_resource_labels(html)
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
    for remote in resources:
        atts['value'] = remote.name  # = ID
        label = remote.resource.dump_label()
        last_modified = remote.stat.modified
        try:
            last_modified = dt.date.fromtimestamp(last_modified).isoformat()
        except TypeError:
            last_modified = 'unknown'
        row = se(tbl, 'tr')
        se(se(se(row, 'td'), 'p'), 'input', atts).tail = NBSP + label
        se(se(se(row, 'td'), 'div', id='div-update-{}'.format(remote.name)),
           'p').text = last_modified
        se(row, 'td').append(update_button(remote.name))
    # Add a checkbox for the CTD-lookup flag.
    cell = se(tbl.getparent(), 'p')
    atts = dict(type='checkbox', name='flags', value='ctd_lookup')
    label = 'skip CTD entries that are MeSH duplicates'
    se(cell, 'input', atts).tail = NBSP + label
    se(cell, 'br').tail = ('(has no effect unless both CTD and MeSH '
                           'are selected)')


def update_button(id_):
    '''
    Create an AJAX request button for updating a resource.
    '''
    return etree.Element('input', type='button', value='...',
                         id='btn-update-{}'.format(id_),
                         disabled='disabled')


def add_resource_labels(doc):
    '''
    Add a list of existing resource/entity type identifiers.
    '''
    for level in ('resource_names', 'entity_type_names'):
        names = set()
        for filter_ in FILTERS.values():
            names.update(getattr(filter_, level)())
        names = sorted(names, key=str.lower)
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


def job_hash(rsc, renaming):
    '''
    Create a hash value considering all options for this job.
    '''
    key = hashlib.sha1()
    for name, rec in rsc.resources:
        # Update with the resource selection.
        key.update(name.encode('utf8'))
        # For each dump file, add the last-modified time to the hash.
        for path in rec.dump_fns():
            # Update with the timestamps (whole-second precision is enough).
            key.update(str(int(os.path.getmtime(path))).encode('utf8'))
    # Update with the "skip" flag ("ctd_lookup").
    for flag in rsc.flags:
        key.update(flag.encode('utf8'))
    # Update with any renaming rules.
    for level in sorted(renaming):
        for entry in sorted(renaming[level].items()):
            for e in entry:
                key.update(e.encode('utf8'))
    return base36digest(key.digest())


def base36digest(octets):
    '''
    Convert a hash digest to base 36.
    '''
    n = sum(256**i * b for i, b in enumerate(octets))
    length = int(math.ceil(math.log(256**len(octets), 36)))
    d = ''
    for _ in range(length):
        n, r = divmod(n, 36)
        d += digits[r]
    return d

digits = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'


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
        with open(path + '.log', 'r', encoding='utf8') as f:
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
                    zipped=False, plot_email=None,
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
            _create_resource(target_fn, resources, renaming)
        except Exception:
            logging.exception('Resource creation failed:')
            if log_exception:
                with open(target_fn + '.log', 'w', encoding='utf8') as f:
                    f.write(
                        'An internal error occurred. '
                        'Please inform the webmaster at info@ontogene.org '
                        'about this, indicating this error code: {}'
                        .format(int(time.time())))
                return
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
        with open(pending_fn, 'a', encoding='utf8') as f:
            f.write('{} {}\n'.format(plot_email, target_fn))

    # Remove old, unused files.
    clean_up_dir(DOWNLOADDIR, None)

    return r_value


def _create_resource(target_fn, rsc, renaming):
    '''
    Run the Bio Term Hub resource creation pipeline.
    '''
    rsc.write_all(target_fn + '.tmp', mapping=renaming)
    os.rename(target_fn + '.tmp', target_fn)


if __name__ == '__main__':
    main()
