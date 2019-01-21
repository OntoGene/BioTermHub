#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2015--2019


'''
Web server for the Bio Term Hub.
'''


import sys
import os
import threading
import multiprocessing as mp
import time
import datetime as dt
import shutil
import signal
import logging
import zipfile
import hashlib
import argparse
from pathlib import Path

from lxml import etree
from bottle import Bottle, request, response, static_file, FormsDict

from ..core import settings
from ..core.aggregate import RecordSetContainer
from ..inputfilters import FILTERS
from ..update.fetch_remote import RemoteChecker
from ..stats.bgplotter import BGPlotter
from ..lib.postfilters import RegexFilter
from ..lib.base36gen import Base36Generator
from ..lib.tools import Tempfile


# Config globals.
HOST = settings.server_host
PORT = settings.server_port
LOGFILE = settings.log_file
DOWNLOADDIR = Path(settings.path_download)
HERE = Path(__file__).parent
DL_ROUTE = 'downloads'


# Raise SIGTERM as an exception, so that the child processes can be gracefully
# terminated.
signal.signal(signal.SIGTERM, lambda *_: sys.exit())


# Patch bottle.FormsDict to have useful __str__ method (for use in logging).
FormsDict.__str__ = lambda self: str(list(self.allitems()))


# Some shorthands.
se = etree.SubElement
NBSP = '\xA0'
WAIT_MESSAGE = ('Please wait while the resource is being created '
                '(this may take a few minutes, '
                'depending on the size of the resource).')
REGEXFILTER = RegexFilter().test
with (HERE/'data'/'template.html').open(encoding='utf8') as _f:
    PAGE = _f.read()
    PAGE = PAGE.replace('WAIT_MESSAGE', repr(WAIT_MESSAGE))
    PAGE = PAGE.replace('RESOURCE_NAMES', repr(list(FILTERS)))


def main():
    '''
    Run as script: start the server.
    '''
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument(
        '-i', '--host', metavar='IP', default=HOST,
        help='host IP')
    ap.add_argument(
        '-p', '--port', metavar='N', default=PORT, type=int,
        help='port number')
    ap.add_argument(
        '-d', '--debug', action='store_true',
        help='display exceptions in the served responses')
    ap.add_argument(
        '-v', '--verbosity', nargs='?', default='INFO', const='DEBUG',
        metavar='LEVEL',
        help='verbosity level (DEBUG/INFO/WARNING)')
    args = vars(ap.parse_args())

    # Set up the logger.
    logging.basicConfig(
        level=args.pop('verbosity'),
        filename=LOGFILE,
        format='%(process)d - %(asctime)s - %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    run(args)


def run(bottle_args):
    '''
    Run a BTH server forever.
    '''
    app = Bottle()
    manager = AsyncManager(len(FILTERS)+1)

    _api(app, manager)

    try:
        app.run(**bottle_args)
    finally:
        manager.destroy()


def _api(app, manager):
    @app.get('/')
    @app.post('/')
    def _page():
        if request.params:
            logging.debug('JS-free termlist polling')
            msg, status = manager.termlist_request(request.params)
            page = jsfree_polling(msg, status, request.params.get('zipped'))
        else:
            logging.info('Serve input page')
            page = input_page()
        return serialise(page, xml_declaration=True, doctype='<!doctype html>')

    @app.post('/termlist')
    def _termlist():
        logging.debug('Termlist request: %s', request.params)
        msg, status = manager.termlist_request(request.params)
        return serialise(msg, fmt='xml', status=status)

    @app.get('/{}/<path:path>'.format(DL_ROUTE))
    def _download(path):
        logging.debug('Serve static file: %s', path)
        return static_file(path, root=str(DOWNLOADDIR))

    @app.get('/check/<name>')
    def _check(name):
        logging.debug('Check request: %s', name)
        msg, status = check_request(name)
        return serialise(msg, status=status)

    @app.get('/update/<name>')
    def _update(name):
        logging.info('Update %s cache', name)
        manager.update_request(name)
        msg = etree.Element('p')
        msg.text = 'Running update...'
        return serialise(msg, status='202 Accepted')

    @app.get('/statplot/<job_id>')
    def _statplot(job_id):
        logging.debug('Statplot request: %s', job_id)
        msg, status = stat_plot_request(job_id)
        return serialise(msg, status=status)

    # Maintenance API (unprotected).

    @app.post('/maintenance/clearcache')
    def _clearcache():
        deleted = clean_up_dir(DOWNLOADDIR, clear_all=True)
        return {'deleted_files': deleted}


class AsyncManager:
    '''
    Delegate requests to background processes.
    '''
    def __init__(self, workers):
        self.pool = mp.Pool(workers)

    def destroy(self):
        '''Terminate all workers.'''
        self.pool.terminate()
        self.pool.join()

    def termlist_request(self, params):
        '''Get an aggregated termlist.'''
        return termlist_request(params, self._start_resource_creation)

    def _start_resource_creation(self, params):
        self.pool.apply_async(create_resource, params)

    def update_request(self, name):
        '''Update a resource from remote.'''
        self.pool.apply_async(update_request, (name,))


def termlist_request(params, callback):
    '''
    Respond to a creation/download request.
    '''
    resources = params.getlist('resources')
    job_id = params.get('job_id')
    zipped = params.get('zipped')
    just_started = False

    if job_id is None:
        # A creation request has been submitted.
        logging.info('New creation request')
        rsc = RecordSetContainer(
            resources=resources,
            flags=params.getlist('flags'),
            mapping=parse_renaming(params),
            postfilter=params.get('postfilter') and REGEXFILTER)
        job_id = job_hash(rsc)

        plot_stats = params.get('plot-stats')
        log_exception = True
        params = (rsc, zipped, plot_stats, job_id, log_exception)
        callback(params)  # start the aggregation job
        just_started = True

    return handle_download_request(job_id, zipped, just_started)


def check_request(name):
    '''
    Check the last-modified date.
    '''
    # Check request only works with AJAX.
    remote = RemoteChecker(name)
    msg = etree.Element('p')
    status = '200 OK'
    if remote.stat.concurrent_update():
        msg.text = 'Concurrent update...'
        status = '202 Accepted'
    elif remote.has_changed():
        msg.text = 'Update available.'
    else:
        msg.text = 'Up-to-date.'
    return msg, status


def update_request(name):
    '''
    Update a resource from remote.
    '''
    # Update request also only works with AJAX.
    remote = RemoteChecker(name)
    try:
        remote.update()
    except RuntimeError as e:
        if e.args != ('Concurrent update in progress'):
            raise


def stat_plot_request(job_id):
    '''
    Create img nodes for term-statistics plots.
    '''
    div = etree.Element('div')  # container
    index = DOWNLOADDIR / job_id / 'index.log'
    if index.exists():
        status = '200 OK'
        se(div, 'hr')
        with index.open(encoding='utf8') as f:
            for line in f:
                # Create <img> nodes for all plots.
                fn = line.rstrip()
                src = '/'.join((DL_ROUTE, job_id, fn))
                se(div, 'img', src=src, onerror='poll_plot_image(this, 1000)')
    elif index.parent.exists():
        status = '202 Accepted'
    else:
        status = '404 Not Found'
    return div, status


def jsfree_polling(msg, status, zipped):
    '''
    Complete page with results for JS-free creation requests.
    '''
    html = response_page()
    if status == '202 Accepted':
        job_id = msg.text
        msg.text = WAIT_MESSAGE
        # Add auto-refresh to the page.
        link = '.?job_id={}'.format(job_id)
        if zipped:
            link += '&zipped=true'
        se(html.find('head'), 'meta',
           {'http-equiv': "refresh",
            'content': "5; url={}".format(link)})
    html.find('.//*[@id="div-result"]').append(msg)
    return html


def serialise(node, status=200, fmt='html', **kwargs):
    '''
    Serialise HTML or XML and set the content-type header.
    '''
    output = etree.tostring(node, method=fmt, encoding='UTF-8', **kwargs)
    response.content_type = 'text/{}; charset=UTF8'.format(fmt)
    response.status = status
    return output


def input_page():
    '''
    Page with the input forms.
    '''
    html = etree.HTML(PAGE)
    html.find('.//div[@id="div-download-page"]').set('class', 'hidden')
    html.find('.//form[@id="form-oger"]').set('action', settings.oger_url)
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
        se(row, 'td').append(source_link(remote.resource.source_ref))
        se(se(se(row, 'td'), 'div', id='div-update-{}'.format(remote.name)),
           'p').text = last_modified
        se(row, 'td').append(update_button(remote.name))
    # Move the instructions to the last column, spanning all rows.
    instructions = doc.find('.//td[@id="cell-instructions"]')
    instructions.set('rowspan', str(len(resources)))
    tbl[1].append(instructions)
    tbl.remove(tbl[0])  # remove the (now empty) first row.
    # Add a checkbox for the CTD-lookup flag.
    labels = ('skip CTD entries that are MeSH duplicates',
              '(has no effect unless both CTD and MeSH are selected)')
    checkbox_par(tbl.getparent(), labels, name='flags', value='ctd_lookup')
    # Add another checkbox for removing short terms.
    label = 'remove very short terms (1 or 2 characters) and plain numbers'
    checkbox_par(tbl.getparent(), [label],
                 name='postfilter', value='true', checked='checked')


def source_link(href):
    '''
    Create a link to a source's reference website.
    '''
    p = etree.XML('<p>(→ <a>source</a>)</p>')
    p[0].set('href', href)
    return p


def update_button(id_):
    '''
    Create an AJAX request button for updating a resource.
    '''
    return etree.Element('input', type='button', value='...',
                         id='btn-update-{}'.format(id_),
                         disabled='disabled')


def checkbox_par(parent, labels, name, value, **kwargs):
    '''
    Append a <p> with a labeled checkbox.
    '''
    par = se(parent, 'p')
    atts = dict(type='checkbox', name=name, value=value, **kwargs)
    se(par, 'input', atts).tail = NBSP + labels[0]
    for tail in labels[1:]:
        se(par, 'br').tail = tail


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


def clean_up_dir(dirpath: Path, clear_all=False):
    '''
    Remove old (or all) files under this directory.
    '''
    fns = dirpath.glob('*')
    if clear_all:
        # Maintenace call: clear the downloads directory.
        del_fns = list(fns)
    else:
        # Automatic clean-up of files older than 35 days.
        deadline = time.time() - 3024000  # 35 * 24 * 3600
        del_fns = [fn for fn in fns if fn.stat().st_mtime < deadline]
    for fn in del_fns:
        try:
            fn.unlink()
        except IsADirectoryError:
            shutil.rmtree(str(fn))
    return list(map(str, del_fns))


def parse_renaming(params):
    '''
    Get any user-specified renaming entries.
    '''
    m = {}
    for level in ('resource', 'entity_type'):
        m[level] = {}
        entries = [params.get('{}-{}'.format(level, n), '').split('\n')
                   for n in ('std', 'custom')]
        for std, custom in zip(*entries):
            if std and custom:
                m[level][std.strip()] = custom.strip()
    return m


def job_hash(rsc):
    '''
    Create a hash value considering all options for this job.
    '''
    key = hashlib.sha1()
    for name, rec, _ in rsc.resources:
        # Update with the resource selection.
        key.update(name.encode('utf8'))
        # For each dump file, add the last-modified time to the hash.
        for path in rec.dump_fns():
            # Update with the timestamps (whole-second precision is enough).
            key.update(str(int(os.path.getmtime(path))).encode('utf8'))
    # Update with the "skip" flag ("ctd_lookup") and the postfilter flag.
    for flag in rsc.flags:
        key.update(flag.encode('utf8'))
    if rsc.params.get('postfilter') is not None:
        key.update(b'postfilter')
    # Update with any renaming rules.
    for level in sorted(rsc.params.get('mapping', ())):
        for entry in sorted(rsc.params['mapping'][level].items()):
            for e in entry:
                key.update(e.encode('utf8'))
    # Convert the hash digest to base 36.
    n = int.from_bytes(key.digest(), 'little')
    return Base36Generator.int2b36(n, big_endian=False)


def handle_download_request(job_id, zipped, just_started):
    '''
    Check if the CSV is ready yet, or if an error occurred.
    '''
    msg = etree.Element('p')
    status = '200 OK'
    path = termlist_path(job_id)
    zpath = zipname(path)
    lpath = logname(path)
    tpath = Tempfile(path).tmp
    if zipped and zpath.exists():
        success_msg(msg, zpath)
    elif not zipped and path.exists():
        success_msg(msg, path)
    elif lpath.exists():
        with lpath.open('r', encoding='utf8') as f:
            msg.text = 'Runtime error: {}'.format(f.read())
        status = '500 Internal Server Error'
    elif tpath.exists() or just_started:
        msg.text = job_id
        status = '202 Accepted'
    else:
        msg.text = 'Cannot find the requested resource.'
        status = '404 Not Found'
    return msg, status


def success_msg(msg, path):
    '''
    Create a download link.
    '''
    msg.text = 'Download resource: '
    link = se(msg, 'a', href='/'.join((DL_ROUTE, path.name)))
    link.text = path.name
    link.tail = size_fmt(path.stat().st_size, ' ({:.1f} {}B)')


def size_fmt(num, fmt='{:.1f} {}B'):
    '''
    Convert file size to a human-readable format.
    '''
    for prefix in ('', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi'):
        if abs(num) < 1024:
            return fmt.format(num, prefix)
        num /= 1024
    return fmt.format(num, 'Yi')


def termlist_path(job_id: str) -> Path:
    '''
    Canonical path to the aggregated termlist.
    '''
    return DOWNLOADDIR / (job_id + '.csv')


def zipname(path: Path) -> Path:
    '''Replace the suffix with ".zip". '''
    return path.with_suffix('.zip')


def logname(path: Path) -> Path:
    '''Add a ".log" suffix.'''
    return path.with_suffix(path.suffix + '.log')


def create_resource(resources,
                    zipped=False, plot_stats=False,
                    job_id=None, log_exception=False):
    '''
    Run the Bio Term Hub resource creation pipeline, if necessary.
    '''
    if job_id is None:
        job_id = job_hash(resources)
    target_fn = termlist_path(job_id)

    stats = None
    if plot_stats:
        plot_dir = target_fn.with_suffix('')
        try:
            plot_dir.mkdir()
        except FileExistsError:
            # The plots exist already. Update mtime, but don't recreate.
            plot_dir.touch()
        else:
            stats = BGPlotter(plot_dir, proc_type=threading.Thread)

    # Check if we really have to create this resource
    # (it might already exist from an earlier job).
    if target_fn.exists():
        # Touch this file to keep it from being cleaned away.
        target_fn.touch()
        if stats:
            stats.from_disk(target_fn)
    else:
        try:
            with Tempfile(target_fn) as tmp:
                resources.write_tsv(str(tmp), stats=stats)
        except Exception:
            logging.exception('Resource creation failed:')
            if log_exception:
                with logname(target_fn).open('w', encoding='utf8') as f:
                    f.write(
                        'An internal error occurred. '
                        'Please inform the webmaster at info@ontogene.org '
                        'about this, indicating this error code: {}'
                        .format(int(time.time())))
                return
            raise

    if zipped:
        zipfn = zipname(target_fn)
        if zipfn.exists():
            zipfn.touch()
        else:
            with zipfile.ZipFile(str(zipfn), 'w', zipfile.ZIP_DEFLATED) as z:
                z.write(str(target_fn), target_fn.name)

    # List all expected plot files in a log file.
    if stats:
        log = plot_dir / 'index.log'
        with log.open('w', encoding='utf8') as f:
            for fn in stats.destinations:
                f.write(fn.name)
                f.write('\n')
            # Add the legend to the list.
            f.write('plot-legend.png\n')
        # Place a copy of the plot legend in plot_dir.
        src = HERE / 'data' / 'plot-legend.png'
        shutil.copy(str(src), str(plot_dir))
        stats.join()

    # Remove old, unused files.
    clean_up_dir(DOWNLOADDIR)

    return
