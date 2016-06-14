#!/usr/bin/env python
# coding: utf8

# Author: Adrian van der Lek, 2015
# Modified: Lenz Furrer, 2016


from os import remove, listdir
from os.path import abspath
import logging

from termhub.core.settings import path_stats, path_batch
from termhub.stats.statplot_poststats import plotstats
from termhub.stats.email_sending import ConnectionSet, send_mail
PENDING = path_batch + "pending"


def main():
    '''
    Run as script: Create stats graphics and send them by email.
    '''
    try:
        with open(PENDING, 'r') as pending:
            pending = list(pending)
    except IOError:
        logging.info("No pending requests, exiting ...")
        return

    for request in pending:
        if not request.strip():
            # Skip empty lines.
            continue
        mail, export = request.split()
        export_name = export.rsplit("/")[-1]

        # clear out STATPATH (http://glowingpython.blogspot.ch/2011/04/how-to-delete-all-files-in-directory.html)
        fileList = listdir(path_stats)
        for fileName in fileList:
            remove(path_stats + fileName)

        # Trigger plotstats, which triggers statistics computation
        plotstats(export)

        # Compile list of files to be sent
        files = [abspath(path_stats) + "/" + f_name for f_name in listdir(path_stats)]

        with open(path_batch + 'config') as config:
            conf_list = config.read().split(",")
        cs = ConnectionSet(*conf_list)

        logging.info("Sending message ...")

        send_mail(cs.address, [mail], 'Statistics for %s' % export_name, 'Hello,\nYou have requested statistics plots for %s. Find them attached.' % export_name, cs, files)

    remove(PENDING)


if __name__ == "__main__":
    main()
