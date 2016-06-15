#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015
# Modified: Lenz Furrer, 2016


import os
import logging

from termhub.core.settings import path_stats, path_batch
from termhub.stats.statplot_poststats import plotstats
from termhub.stats.email_sending import ConnectionSet, send_mail

PENDING = os.path.join(path_batch, "pending")


def main():
    '''
    Run as script: Create stats graphics and send them by email.
    '''
    try:
        with open(PENDING, 'r', encoding='utf8') as f:
            pending = list(f)
    except IOError:
        logging.info("No pending requests, exiting ...")
        return

    with open(os.path.join(path_batch, 'config')) as f:
        conf_list = f.read().split(",")
        cs = ConnectionSet(*conf_list)

    for request in pending:
        if not request.strip():
            # Skip empty lines.
            continue
        recipient, export = request.split()

        # clear out STATPATH.
        for fn in os.listdir(path_stats):
            os.remove(os.path.join(path_stats, fn))

        # Trigger plotstats, which triggers statistics computation
        plotstats(export)

        # Compile list of files to be sent
        files = [os.path.abspath(os.path.join(path_stats, fn))
                 for fn in os.listdir(path_stats)]

        logging.info("Sending message ...")

        send_mail(cs.address,
                  [recipient],
                  cs,
                  subject='OntoGene Bio Term Hub Statistics',
                  text=message,
                  files=files)

    os.remove(PENDING)


message = '''Hello,
You have requested statistics plots from the OntoGene Bio Term Hub.
Please find them attached.
'''


if __name__ == "__main__":
    main()
