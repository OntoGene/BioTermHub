from os import remove, listdir
from os.path import abspath
from email_sending import ConnectionSet, send_mail

from settings import path_stats, path_batch
from statplot_poststats import plotstats
PENDING = path_batch + "pending"

def main():
    try:
        with open(PENDING, 'r') as pending:
            pending = list(pending)
    except IOError:
        print "No pending requests, exiting ..."
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

        print "Sending message ..."

        send_mail(cs.address, [mail], 'Statistics for %s' % export_name, 'Hello,\nYou have requested statistics plots for %s. Find them attached.' % export_name, cs, files)

    remove(PENDING)


if __name__ == "__main__":
    main()
