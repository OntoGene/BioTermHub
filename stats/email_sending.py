# Adapted from:
# http://stackoverflow.com/a/3363254/5272432

import smtpd
import asyncore
from threading import Thread

import smtplib
from os import listdir, remove
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

class ConnectionSet:
    def __init__(self, address, server, port, user, pwd):
        self.address = address
        self.server = server
        self.port = port
        self.user = user
        self.pwd = pwd

def send_mail(send_from, send_to, subject, text, connection, files=None):
    assert isinstance(send_to, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = Date=formatdate(localtime=True)
    msg['Subject'] = subject
    
    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            msg.attach(MIMEApplication(
                fil.read(),
                Content_Disposition='attachment; filename="%s"' % basename(f),
                Name=basename(f)
            ))

    server = smtplib.SMTP(connection.server, connection.port)
    server.ehlo()
    server.starttls()
    server.login(connection.user, connection.pwd)
    server.sendmail(send_from, send_to, msg.as_string())
    server.close()
