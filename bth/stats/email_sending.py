#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015


'''
Send an email with attachments.
'''


# Adapted from:
# http://stackoverflow.com/a/3363254/5272432
# http://stackoverflow.com/questions/73781


import smtplib
from os.path import basename
from collections import namedtuple
from subprocess import Popen, PIPE
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


ConnectionSet = namedtuple('ConnectionSet',
                           'address server port user pwd')


def send_mail(send_from, send_to, connection, **msg_contents):
    '''
    Send an email with attachments.
    '''
    if not isinstance(send_to, list):
        raise TypeError('Argument `send_to`: expected list, got %s' %
                        type(send_to))

    msg = compose_message(send_from, send_to, **msg_contents)

    if connection.server:
        # Use a remote SMTP server.
        send_with_smtplib(msg, send_from, send_to, connection)
    else:
        # Use Unix `sendmail`.
        send_with_ssmtp(msg)


def compose_message(send_from, send_to, subject='', text='', files=()):
    '''
    Generate a properly formatted MIME message.
    '''
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            msg.attach(MIMEApplication(
                fil.read(),
                Content_Disposition='attachment; filename="%s"' % basename(f),
                Name=basename(f)
            ))

    return msg.as_string()


def send_with_ssmtp(msg):
    '''
    Use Unix' sendmail/ssmtp command for sending.
    '''
    p = Popen(['sendmail', '-t', '-oi'], stdin=PIPE)
    p.communicate(msg.encode('utf8'))


def send_with_smtplib(msg, send_from, send_to, connection):
    '''
    Login to an SMTP server for sending.
    '''
    server = smtplib.SMTP(connection.server, connection.port)
    server.ehlo()
    server.starttls()
    server.login(connection.user, connection.pwd)
    server.sendmail(send_from, send_to, msg)
    server.close()
