__author__ = 'chuqiao'


import smtplib
import base64
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText



def viewlog(file):
    file = open("syncsolr.log")
    file.seek(0,2)# Go to the end of the file

    while True:
        line = file.readline()

        if "***Finished synchronizing***" in line:
            mailUpdate()
        elif "***Synchronize failed***" in line:
            mailAlert()


def mailUpdate():
    fromaddr = 'bioeventsportal@gmail.com'
    toaddr = 'info@bioevents-portal.org'

    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "Synchronize two SOLRS"

    body = '''Finished synchronizing two solrs
           '''
    msg.attach(MIMEText(body, 'plain'))
    username = 'gongchuqiao'
    password = base64.b64decode('YmlvZXZlbnRzMzIx')
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(username, password)

    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()

def mailAlert():
    fromaddr = 'bioeventsportal@gmail.com'
    toaddr = 'info@bioevents-portal.org'

    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "Synchronize two SOLRS failed"

    body = '''Failed to synchronize two SOLRS
           '''
    msg.attach(MIMEText(body, 'plain'))
    username = 'gongchuqiao'
    password = base64.b64decode('YmlvZXZlbnRzMzIx')
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(username, password)

    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()



if  __name__ == '__main__':
    viewlog(file)


