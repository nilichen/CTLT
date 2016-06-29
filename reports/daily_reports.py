import json
import urllib2
import datetime
import pandas as pd
from bs4 import BeautifulSoup
import re
import csv

import os
import httplib2
from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools
import base64
import email


SCOPES = 'https://www.googleapis.com/auth/gmail.readonly'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Gmail API Python Quickstart'


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'gmail-python-quickstart.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print 'Storing credentials to ' + credential_path
    return credentials

def get_content(date):
    tomorrow = date + datetime.timedelta(days=1)
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('gmail', 'v1', http=http)
    content = ''
    messages = service.users().messages()\
    .list(userId='me', q="from:news@edx.org after:%s before:%s" 
          % (date.strftime('%Y/%m/%d'), tomorrow.strftime('%Y/%m/%d'))).execute().get('messages', [])
    if messages:
        for message in messages:
            tdata = service.users().messages().get(userId='me', id=message['id'], format='raw').execute()
            msg_str = base64.urlsafe_b64decode(tdata['raw'].encode('ASCII'))
            mime_msg = email.message_from_string(msg_str)
            for part in mime_msg.walk():
                # each part is a either non-multipart, or another multipart message
                # that contains further parts... Message is organized like a tree
                if part.get("content-transfer-encoding"):
                    content = part.get_payload(decode=True) 
    return content

# content = get_content()

def inEmail(content, org='UBCx'):
    email = {}
    if content:
        pos = 1
        soup = BeautifulSoup(content)
        tds = soup.find_all('td', class_='mcnTextContent')
        for td in tds:
            if td.find('span'):
                if td.find('em'):
                    univ = td.find('em').get_text()
                    # print('span', pos, univ)
                    if org in univ:
                        email[pos] = td.find('span').get_text()
                    pos += 1

            elif td.find('a'):
                course = td.find('a').get_text()
                if '-' in course:
                    if org in course:
                        email[pos] = re.match('(.+)\s-\s.+', course).groups()[0]
                    # print('a', pos, course)
                    pos += 1
    return email


def featured(org='UBCx'):    
    feature = {}
    r = urllib2.urlopen('https://www.edx.org/course').read()
    soup = BeautifulSoup(r, 'html.parser')
    courses = soup.find_all('div', class_="course-card verified ")
    pos = 1
    for course in courses:
        if course.find(class_='label').get_text() == org:
            feature[pos] = course.find(class_='title').get_text().strip()
        pos += 1
    return feature


def onHomepage(org='UBCx'):
    homepage = {}
    url = 'https://www.edx.org/api/discovery/v1/cards?limit=12&tags=14e5ea80-f725-4cd6-82f6-6d2bd63d5159'
    data = json.loads(urllib2.urlopen(url).read())
    pos = 1
    for entry in data:
        if entry['organizations'][0]['display_name'] == org:
            homepage[pos] = entry['title'].strip()
        pos += 1
    return homepage


def appendDFToCSV(df, date, csvFilePath, daily=False):
    data = [date.strftime('%Y-%m-%d')]
    if daily:
        homepage = onHomepage()
        feature = featured()
        content = get_content(date)
        emaillist = inEmail(content)

        promote = ''
        for k, v in homepage.iteritems():
            promote += 'Homepage %s: %s; ' % (k, v)
        for k, v in emaillist.iteritems():
            promote += 'Email %s: %s; ' % (k, v)
        for k, v in feature.iteritems():
            promote += 'Feature %s: %s; ' % (k, v)
        data += [promote]
        
    for i in range(len(df)):
        data += map(str, df.ix[i, :].values)

    if not os.path.isfile(csvFilePath):
        header1 = ['']
        if daily:
            header1 += ['']
        for i in range(len(df)):
            header1 += [df.index[i]] 
            header1 += [''] * (df.shape[1]-1)
        header2 = ['Date']
        if daily:
            header2 += ['Promote']
        for i in range(len(df)):
            header2 += list(df.columns)[:]
            
        with open(csvFilePath, 'a') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(header1)
            writer.writerow(header2)
            writer.writerow(data)
    else:
        with open(csvFilePath, 'a') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(data)


course_list = [
            'UBCx__Marketing1x__3T2015',        
            'UBCx__Climate1x__2T2016',
            'UBCx__SPD1x__2T2016',
            'UBCx__SPD2x__2T2016',
            'UBCx__SPD3x__2T2016',
            'UBCx__UseGen_1x__1T2016',
            'UBCx__UseGen_2x__1T2016'
]

today = datetime.date.today()
# yesterday = today - datetime.timedelta(days=1)


def enroll_unenroll_verify(course_list=course_list, date=today, filepath='data/enroll_unenroll_verify_yd.csv'):
    """Daily report for number of students who enrolled, unenrolled and verified for the last week"""
    
#     def promote(course_id):
#         course_no = re.match('UBCx\/(.+)\/', course_id).groups()[0]
#         content = ''
#         if course_no in homepage.keys():
#             content += 'Homepage: %s ' % homepage[course_no]
#         if course_no in feature.keys():
#             content += 'Feature: %s ' % feature[course_no]
#         return content
    yesterday = date - datetime.timedelta(days=1)
    enroll_tables = ',\n'.join(['[%s.enrollment_events]' % x for x in course_list])
    verify_tables = ',\n'.join(['[%s.person_enrollment_verified]' % x for x in course_list])
    pc_tables = ',\n'.join(['[%s.person_course]' % x for x in course_list])

    # query enroll, unenroll and verify data from bigquery
    query = \
    """SELECT course_id, Date(time) As date, count(*) As num 
    FROM {0} 
    Where Date(time) = '{1}' And activated=1 
    Group By course_id, date 
    Order by course_id, date""".format(enroll_tables, yesterday.strftime('%Y-%m-%d'))
    enroll = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')

    query = \
    """SELECT course_id, Date(time) As date, count(*) As num 
    FROM {0} 
    Where Date(time) = '{1}' And deactivated=1 
    Group By course_id, date 
    Order by course_id, date""".format(enroll_tables, yesterday.strftime('%Y-%m-%d'))
    unenroll = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')

    verify_tables = ',\n'.join(['[%s.person_enrollment_verified]' % x for x in course_list])
    query = \
    """SELECT course_id, Date(verified_enroll_time) As date, Count(*) As num 
    FROM {0}
    Where Date(verified_enroll_time) = '{1}'
    Group By course_id, date 
    Order by course_id, date""".format(verify_tables, yesterday.strftime('%Y-%m-%d'))
    verify = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')
    # for course in course_list:
    #     course = course.replace('__', '/').replace('_', '.')
    #     if course not in verify.course_id.values:
    #         verify.loc[len(verify)] = [course, week_ago.strftime('%Y-%m-%d'), 0]

    enroll['type'] = 'enroll'
    unenroll['type'] = 'unenroll'
    verify['type'] = 'verify'
    overall = pd.pivot_table(pd.concat([enroll, unenroll, verify]), index='course_id',
                             columns=['type'], values='num').fillna(0)


    overall[''] = ''
    print overall
    # filepath = '/Users/katrinani/Google Drive/Data scripts/enroll_unenroll_verify.csv'
    appendDFToCSV(overall, date, filepath, daily=True)




if __name__=="__main__":

    # homepage_courses(filepath='/Users/katrinani/Google Drive/Data scripts/homepage_courses.txt')
    enroll_unenroll_verify(filepath='/Users/katrinani/Google Drive/Data scripts/enroll_unenroll_verify_yd.csv')
