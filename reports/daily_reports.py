import json
import urllib2
import datetime
import pandas as pd
from bs4 import BeautifulSoup
import re
from weekly_reports import appendDFToCSV

def featured(org='UBCx'):    
    feature = {}
    r = urllib2.urlopen('https://www.edx.org/course').read()
    soup = BeautifulSoup(r, 'html.parser')
    courses = soup.find_all('div', class_="course-card verified ")
    pos = 1
    for course in courses:
        if course.find(class_='label').get_text() == org:
            feature[course.find(class_='course-code').get_text().strip()] = pos
        pos += 1
    return feature

def onHomepage(org='UBCx'):
    homepage = {}
    url = 'https://www.edx.org/api/discovery/v1/cards?limit=12&tags=14e5ea80-f725-4cd6-82f6-6d2bd63d5159'
    data = json.loads(urllib2.urlopen(url).read())
    pos = 1
    for entry in data:
        if entry['organizations'][0]['display_name'] == org:
            if entry['card_type'] == 'course':
                homepage[entry['attributes']['course_number']] = pos
            else:
                homepage['SPD1x'] = pos
                homepage['SPD2x'] = pos
                homepage['SPD3x'] = pos
        pos += 1
    return homepage


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
yesterday = today - datetime.timedelta(days=1)
homepage = onHomepage()
feature = featured()

def enroll_unenroll_verify(course_list=course_list, date=yesterday, filepath='enroll_unenroll_verify.csv'):
    """Daily report for number of students who enrolled, unenrolled and verified for the last week"""

    def promote(course_id):
        course_no = re.match('UBCx\/(.+)\/', course_id).groups()[0]
        content = ''
        if course_no in homepage.keys():
            content += 'Homepage: %s ' % homepage[course_no]
        if course_no in feature.keys():
            content += 'Feature: %s ' % feature[course_no]
        return content

    enroll_tables = ',\n'.join(['[%s.enrollment_events]' % x for x in course_list])
    verify_tables = ',\n'.join(['[%s.person_enrollment_verified]' % x for x in course_list])
    pc_tables = ',\n'.join(['[%s.person_course]' % x for x in course_list])

    # query enroll, unenroll and verify data from bigquery
    query = \
    """SELECT course_id, Date(time) As date, count(*) As num 
    FROM {0} 
    Where Date(time) = '{1}' And activated=1 
    Group By course_id, date 
    Order by course_id, date""".format(enroll_tables, date.strftime('%Y-%m-%d'))
    enroll = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')

    query = \
    """SELECT course_id, Date(time) As date, count(*) As num 
    FROM {0} 
    Where Date(time) = '{1}' And deactivated=1 
    Group By course_id, date 
    Order by course_id, date""".format(enroll_tables, date.strftime('%Y-%m-%d'))
    unenroll = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')

    verify_tables = ',\n'.join(['[%s.person_enrollment_verified]' % x for x in course_list])
    query = \
    """SELECT course_id, Date(verified_enroll_time) As date, Count(*) As num 
    FROM {0}
    Where Date(verified_enroll_time) = '{1}'
    Group By course_id, date 
    Order by course_id, date""".format(verify_tables, date.strftime('%Y-%m-%d'))
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


    overall[''] = overall.index.map(lambda x: promote(x))
    print overall
    # filepath = '/Users/katrinani/Google Drive/Data scripts/enroll_unenroll_verify.csv'
    appendDFToCSV(overall, date, filepath)


# def homepage_courses(filepath='homepage_courses.txt'):
#     """
#     Date, position and title of any UBC course on that day;
#     If no UBC course on the homepage, then print 'No courses from UBC on the homepage today'.
#     """
#     today = datetime.date.today()
#     url = 'https://www.edx.org/api/discovery/v1/cards?limit=12&tags=14e5ea80-f725-4cd6-82f6-6d2bd63d5159'
#     data = json.loads(urllib2.urlopen(url).read())
#     with open(filepath, 'a+') as f:
#         f.write(str(today) + '\n')
#         pos = 1
#         ind = True
#         for entry in data:
#             if entry['organizations'][0]['display_name'] == "UBCx":
#                 ind = False
#                 print 'Pos %s: ' % (pos) + entry['title'].strip()
#                 f.write('Pos %s: ' % (pos) + entry['title'].strip().encode('utf8') + '\n')
#             pos += 1
#         if ind:
#             print 'No courses from UBC on the homepage today'
#             f.write('No courses from UBC on the homepage today\n')
#         f.write('\n')



if __name__=="__main__":

    # homepage_courses(filepath='/Users/katrinani/Google Drive/Data scripts/homepage_courses.txt')
    enroll_unenroll_verify(filepath='/Users/katrinani/Google Drive/Data scripts/enroll_unenroll_verify.csv')
