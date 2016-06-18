import json
import urllib2
import datetime
import pandas as pd
from weekly_reports import appendDFToCSV

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

def enroll_unenroll_verify(course_list=course_list, date=yesterday, filepath='enroll_unenroll_verify.csv'):
    """Daily report for number of students who enrolled, unenrolled and verified for the last week"""

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

    print overall
    # filepath = '/Users/katrinani/Google Drive/Data scripts/enroll_unenroll_verify.csv'
    appendDFToCSV(overall, date, filepath)


def homepage_courses(filepath='homepage_courses.txt'):
    """
    Date, position and title of any UBC course on that day;
    If no UBC course on the homepage, then print 'No courses from UBC on the homepage today'.
    """
    today = datetime.date.today()
    url = 'https://www.edx.org/api/discovery/v1/cards?limit=12&tags=14e5ea80-f725-4cd6-82f6-6d2bd63d5159'
    data = json.loads(urllib2.urlopen(url).read())
    with open(filepath, 'a+') as f:
        f.write(str(today) + '\n')
        pos = 1
        ind = True
        for entry in data:
            if entry['organizations'][0]['display_name'] == "UBCx":
                ind = False
                print 'Pos %s: ' % (pos) + entry['title'].strip()
                f.write('Pos %s: ' % (pos) + entry['title'].strip().encode('utf8') + '\n')
            pos += 1
        if ind:
            print 'No courses from UBC on the homepage today'
            f.write('No courses from UBC on the homepage today\n')
        f.write('\n')



if __name__=="__main__":

    homepage_courses(filepath='/Users/katrinani/Google Drive/Data scripts/homepage_courses.txt')
    enroll_unenroll_verify(filepath='/Users/katrinani/Google Drive/Data scripts/enroll_unenroll_verify.csv')
