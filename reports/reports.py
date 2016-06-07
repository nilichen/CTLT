from __future__ import division
import pandas as pd
import datetime 
import json
import urllib2

# currently running courses
course_list = [
            'UBCx__Marketing1x__3T2015',        
            'UBCx__Climate1x__2T2016',
            'UBCx__SPD1x__2T2016',
            'UBCx__SPD2x__2T2016',
            'UBCx__SPD3x__2T2016',
            'UBCx__UseGen_1x__1T2016',
            'UBCx__UseGen_2x__1T2016'
]
# verification prices
prices = [49, 50, 49, 49, 49, 49, 49]

today = datetime.date.today()
week_ago = today - datetime.timedelta(days=7)

def homepage_courses():
    """Date and a list of popular courses on the homepage on that day"""

    url = 'https://www.edx.org/api/discovery/v1/cards?limit=12&tags=14e5ea80-f725-4cd6-82f6-6d2bd63d5159'
    data = json.loads(urllib2.urlopen(url).read())
    with open('homepage_courses.txt', 'a+') as f:
        f.write(str(today) + '\n')
        for entry in data:
            f.write(entry['title'].strip().encode('utf8') + '\n')
        f.write('\n\n')


def daily_lastweek(course_list=course_list):
    """Daily report for number of students who enrolled, unenrolled and verified for the last week"""

    enroll_tables = ',\n'.join(['[%s.enrollment_events]' % x for x in course_list])
    verify_tables = ',\n'.join(['[%s.person_enrollment_verified]' % x for x in course_list])
    pc_tables = ',\n'.join(['[%s.person_course]' % x for x in course_list])

    # query enroll, unenroll and verify data from bigquery
    query = \
    """SELECT course_id, Date(time) As date, count(*) As num 
    FROM {0} 
    Where Date(time) >= '{1}' And activated=1 
    Group By course_id, date 
    Order by course_id, date""".format(enroll_tables, week_ago.strftime('%Y-%m-%d'))
    enroll = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')

    query = \
    """SELECT course_id, Date(time) As date, count(*) As num 
    FROM {0} 
    Where Date(time) >= '{1}' And deactivated=1 
    Group By course_id, date 
    Order by course_id, date""".format(enroll_tables, week_ago.strftime('%Y-%m-%d'))
    unenroll = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')

    verify_tables = ',\n'.join(['[%s.person_enrollment_verified]' % x for x in course_list])
    query = \
    """SELECT course_id, Date(verified_enroll_time) As date, count(*) As num 
    FROM {0}
    Where Date(verified_enroll_time) >= '{1}'
    Group By course_id, date Order by course_id, date""".format(verify_tables, week_ago.strftime('%Y-%m-%d'))
    verify = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')

    enroll['type'] = 'enroll'
    unenroll['type'] = 'unenroll'
    verify['type'] = 'verify'
    overall = pd.pivot_table(pd.concat([enroll, unenroll, verify]), index=['course_id', 'type'],
                             columns='date', values='num').fillna(0)
    overall['week_total'] = overall.sum(axis=1)

    print overall
    filename = 'daily' + str(today) + '.csv'
    overall.to_csv(filename)



def activity_lastweek(course_list=course_list):
    """
    Activity for the last week including number of students active, nevents, 
    nvideo_viewed, nproblem_attempted, nforum_posts;
    Data is updated in bigquery once a week on Monday morning, 
    run the command on Monday afternoon => activity for the last week;
    Nproblem_attempts for useGen.1x and useGen.2x need recalculate 
    due to different implementation of course structure.
    """
    
    pcd_tables = ',\n'.join(['[%s.person_course_day]' % x for x in course_list])
    query = \
    """SELECT course_id, Count(Distinct username) As nactive, 
    sum(nevents) As nevents, sum(nvideos_viewed) As nvideo_views, 
    sum(nproblems_attempted) As nproblem_attempts, sum(nforum_posts) As nforum_posts 
    FROM {0}
    Where date >= '{1}'
    Group By course_id Order By course_id""".format(pcd_tables, week_ago.strftime('%Y-%m-%d'))
    activity = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')
    activity.set_index('course_id', inplace=True)

    if 'UBCx__UseGen_1x__1T2016' in course_list:
        query = \
        """SELECT sum(n_attempts) 
        FROM [UBCx__UseGen_1x__1T2016.person_item] 
        Where Date(date) >= '{0}'""".format(week_ago.strftime('%Y-%m-%d'))
        value = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, 
                                   private_key='ubcxdata.json').values[0][0]
        activity.ix['UBCx/UseGen.1x/1T2016', 'nproblem_attempts'] = int(value)

    if 'UBCx__UseGen_2x__1T2016' in course_list:
        query = \
        """SELECT sum(n_attempts) 
        FROM [UBCx__UseGen_2x__1T2016.person_item] 
        Where Date(date) >= '{0}'""".format(week_ago.strftime('%Y-%m-%d'))
        value = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, 
                                   private_key='ubcxdata.json').values[0][0]
        activity.ix['UBCx/UseGen.2x/1T2016', 'nproblem_attempts'] = int(value)

    print activity
    filename = 'activity' + str(today) + '.csv'
    activity.to_csv(filename)


def uptodate(course_list=course_list, prices=prices):
    """
    Up-to-date (last Sunday) information about number of students registered, verifed, % verified, revenue;
    Data (this Monday not included) is updated in bigquery once a week on Monday morning, 
    run the command on Monday afternoon.
    """
    
    pc_tables = ',\n'.join(['[%s.person_course]' % x for x in course_list])
    query = \
    """Select course_id, count(*) As nregistered, 
    Sum(Case When mode='verified' Then 1 Else 0 End) As nverified
    From {0}
    Group By course_id Order By course_id""".format(pc_tables)
    verify_todate = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')
    verify_todate['pct_verified'] = verify_todate.nverified / verify_todate.nregistered
    verify_todate['revenue_todate'] = prices * verify_todate.nverified
    verify_todate.set_index('course_id', inplace=True)

    print verify_todate
    filename = 'uptodate' + str(today) + '.csv'
    verify_todate.to_csv(filename)


