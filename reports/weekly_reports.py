from __future__ import division
import pandas as pd
import datetime 
import os
from daily_reports import appendToExcel

# currently running courses
course_list = [
            'UBCx__Marketing1x__3T2015',        
            # 'UBCx__Climate1x__2T2016',
            'UBCx__SPD1x__2T2016',
            'UBCx__SPD2x__2T2016',
            'UBCx__SPD3x__2T2016',
            'UBCx__UseGen_1x__1T2016',
            'UBCx__UseGen_2x__1T2016'
]
# verification prices
prices = {            
        'UBCx__Marketing1x__3T2015': 49,        
        # 'UBCx__Climate1x__2T2016': 50,
        'UBCx__SPD1x__2T2016': 49,
        'UBCx__SPD2x__2T2016': 49,
        'UBCx__SPD3x__2T2016': 49,
        'UBCx__UseGen_1x__1T2016': 49,
        'UBCx__UseGen_2x__1T2016': 49
}

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
# week_ago = today - datetime.timedelta(days=7)




def activity_lastweek(course_list=course_list, dates=[yesterday], filepath='data/activity_lastweek.xlsx'):
    """
    Activity for the last week including number of students active, nevents, 
    nvideo_viewed, nproblem_attempted, nforum_posts;
    Data is updated in bigquery once a week on Monday morning, 
    run the command on Monday afternoon => activity for the last week;
    Nproblem_attempts for useGen.1x and useGen.2x need recalculate 
    due to different implementation of course structure.
    """
   
    for course_id in course_list:
        dfs = []
        if 'UseGen' in course_id:
            query = """
            SELECT '{2}' As Date, Count(Distinct username) As nactive, sum(nvideos_viewed) As nvideo_views, 
            sum(nproblems_attempted) As nproblem_attempts, sum(nforum_posts) As nforum_posts
            From
            (Select pcd.date As date, pcd.username As username, pi.n_attempts As nproblems_attempted, 
            pcd.nvideos_viewed As nvideos_viewed, pcd.nforum_posts As nforum_posts
            FROM [{0}.person_course_day] pcd
            Join [{0}.user_info_combo] uic
            On pcd.username = uic.username
            Left Join 
            (Select Date(date) As date, user_id, Sum(n_attempts) As n_attempts
            From [{0}.person_item]
            Where Date(date) Between '{1}' And '{2}'
            Group By date, user_id) pi
            On pi.user_id = uic.user_id And pcd.date = pi.date
            Where pcd.date Between '{1}' And '{2}')
            """
        else:
            query = """
            SELECT '{2}' As Date, Count(Distinct username) As nactive, sum(nvideos_viewed) As nvideo_views, 
            sum(nproblems_attempted) As nproblem_attempts, sum(nforum_posts) As nforum_posts 
            FROM [{0}.person_course_day]
            Where date>='{1}' And date<='{2}'
            """
            
        for date in dates:
            week_ago = date - datetime.timedelta(days=6)
            dfs.append(pd.io.gbq.read_gbq(query.format(course_id, week_ago.strftime('%Y-%m-%d'), date.strftime('%Y-%m-%d')),
                                          project_id='ubcxdata', verbose=False, private_key='ubcxdata.json'))
        activity = pd.concat(dfs)
        activity.Date = pd.to_datetime(activity.Date, format='%Y-%m-%d').dt.date
        # print activity
        appendToExcel(course_id, activity.fillna(0), filepath)


def uptodate(course_list=course_list, prices=prices, dates=[yesterday], filepath='data/register_verify_revenue_utd.xlsx'):
    """
    Up-to-date (last Sunday) information about number of students registered, verifed, % verified, revenue;
    Data (this Monday not included) is updated in bigquery once a week on Monday morning, 
    run the command on Monday afternoon.
    """
    for course_id in course_list:
        dfs = []
        for date in dates:
            query = """
            Select '{1}' As Date, count(*) As nregistered, 
            Sum(Case When enrollment_mode='verified' Then 1 Else 0 End) As nverified
            From [{0}.user_info_combo]
            Where Date(enrollment_created) <= '{1}'""".format(course_id, date.strftime('%Y-%m-%d'))
            dfs.append(pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json'))
        uptodate = pd.concat(dfs)
        uptodate['pct_verified'] = uptodate.nverified / uptodate.nregistered
        uptodate['revenue_todate'] = prices[course_id] * uptodate.nverified
        appendToExcel(course_id, uptodate, filepath)




if __name__ == "__main__":

    # print "Number of students who enrolled, unenrolled and verified during the last week:"
    # enroll_unenroll_verify()
    # print "Number of students active, nevents, nvideo_viewed, nproblem_attempted and nforum_posts during last week:"
    activity_lastweek(filepath='/Users/katrinani/Google Drive/Data scripts/activity_lastweek.csv')
    # print "Number of students registered, verifed, pct_verified and revenue up-to-date"
    uptodate(filepath='/Users/katrinani/Google Drive/Data scripts/register_verify_revenue_utd.csv')



