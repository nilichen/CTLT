from __future__ import division
import pandas as pd
#import numpy as np
#import matplotlib.pyplot as plt
import datetime 

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
	
def daily_lastweek(course_list=course_list):
	# daily report for number of students who enrolled, unenrolled and verified for the last week

	enroll_tables = ',\n'.join(['[%s.enrollment_events]' % x for x in course_list])
	verify_tables = ',\n'.join(['[%s.person_enrollment_verified]' % x for x in course_list])
	pc_tables = ',\n'.join(['[%s.person_course]' % x for x in course_list])


	# query enroll, unenroll and verify data from bigquery
	query = "SELECT course_id, Date(time) As date, count(*) As num FROM " + enroll_tables + " Where Date(time) >= '" +\
	            week_ago.strftime('%Y-%m-%d') + "' And activated=1 Group By course_id, date Order by course_id, date"
	enroll = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')
	#print enroll

	query = "SELECT course_id, Date(time) As date, count(*) As num FROM " + enroll_tables + " Where Date(time) >= '" +\
	            week_ago.strftime('%Y-%m-%d') + "' And deactivated=1 Group By course_id, date Order by course_id, date"
	unenroll = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')

	verify_tables = ',\n'.join(['[%s.person_enrollment_verified]' % x for x in course_list])
	query = "SELECT course_id, Date(verified_enroll_time) As date, count(*) As num FROM " + verify_tables + " Where Date(verified_enroll_time) >= '" +\
	            week_ago.strftime('%Y-%m-%d') + "' Group By course_id, date Order by course_id, date"
	verify = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')

	enroll['type'] = 'enroll'
	unenroll['type'] = 'unenroll'
	verify['type'] = 'verify'
	overall = pd.pivot_table(pd.concat([enroll, unenroll, verify]), index=['course_id', 'type'], columns='date', values='num').fillna(0)
	overall['week_total'] = overall.sum(axis=1)

	#print overall
	filename = 'daily' + str(today) + '.csv'
	overall.to_csv(filename)


def uptodate(course_list=course_list, prices=prices):
	# up-to-date (last Sunday) information about number of students registered, verifed, % verified, revenue 
	# data (this Monday not included) is updated in bigquery once a week on Monday morning, run the command on Monday afternoon
	pc_tables = ',\n'.join(['[%s.person_course]' % x for x in course_list])
	query = """Select course_id, count(*) As nregistered, Sum(Case When mode='verified' Then 1 Else 0 End) As nverified
	            From """ + pc_tables + " Group By course_id Order By course_id"
	verify_todate = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')
	verify_todate['pct_verified'] = verify_todate.nverified / verify_todate.nregistered
	verify_todate['revenue_todate'] = prices * verify_todate.nverified
	verify_todate.set_index('course_id', inplace=True)

	#print verify_todate
	filename = 'uptodate' + str(today) + '.csv'
	verify_todate.to_csv(filename)


def activity_lastweek(course_list=course_list):
	# activity for the last week including number of students active, nevents, nvideo_viewed, nproblem_attempted, nforum_posts
	# data is updated in bigquery once a week on Monday morning, run the command on Monday afternoon => activity for the last week
	pcd_tables = ',\n'.join(['[%s.person_course_day]' % x for x in course_list])
	query = """SELECT course_id, Count(Distinct username) As nactive, sum(nevents) As nevents, sum(nvideos_viewed) As nvideos_viewed, 
	            sum(nproblems_attempted) As nproblems_attempted, sum(nforum_posts) As nforum_posts FROM """ + pcd_tables + \
	            " Where date >= '" +  week_ago.strftime('%Y-%m-%d') + "' Group By course_id Order By course_id"
	activity = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json')
	activity.set_index('course_id', inplace=True)

	#nproblem_attempts for useGen.1x and useGen.2x need recalculate because of their implementation of course structure for problems is different from others
	if 'UBCx__UseGen_1x__1T2016' in course_list:
		query = "SELECT sum(n_attempts) FROM [UBCx__UseGen_1x__1T2016.person_item] Where Date(date) >= '" + \
            week_ago.strftime('%Y-%m-%d') + "'"
		value = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json').values[0][0]
		nactive.ix['UBCx/UseGen.1x/1T2016', 'nproblem_attempts'] = int(value)

	if 'UBCx__UseGen_2x__1T2016' in course_list:
		query = "SELECT sum(n_attempts) FROM [UBCx__UseGen_2x__1T2016.person_item] Where Date(date) >= '" + \
            week_ago.strftime('%Y-%m-%d') + "'"
		value = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False, private_key='ubcxdata.json').values[0][0]
		nactive.ix['UBCx/UseGen.2x/1T2016', 'nproblem_attempts'] = int(value)

	#print verify_todate
	filename = 'activity' + str(today) + '.csv'
	activity.to_csv(filename)
