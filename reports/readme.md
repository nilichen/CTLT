## How to use

- dependency: [pandas](https://github.com/pydata/pandas), recommend to install [Anaconda](https://www.continuum.io/downloads)

- leave the credential file 'ubcxdata.json' in the reports directory

- default course_list: currently active courses:
  - 'UBCx/Marketing1x/3T2015'
  - 'UBCx/Climate1x/2T2016',
  - 'UBCx/SPD1x/2T2016',
  - 'UBCx/SPD2x/2T2016',
  - 'UBCx/SPD3x/2T2016',
  - 'UBCx/UseGen.1x/1T2016',
  - 'UBCx/UseGen.2x/1T2016'


- daily_lastweek: can run daily for information about number students enrolled, unenrolled and verified for the last week

- uptodate: run weekly (Monday afternoon because data in bigquery is updated once a week on Monday Morning) for up-to-date (last Sunday) information about total number of students registered and verified, % students verified and revenue up-to-date

- activity_lastweek: run weekly (Monday afternoon) for activity information during the last week including number of students active, total number of events, total number of video views, problem attempts and forum posts

** Example use: **

cd to reports directory, python

- default course_list

~~~~
from reports import daily_lastweek, uptodate, activity_lastweek

daily_lastweek()    # generates daily+date.csv (eg. daily2016-06-01.csv)
uptodate()          # generates daily+date.csv (eg. uptodate2016-06-01.csv)
activity_lastweek() # generates activity+date.csv (eg. activity2016-06-01.csv)
~~~~
- specify courses

~~~~
from reports import daily_lastweek, uptodate, activity_lastweek

course_list = [ 'UBCx__Marketing1x__3T2015',  'UBCx__Climate1x__2T2016',
                'UBCx__SPD1x__2T2016', 'UBCx__SPD2x__2T2016', 'UBCx__SPD3x__2T2016']
prices = [50, 49, 49, 49, 49]

daily_lastweek(course_list)
uptodate(course_list, prices)
activity_lastweek(course_list)
~~~~
