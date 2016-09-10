## Work summary

- [Course metrics dashboard](https://github.com/nilichen/CTLT/tree/master/metric):
  - Develop metrics and visualizations to present and compare
    - course structure,
    - enrollment and retention,
    - students’ engagement and learning pattern etc.
    for edX instructors and administrators
  - Streamline and automate the process of data import, data munging and visualization in creating dashboards for various courses


- Customized reports:
  - [Automated](https://github.com/nilichen/CTLT/tree/master/reports)
    - Daily reports for enrollment, unenrollment and verification
    - Weekly reports for students’ activity: nactive, nvideo_views, nproblem_attempts, nforum_posts
    - Weekly report for up-to-date # of registration, verification and revenue
  - [Mobile analysis](https://github.com/nilichen/CTLT_MOOC_analysis/blob/master/reports/mobile_analysis.ipynb):
    - % light mobileApp/mobile users and % frequent mobileApp/mobile users by course, age, country and learner type
  - List of email addresses for all the students enrolled in UBC edX


- Research analysis:
  -	[Forum analysis](https://github.com/nilichen/CTLT_MOOC_analysis/tree/master/Climate/forum):
  Analyze and compare students’ participation in the forum for 3 iterations of Climate after changing the way TAs are involved in the discussion
  -	[Map assignments](https://github.com/nilichen/CTLT_MOOC_analysis/tree/master/Climate/map_assignment) – before and after analysis:
    - After assigning students into regional and global for map assignment, analyze and compare students’ engagement before and after finishing the first map assignment.
  -	[Survey analysis](https://github.com/nilichen/CTLT_MOOC_analysis/blob/master/metric/survey.ipynb) (currently excluded from course metrics dashboard due to lack of data for all the courses):
    -	main reasons taking the course
    -	if goals are met
    -	which components are satisfied
    -	likely to recommend


- Data export and preparation for instructors: 10%
  -	PHYS100-98a and UseGen: PI data export and analysis
  -	PSYC101: Data export and mapping
  -	Phot1x and CPSC110: Forum data
  -	CPSC210: Problems attempts for each student


- [edx2bigquery](https://github.com/nilichen/edx2bigquery):
  -	Maintain the pipeline transforming and converting unstructured data from edX to structured data in Google Bigquery


- A4L:
  -	Data validation and verification
  -	Create baseline reports for evaluation of A4L
