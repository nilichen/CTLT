import pandas as pd

def query_cs(course_id):    
    query = """
    SELECT 
    Case 
         When c1.category='problem' And c1.graded='true' Then 'graded_problem'
         When c1.category='problem' And c1.graded!='true' Then 'self_test' 
         Else c1.category
    End As category, c1.index As index, c1.name As name,
    c1.url_name As url_name, c2.name As chapter
    FROM [{0}.course_axis] c1
    Left Join [{0}.course_axis] c2
    On c1.chapter_mid = c2.module_id
    Where c1.category in ('video', 'problem', 'openassessment')
    Order By c1.index""".format(course_id)

    structure = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    return structure

def query_nstudents(course_id, cs, index, cols):
    """Query and calculate number of students viewed the video, attempted the problem"""
    query = """
    SELECT video_id, Count(distinct username) As nstudents 
    FROM [%s.video_stats_day]
    Group By video_id""" % course_id
    videos = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    
    query = """
    SELECT problem_url_name, Count(distinct user_id) As nstudents, Sum(item.correct_bool) As ncorrect
    FROM [%s.problem_analysis]
    Group By problem_url_name""" % course_id
    problems = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    
    problems = cs[cs.category=='graded_problem'].merge(problems, left_on='url_name', right_on='problem_url_name')
    videos =  cs[cs.category=='video'].merge(videos, left_on='url_name', right_on='video_id')
    
    return videos, problems
    

def nstudents(course_id):
    query = """
    SELECT video_id, Count(distinct username) As nstudents 
    FROM [%s.video_stats_day]
    Group By video_id""" % course_id
    videos = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    
    query = """
    SELECT problem_url_name, Count(distinct user_id) As nstudents, Sum(item.correct_bool) As ncorrect
    FROM [%s.problem_analysis]
    Group By problem_url_name""" % course_id
    problems = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    
    return videos, problems


def query_entry(course_id):
    """
    Query entry survey;
    Example course_id: 'UBCx__Marketing1x__3T2015'
    """
    query = "Select * From [%s.entry_survey_mapped]" % course_id
    survey = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    survey = survey.drop_duplicates('user_id', keep='last').ix[:, 11:]
    survey.columns = survey.columns.str.replace("s_", "")
    return survey

def query_exit(course_id):
    """
    Query exit survey;
    Example course_id: 'UBCx__Marketing1x__3T2015'
    """   
    query = "Select * From [%s.exit_survey_mapped]" % course_id
    survey = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    survey = survey.drop_duplicates('user_id', keep='last').ix[:, 11:]
    survey.columns = survey.columns.str.replace("s_", "")
    return survey

def query_pc(course_id):     
    """
    Query person_course;
    Example course_id: 'UBCx__Marketing1x__3T2015'
    """
    query = \
    """Select user_id, is_active, mode, explored, certified, grade, 
    ndays_act, nproblem_check, nplay_video, nforum_posts
    From [%s.person_course]
    Where viewed = 1""" % course_id           
    person_course = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    return person_course
    
def query_pev(course_list):
    """
    Query students' behavior: median_grade, median ndays_act, # students posted, etc.
    grouped by passing vs. explored vs. viewed;
    Example course_list : 
    ['UBCx/Climate101x/3T2015', 'UBCx/Climate1x/1T2016', 'UBCx/Marketing1x/3T2015']
    """
    courses = "', '".join([x for x in course_list])
    query = """Select course_id, grade_median, nevents_median, ndays_act_median, 
                sum_dt_median, sum_n_nonzeo_nforum_posts
                From [courses.certified_median_stats_by_course]
                Where course_id in ('%s')""" % courses
    passing = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)

    query = """Select course_id, grade_median, nevents_median, ndays_act_median, 
                sum_dt_median, sum_n_nonzeo_nforum_posts
                From [courses.explored_median_stats_by_course]
                Where course_id in ('%s')""" % courses
    explored = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)

    query = """Select course_id, grade_median, nevents_median, ndays_act_median, 
                sum_dt_median, sum_n_nonzeo_nforum_posts
                From [courses.viewed_median_stats_by_course]
                Where course_id in ('%s')""" % courses
    viewed = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)

    passing['category'] = 'Passing'
    explored['category'] = 'Explored'
    viewed['category'] = 'Viewed'
    pev = pd.concat([passing, explored, viewed])   

    return pev


def query_convs(course_list):
    """
    Query conversion numbers: nregistered, nviewed, nexplored, npassing, nverified for multiple courses
    Example course_list : 
    ['UBCx/Climate101x/3T2015', 'UBCx/Climate1x/1T2016', 'UBCx/Marketing1x/3T2015']
    """
    courses = "', '".join([x for x in course_list])
    query = \
    """Select course_id, registered_sum As nregistered, viewed_sum As nviewed, 
    explored_sum As nexplored, certified_sum As npassing, n_verified_id As nverified
    From [courses.broad_stats_by_course] 
    Where course_id in ('%s') 
    Order By registered_sum Desc""" % courses
    convs = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    
    return convs