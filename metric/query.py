import pandas as pd


ddef query_entry(course_id):
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