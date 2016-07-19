
# coding: utf-8

# # Multiple courses metric report
# ## Course structure

# In[1]:

get_ipython().magic(u'matplotlib inline')
from __future__ import division
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from collections import OrderedDict

import plotly
import plotly.offline as py
import plotly.graph_objs as go
import plotly.tools as tls

py.init_notebook_mode() # graphs charts inline (IPython).


# In[89]:

course_list = [
    'UBCx__Marketing1x__3T2015',
    'UBCx__Climate1x__1T2016',        
    'UBCx__SPD1x__2T2015',
    'UBCx__SPD2x__2T2015',
    'UBCx__SPD3x__3T2015',
    'UBCx__UseGen_1x__3T2015',
    'UBCx__UseGen_2x__3T2015',
    'UBCx__China300_1x__1T2016',
    'UBCx__China300_2x__1T2016',
    'UBCx__Forest222x__1T2015',
    'UBCx__IndEdu200x__3T2015',
    'UBCx__Water201x_2__2T2015',
#     'UBCx__CW1_1x__1T2016',
#     'UBCx__CW1_2x__1T2016',
#     'UBCx__Phot1x__3T2015',
#     'UBCx__ITSx__2T2015'
]

indices = [course.replace('__', '/').replace('_', '.') for course in course_list]
indices[indices.index('UBCx/Water201x.2/2T2015')] = 'UBCx/Water201x_2/2T2015'


# In[45]:

def stacked_bar(df, names, barmode, title=None, width=600):
    """
    Plot stacked or overlay bar graph given the dataframe
    names: names for all the traces
    """
    data = []
    for i in range(0, df.shape[1]):
        data.append(go.Bar(x=df.index, y=df.ix[:, i], name=names[i]))
        
    layout = go.Layout(
    width=width,
    margin=go.Margin(b=120),
    barmode=barmode,
    title=title)
    
    fig = go.Figure(data=data, layout=layout)
    py.iplot(fig)


# In[46]:

def query_cs(course_id):    
    query = """
    SELECT
    Case 
         When c1.category='problem' And c1.graded='true' Then 'graded_problem'
         When c1.category='problem' And c1.graded!='true' Then 'self_test' 
         Else c1.category
    End As category, c1.index As index, c1.name As name,
    c1.url_name As url_name, c2.name As chapter
    FROM [[{0}.course_axis] c1
    Left Join [{0}.course_axis] c2
    On c1.chapter_mid = c2.module_id
    Where c1.category in ('video', 'problem', 'openassessment', 'chapter')
    Order By c1.index""".format(course_id)

    structure = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    structure = structure[~((structure.category != 'chapter') & (structure.chapter.isnull()))]
    return structure


# In[90]:

def query_css(course_list):
    cs_tables = ',\n'.join(['[%s.course_axis]' % x for x in course_list])
    query = """
    SELECT course_id,
    Case 
         When category='problem' And graded='true' Then 'graded_problem'
         When category='problem' And graded!='true' Then 'self_test' 
         Else category
    End As category, Count(*) As num
    From %s
    Where category in ('video', 'problem', 'openassessment')
    Group By course_id, category
    Order By course_id, category""" % cs_tables
    course_structures = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    course_structures = course_structures.pivot_table(index='course_id', columns='category', values='num', fill_value=0)
    course_structures = course_structures[['video', 'graded_problem', 'self_test', 'openassessment']]
    
    for course in course_list:
        if 'UseGen' in course:
            value = pd.io.gbq.read_gbq("SELECT COunt(*) FROM [%s.course_item]" % course, 
                       project_id='ubcxdata', verbose=False)
            course_structures.ix[course.replace('__', '/').replace('_', '.'), 'graded_problem'] = value.values[0][0]
        
    course_structures['chapter'] = np.nan
    return course_structures.reindex(indices)


# In[91]:

course_structures = query_css(course_list)


# In[114]:

data = []
# colors = ['rgb(77, 175, 74)', 'rgb(255, 127, 0)', 'rgb(55, 126, 184)', 'rgb(228, 26, 28)']
colors = {'video': 'rgb(202,178,214)', 'graded_problem': 'rgb(66,146,198)', 
          'self_test': 'rgb(166,206,227)', 'openassessment': 'rgb(116,196,118)', 'chapter': 'rgb(0, 0, 0)'}
for i in range(0, course_structures.shape[1]):
    data.append(go.Bar(x= course_structures.ix[:, i], y=course_structures.index, 
                       marker=dict(color=colors[course_structures.columns[i]]), 
                       orientation='h', name=course_structures.columns[i]))

layout = go.Layout(
    xaxis=dict(showgrid=False),
    yaxis=dict(autorange='reversed'),
    height=50+30*len(course_structures),
    width=600,
    margin=go.Margin(l=180, b=25, t=25),
    legend=dict(x=100, y=0),
    barmode='stack',
    title='Course structures')

fig = go.Figure(data=data, layout=layout)
py.iplot(fig)


# In[115]:

def rolling_count(df):

    df['block'] = (df['category'] != df['category'].shift(1)).astype(int).cumsum()
    df['count'] = df.groupby('block').url_name.transform(lambda x: range(1, len(x) + 1))
    return df


for course in  course_list:
    df = query_cs(course)
    df.fillna(method='bfill', inplace=True)
    df = df.groupby('chapter').apply(rolling_count)
    idx = df.groupby(['chapter', 'block'])['count'].transform(max) == df['count']
    df = df.ix[idx]
    if 'UseGen' in course:
        query = """
        SELECT problem_id, count(*) As count FROM 
        [%s.course_item] 
        Group by problem_id""" % course
        graded_problem = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)

        df = df.merge(graded_problem, left_on='url_name', right_on='problem_id', how='left')
        df['count'] = df['count_y'].fillna(df['count_x'])
        df = df.drop(['problem_id', 'count_x', 'count_y'], axis=1)
        
    
    data = [go.Bar(x=df['count'], y=[course.replace('__', '/').replace('_', '.')]*len(df), 
                   orientation='h', hoverinfo='y',
                  marker=dict(color=df.category.apply(lambda x: colors[x]).values))]
    layout = go.Layout(
        xaxis=dict(tickfont=dict(size=8), showgrid=False),
        barmode='stack', 
        width=900,
        height=45,
        margin=go.Margin(l=180, b=10, t=0)
    )
    fig = go.Figure(data=data, layout=layout)
    py.iplot(fig)


# ## Total learners
# - Registered: those registered in the course
# - Sampled: those with sum_dt >= 5 min
# - Learned: those with sum_dt >= 30 min
# - Passed: those whose grade is more than 50%
# - Verified: those purchased the verified certificate
# 
# ** sum_dt **: Total elapsed time (in seconds) spent by user on this course, based on time difference of consecutive events, with 5 min max cutoff, from tracking logs

# In[109]:

def query_convs(course_list):
    """
    Query conversion numbers: nregistered, nviewed, nexplored, npassing, nverified for multiple courses
    Example course_list : 
    ['UBCx__Climate101x__3T2015', 'UBCx__Climate1x__1T2016', 'UBCx__Marketing1x__3T2015']
    """
    pc_tables = ',\n'.join(['[%s.person_course]' % x for x in course_list])
    query =     """SELECT course_id, Count(*) As Registered, 
    Sum(Case When sum_dt >= 300 Then 1 Else 0 End) As Sampled,
    Sum(Case When sum_dt >= 1800 Then 1 Else 0 End) As Learned, 
    Sum(Case When grade >= 0.5 Then 1 Else 0 End) As Passed, 
    Sum(Case When mode='verified' Then 1 Else 0 End) As Verified
    FROM %s
    Group By course_id""" % pc_tables
    convs = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    convs.set_index('course_id', inplace=True)
    
#     if 'UBCx__SPD1x__2T2015' in course_list:
#         nexplored = pd.io.gbq.read_gbq("Select Count(*) From [UBCx__SPD1x__2T2015.pc_nchapters] Where nchapters >= 3", 
#                                        project_id='ubcxdata', verbose=False)
#         convs.ix['UBCx/SPD1x/2T2015', 'nexplored'] = nexplored.values[0][0]
    
    return convs.reindex(indices)


# In[110]:

# course_list = ['UBCx/Climate101x/3T2015', 'UBCx/Climate1x/1T2016',
#                'UBCx/Marketing1x/3T2015', 'UBCx/SPD1x/1T2016']
convs = query_convs(course_list)


# In[122]:

convs_pct = convs.drop('Registered', axis=1).divide(convs.drop('Registered', axis=1).max(axis=1), axis=0)#.round(3)
convs_txt = convs_pct.copy()
# convs_txt['Sampled'] = convs.Sampled / convs.Registered
convs_df = convs_txt.divide(convs_txt.max(axis=0), axis=1).ix[:, 1:]
convs_pct = convs_pct.applymap(lambda x: "{0:.2f}".format(x * 100))
convs_txt = convs_txt.applymap(lambda x: "{0:.2f}".format(x * 100)).ix[:, 1:]
# convs_pct


# In[124]:

names = convs.columns
colors = ['rgb(55, 126, 184)', 'rgb(255, 127, 0)', 'rgb(77, 175, 74)', 'rgb(228, 26, 28)', 'rgb(152, 78, 163)']
fig = tls.make_subplots(rows=1, cols=3, print_grid=False,
                              subplot_titles=('# studednts', '% conversion', 'Compare % conversion'))
for i in range(0, convs.shape[1]):
        fig.append_trace(go.Bar(x=convs.ix[:, i], y=convs.index, orientation='h', 
                                marker=dict(color=colors[i]), name=names[i]), 1, 1)

for i in range(0, convs_pct.shape[1]):
        fig.append_trace(go.Bar(x=convs_pct.ix[:, i], y=convs_pct.index, orientation='h',
                                marker=dict(color=colors[i+1]), name=names[i+1], showlegend=False), 1, 2)
        
fig.append_trace(go.Heatmap(z=convs_df.values, x=convs_df.columns, y=convs_df.index, 
                            text=convs_txt.values, hoverinfo='y+text', 
                            colorscale=[[0.0, 'rgb(224,243,248)'], [1.0, 'rgb(43,130,189)']], showscale=False), 1, 3)

fig['layout']['xaxis1'].update(showgrid=False)
fig['layout']['yaxis1'].update(autorange='reversed')
fig['layout']['xaxis2'].update(showgrid=False)
fig['layout']['yaxis2'].update(showticklabels=False, autorange='reversed')
fig['layout']['yaxis3'].update(showticklabels=False, autorange='reversed')
fig['layout']['xaxis3'].update(autorange='reversed')
fig['layout']['legend'].update(x=0.13, y=0.15)

fig['layout'].update(barmode='overlay', height=80+30*len(convs), width=900, margin=go.Margin(l=180, t=25))
py.iplot(fig) 


# ## Engagement
# 
# ** Students' engagement in the course: **
# - number of days active in the course
# - hours spent in the course
# - number of video plays and problem checks 
# - % students posted in the forum

# In[70]:

def query_pcs_learned(course_list):
#     pc_tables = ',\n'.join(['[%s.person_course]' % x for x in course_list])
#     query = """
#     Select user_id, grade, course_id, nplay_video, nproblem_check, 
#     ndays_act,  sum_dt / 3600 As sum_dt, nforum_posts
#     From %s
#     Where sum_dt >= 1800""" % pc_tables
    
#     pcs_learned = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False).fillna(0)
    dfs = []
    for course in course_list:
        query = """
        Select pc.course_id As course_id, pc.user_id As user_id, pc.mode As mode, pc.grade As grade, 
        pc.ndays_act As ndays_act, pc.sum_dt / 3600 As sum_dt, pc.nforum_posts As nforum_posts,
        v.videos_watched / t.nvideos As pct_video_watched, p.problems_attempted / t.nproblems As pct_problem_attempted
        From [{0}.person_course] pc
        Left Join
        (SELECT username, Count(Distinct video_id) As videos_watched 
        FROM [{0}.video_stats_day]
        Group By username) v
        on pc.username = v.username
        Left Join 
        (Select user_id, Count(Distinct problem_url_name) As problems_attempted
        From [{0}.problem_analysis]
        Group By user_id) p
        On pc.user_id = p.user_id
        Left Join 
        (Select course_id,
        Sum(Case When category = 'video' Then 1 Else 0 End) As nvideos,
        Sum(Case When category = 'problem' Then 1 Else 0 End) As nproblems
        From [{0}.course_axis]
        Group By course_id) t
        On pc.course_id = t.course_id
        Where pc.sum_dt >= 1800""".format(course)
        dfs.append(pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False).fillna(0))
        pcs_learned = pd.concat(dfs)
    return pcs_learned


# In[71]:

pcs_learned = query_pcs_learned(course_list)


# In[100]:

cols = ['ndays_act', 'sum_dt', 'pct_video_watched', 'pct_problem_attempted', 'nforum_posts']
pcs_learned_agg = pcs_learned.groupby('course_id').agg({'ndays_act': np.median, 'sum_dt': np.median, 
                                                        'pct_video_watched': np.median, 'pct_problem_attempted': np.median, 
                                                        'nforum_posts': lambda s: (s > 0).sum() / len(s)})
pcs_learned_agg = pcs_learned_agg[cols].reindex(indices)

pcs_passed = pcs_learned[pcs_learned.grade >= 0.5]
pcs_passed_agg = pcs_passed.groupby('course_id').agg({'ndays_act': np.median, 'sum_dt': np.median, 
                                                        'pct_video_watched': np.median, 'pct_problem_attempted': np.median, 
                                                        'nforum_posts': lambda s: (s > 0).sum() / len(s)})
pcs_passed_agg = pcs_passed_agg[cols].reindex(indices)

pcs_passed_norm = pcs_passed_agg.divide(pcs_passed_agg.max(axis=0), axis=1)
pcs_learned_norm = pcs_learned_agg.divide(pcs_learned_agg.max(axis=0), axis=1)

pcs_passed_agg.sum_dt = pcs_passed_agg.sum_dt.round(2)
pcs_passed_agg[['pct_video_watched', 'pct_problem_attempted', 'nforum_posts']] = pcs_passed_agg[['pct_video_watched', 'pct_problem_attempted', 'nforum_posts']].applymap(lambda x: "{0:.2f}".format(x * 100))
pcs_learned_agg.sum_dt = pcs_learned_agg.sum_dt.round(2)
pcs_learned_agg[['pct_video_watched', 'pct_problem_attempted', 'nforum_posts']] = pcs_learned_agg[['pct_video_watched', 'pct_problem_attempted', 'nforum_posts']].applymap(lambda x: "{0:.2f}".format(x * 100))


# In[101]:

names = ['median days active', 'median sum_dt (H)', 
         'median % videos', 'median % problems', '% students posted']
trace1 = go.Heatmap(
            z=pcs_learned_norm.values,
            x=names,
            y=pcs_learned_norm.index,
            text = pcs_learned_agg.values,
            hoverinfo='x+text',
            showscale=False,
            colorscale=[[0.0, 'rgb(224,243,248)'], [1.0, 'rgb(51,160,44)']])


trace2 = go.Heatmap(
            z=pcs_passed_norm.values,
            x=names,
            y=pcs_passed_norm.index,
            text = pcs_passed_agg.values,
            hoverinfo='x+text',
            showscale=False,
            colorscale=[[0.0, 'rgb(224,243,248)'], [1.0, 'rgb(43,130,189)']])

fig = tls.make_subplots(rows=1, cols=2, print_grid=False,
                              subplot_titles=('Engagement for the learned', 'Engagement for the passed'))
fig.append_trace(trace1, 1, 1)
fig.append_trace(trace2, 1, 2)
fig['layout']['yaxis1'].update( autorange='reversed')
fig['layout']['yaxis2'].update(showticklabels=False, autorange='reversed')

fig['layout'].update(
    width=850, height=140+30*len(pcs_learned_agg),
    margin=go.Margin(l=180, b=120, t=20)
    # title=title
)

py.iplot(fig)


# In[102]:

def plot_stEngagement(passed, learned, names, title=None):
    color_passed = ['rgb(166,206,227)', 'rgb(253,191,111)', 'rgb(178,223,138)', 'rgb(251,154,153)', 'rgb(202,178,214)']
    color_learned = ['rgb(31,120,180)', 'rgb(255,127,0)', 'rgb(51,160,44)', 'rgb(227,26,28)', 'rgb(106,61,154)']
    fig = tls.make_subplots(rows=1, cols=passed.shape[1], print_grid=False)
    for i in range(0, passed.shape[1]):
        fig.append_trace(go.Bar(x=passed.ix[:, i], y=passed.index, marker=dict(color=color_passed[i]),
                        name='passed', orientation='h', showlegend=False), 1, i+1)
        fig.append_trace(go.Bar(x=learned.ix[:, i], y=learned.index, marker=dict(color=color_learned[i]),
                                name='learned', orientation='h', showlegend=False), 1, i+1)

    showticklabels = True
    # fig['layout']['yaxis1'].update(autorange='reversed')
    for i in range(1, passed.shape[1]+1):
        if i >= 2:
            showticklabels = False
        fig['layout']['xaxis%s' % i].update(title=names[i-1], titlefont=dict(size=10), 
                                            tickfont=dict(size=8), showgrid=False)
        fig['layout']['yaxis%s' % i].update(showticklabels=showticklabels, showgrid=False, autorange='reversed')

    fig['layout'].update(barmode='overlay', height=100+30*len(passed), width=900, 
                         margin=go.Margin(t=40, l=180), title=title)
    py.iplot(fig)


# In[103]:

plot_stEngagement(pcs_passed_agg, pcs_learned_agg, names, "Engagement for the learned and the passed")


# ** Students' activity in the course by modules: **

# In[104]:

def query_moduleActivity(course_id):
    query = """
    Select sub.course_id As course_id, sub.index As index, sub.module_id As module_id, sub.chapter_name As chapter_name, 
    Count(Distinct sub.user_id) As tried_problem
    From
    (SELECT p.course_id As course_id, p.user_id As user_id, c2.index As index, 
    c2.module_id As module_id, c2.name As chapter_name
    FROM [{0}.problem_analysis] p
    Left Join [{0}.course_axis] c1
    on p.problem_url_name = c1.url_name
    Left Join [{0}.course_axis] c2
    On c1.chapter_mid = c2.module_id) sub
    Group By course_id, index, module_id, chapter_name
    Order By index""".format(course_id)
    tried_problem = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    tried_problem = tried_problem[tried_problem.tried_problem > 20]

    query = """
    Select sub.course_id As course_id, sub.index As index, sub.module_id As module_id, sub.chapter_name As chapter_name, 
    Count(Distinct sub.username) As watched_video
    From
    (SELECT c1.course_id As course_id, v.username As username, c2.index As index, 
    c2.module_id As module_id, c2.name As chapter_name
    FROM [{0}.video_stats_day] v
    Left Join [{0}.course_axis] c1
    on v.video_id = c1.url_name
    Left Join [{0}.course_axis] c2
    On c1.chapter_mid = c2.module_id) sub
    Group By course_id, index, module_id, chapter_name
    Order By index""".format(course_id)
    watched_video = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    watched_video = watched_video[watched_video.watched_video > 20]

    query = """
    Select course_id, module_id, Count(Distinct student_id) As nactive
    From [{0}.studentmodule]
    Where module_type = 'chapter' 
    Group By course_id, module_id""".format(course_id)
    nactive = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    nactive.module_id = nactive.module_id.str.replace('i4x://', '')
    nactive = nactive[nactive.nactive > 20]
    
    module_activity = watched_video.merge(tried_problem, how='outer').merge(nactive, how='outer').fillna(0)
    return module_activity[module_activity.chapter_name != 0].sort_values('index')
    # return tried_problem, watched_video, nactive


# In[158]:

fig = tls.make_subplots(rows=int(np.ceil(len(course_list)/2)), cols=2, 
                        subplot_titles=tuple([course.replace('__', '/').replace('_', '.') for course in course_list]), 
                        print_grid=False)
showlegend = True
i = 0
while i < len(course_list):
    module_activity = query_moduleActivity(course_list[i])
    if i >= 1:
        showlegend = False
    fig.append_trace(go.Scatter(x=module_activity.chapter_name, y=module_activity.tried_problem, 
                                name='tried a problem', fill='tozeroy', mode='lines',
                                line = dict(color = ('rgb(255, 127, 0)')), showlegend=showlegend), i//2+1, 1)
    fig.append_trace(go.Scatter(x=module_activity.chapter_name, y=module_activity.watched_video, 
                                name='watched a video',  fill='tonexty', mode='lines',
                                line = dict(color = ('rgb(77, 175, 74)')), showlegend=showlegend), i//2+1, 1)
    fig.append_trace(go.Scatter(x=module_activity.chapter_name, y=module_activity.nactive, 
                                name='with any activity', fill='tonexty', mode='lines', 
                                line = dict(color = ('rgb(55, 126, 184)')), showlegend=showlegend), i//2+1, 1)
    i += 1
    if i >= 1:
        showlegend = False
    module_activity = query_moduleActivity(course_list[i])
    fig.append_trace(go.Scatter(x=module_activity.chapter_name, y=module_activity.tried_problem, 
                                name='tried a problem', fill='tozeroy', mode='lines',
                                line = dict(color = ('rgb(255, 127, 0)')), showlegend=showlegend), i//2+1, 2)
    fig.append_trace(go.Scatter(x=module_activity.chapter_name, y=module_activity.watched_video, 
                                name='watched a video', fill='tonexty', mode='lines',
                                line = dict(color = ('rgb(77, 175, 74)')), showlegend=showlegend), i//2+1, 2)
    fig.append_trace(go.Scatter(x=module_activity.chapter_name, y=module_activity.nactive, 
                                name='with any activity', fill='tonexty', mode='lines',
                                line = dict(color = ('rgb(55, 126, 184)')), showlegend=showlegend), i//2+1, 2)
    i += 1


# In[159]:

for i in range(1, len(course_list)+1):
    fig['layout']['xaxis%s' % i].update(showticklabels=False, showgrid=False)
    fig['layout']['yaxis%s' % i].update(showgrid=False)
fig['layout']['legend'].update(x=0.9, y=1)
fig['layout'].update(height=160+150*len(module_activity), width=850, title="Students' activities by module")  
py.iplot(fig)


# ## Outcome
# 
# ### Exit survey feedback grouped by goals from entry survey
# ** Are you likely to...: **
# 
# Recommend this course to a friend

# In[ ]:

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

entry_marketing = query_entry('UBCx__Marketing1x__3T2015')
exit_marketing = query_exit('UBCx__Marketing1x__3T2015')
surveys_marketing = pd.merge(entry_marketing, exit_marketing, on='user_id', how='inner')

exit_climate = query_exit('UBCx__Climate101x__3T2015')
entry_climate = query_entry('UBCx__Climate101x__3T2015')
surveys = pd.merge(entry_climate, exit_climate, on='user_id', how='inner')
# pc_climate = query_pc('UBCx__Climate101x__3T2015')
# merged = entry_climate.merge(pc_climate, on='user_id')
# stats = query_stats('UBCx__Climate101x__3T2015')

exit_climate2 = query_exit('UBCx__Climate1x__1T2016')
entry_climate2 = query_entry('UBCx__Climate1x__1T2016')
surveys2 = pd.merge(entry_climate2, exit_climate2, on='user_id', how='inner')


# In[26]:

def compute_heatmap(surveys, selections, matrix, start, col_names):
    """
    Compute stats for questions 'Are you likely...' and 'How satisfied ...';
    surveys: entry survey merged with exit survey
    selections: a list of all the goals from entry survey
    start: index of the first column for question about goals in the merged survey
    matrix: for question Are you likely...' or 'How satisfied ...', 
    a dictionary with column names as keys and an empty list as values
    e.g. matrix = {'Q2_2_1': [], 'Q2_2_2': [], 'Q2_2_3': [], 'Q2_2_4': [], 'Q2_2_5': []}
    col_names: corresponding names for the columns
    e.g. ['Recommend', 'Advanced', 'Revisit', 'Involvement', 'Habbits']
    """
    rm = []
    selections2 = selections[:]
    survey_count = len(surveys)*0.05

    for i in range(len(selections2)):
        group_count = surveys.ix[:, i+start].notnull().sum()
        if group_count > survey_count:
            for k, v in matrix.iteritems():
                v.append(surveys.ix[surveys.ix[:, i+start].notnull(), k].notnull().sum() / group_count)
        else:
            rm.append(selections2[i])
    for k, v in matrix.iteritems():
        v.append(surveys.ix[:, k].notnull().sum() / len(surveys))
    for s in rm:
        selections2.remove(s)
    selections2 = selections2 + ['Overall']
    matrix = pd.DataFrame(matrix)
    matrix.index = selections2
    matrix.columns = col_names
    return matrix

def heatmap(df, title=None, xlabel=None, ylabel=None, width=700):
    """Plot heatmap given a dataframe"""
    data = [
        go.Heatmap(
            z=df.values,
            x=df.columns,
            y=df.index,
            colorscale=[[0.0, 'rgb(224,243,248)'], [1.0, 'rgb(43,130,189)']]
        )
    ]

    layout = go.Layout(
        xaxis=dict(title=xlabel),
        yaxis=dict(title=ylabel, autorange='reversed'), 
        width=width, height=500,
        margin=go.Margin(l=150, b=120),
        title=title
    )

    fig = go.Figure(data=data, layout=layout)
    py.iplot(fig)


# In[ ]:

selections_m = ['Practical knowledge',
              'Verified certificate',
              'Something interesting',
              'More about MOOCs',
              'Substantial understanding',
              'Improve English',
              'Others']
matrix_m = {'Q1_1': [], 'Q1_2_y': [], 'Q1_3_y': [], 'Q1_4': [], 'Q1_5': [], 'Q1_6': [], 'Q1_7': [], 'Q1_8_y': []}
colnames_m = ['Recommend', 'Advanced', 'Revist', 'Instructor', 'Involvement', 'UBC', 'Habbits', 'edX']
likely_marketing = compute_heatmap(surveys_marketing, selections_m, matrix_m, 0, colnames_m )
#likely_marketing


# In[10]:

selections = ['Relevant understanding', 
              'Something interesting',
              'Verified certificate',
              'Pursue further',
              'Learn with friends',
              'Impress people',
              'More about MOOCs',
              'Substantial understanding',
              'Practical knowledge',
              'Improve English',
              'Others']
matrix = {'Q2_2_1': [], 'Q2_2_2': [], 'Q2_2_3': [], 'Q2_2_4': [], 'Q2_2_5': []}
col_names = ['Recommend', 'Advanced', 'Revisit', 'Involvement', 'Habbits']

likely = compute_heatmap(surveys, selections, matrix, 10, col_names)


# In[11]:

matrix = {'Q2_2_1': [], 'Q2_2_2': [], 'Q2_2_3': [], 'Q2_2_4': [], 'Q2_2_5': []}
likely2 = compute_heatmap(surveys2, selections, matrix, 10, col_names)


# In[ ]:

recommend = pd.concat([likely.loc[likely_marketing.index].Recommend, likely2.loc[likely_marketing.index].Recommend,
                        likely_marketing.Recommend], axis=1)
recommend.columns = ['Climate2015', 'Climate2016', 'Marketing2015']
recommend


# In[44]:

heatmap(recommend.T, title='Percent recommendation grouped by goals', xlabel='Goals', ylabel='Courses', width=600)


# In[ ]:



