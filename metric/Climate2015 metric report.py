
# coding: utf-8

# # Climate2015 metric report

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


# In[2]:

course_id = 'UBCx__Climate101x__3T2015'


# In[3]:

def query_cs(course_id = course_id):    
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


# In[4]:

# index = ['About Climate Change: The Science', '1. Course Introduction - State of the Science',
#          '2.  Introduction to the Climate System', "3. Earth's Energy Budget", '4. The Carbon Cycle',
#          '5. Climate Models', '6. Future Climate', 'Assignments', 'Final Exam', 'End of Term Surveys']
course_structure = query_cs()
indices = course_structure[['index', 'chapter']].drop_duplicates('chapter').dropna().chapter.values
cs_chapter = course_structure.groupby('category',).chapter.value_counts(sort=False).unstack('category').reindex(indices)
cols = []
for col in ['video', 'graded_problem', 'self_test', 'openassessment']:
    if col in cs_chapter.columns.values:
        cols.append(col)
cs_chapter = cs_chapter[cols]
cs_chapter['chapter'] = np.nan


# ### Course structure and activity

# In[5]:

def query_moduleActivity(course_id=course_id):
    query = """
    Select sub.course_id As course_id, sub.index As index, sub.module_id As module_id,
    sub.chapter_name As chapter_name, Count(Distinct sub.user_id) As tried_problem
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
    Select sub.course_id As course_id, sub.index As index, sub.module_id As module_id, 
    sub.chapter_name As chapter_name, Count(Distinct sub.username) As watched_video
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
    Select sub.course_id As course_id, sub.module_id As module_id, 
    c.name As chapter_name, c.index As index, sub.nactive As nactive
    From [{0}.course_axis] c
    Join 
    (Select course_id As course_id, Regexp_replace(module_id,'i4x://', '') As module_id, 
    Count(Distinct student_id) As nactive
    From [{0}.studentmodule]
    Where module_type = 'chapter' 
    Group By course_id, module_id) sub
    On sub.module_id = c.module_id
    Order By index""".format(course_id)
    nactive = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    # nactive.module_id = nactive.module_id.str.replace('i4x://', '')
    nactive = nactive[nactive.nactive > 20]
    
    module_activity = watched_video.merge(tried_problem, how='outer').merge(nactive, how='outer').fillna(0)
    return module_activity.set_index('chapter_name').reindex(indices)
    # return tried_problem, watched_video, nactive
    
module_activity = query_moduleActivity()


# In[6]:

fig = tls.make_subplots(rows=1, cols=2, print_grid=False, subplot_titles=('Course structure', 'Module activity'))

# colors = ['rgb(77, 175, 74)', 'rgb(255, 127, 0)', 'rgb(55, 126, 184)', 'rgb(228, 26, 28)']
colors = {'video': 'rgb(202,178,214)', 'graded_problem': 'rgb(66,146,198)', 
          'self_test': 'rgb(166,206,227)', 'openassessment': 'rgb(116,196,118)', 'chapter': 'rgb(0, 0, 0)'}

fig.append_trace(go.Scatter(y=module_activity.index, x=module_activity.watched_video, 
                    name='watched a video', fill='tozerox', mode='lines', 
                            line=dict(color='rgb(152,78,163)')), 1, 2)
fig.append_trace(go.Scatter(y=module_activity.index, x=module_activity.tried_problem, 
                    name='tried a problem', fill='tonextx', mode='lines', 
                            line=dict(color='rgb(66,146,198)')), 1, 2)
fig.append_trace(go.Scatter(y=module_activity.index, x=module_activity.nactive, 
                    name='with any activity', fill='tonextx', mode='lines', 
                            line=dict(color='rgb(255,127,0)')), 1, 2)

for i in range(0, cs_chapter.shape[1]):
    fig.append_trace(go.Bar(y=cs_chapter.index, x=cs_chapter.ix[:, i], orientation='h',
                       marker=dict(color=colors[cs_chapter.columns[i]]), name=cs_chapter.columns[i]), 1, 1)

fig['layout']['yaxis1'].update(tickfont=dict(size=8), showgrid=False, autorange='reversed')
fig['layout']['yaxis2'].update(showticklabels=False, showgrid=False, autorange='reversed')
fig['layout']['xaxis1'].update(showgrid=False)
fig['layout']['xaxis2'].update(showgrid=False)
fig['layout']['legend'].update(x=0.8, y=0, traceorder='normal')
fig['layout'].update(height=50+30*len(cs_chapter), width=850, margin=go.Margin(l=185, t=25, b=20), barmode='stack')

#fig = go.Figure(data=data, layout=layout)
py.iplot(fig)


# In[7]:

def rolling_count(df):
    df['block'] = (df['category'] != df['category'].shift(1)).astype(int).cumsum()
    df['count'] = df.groupby('block').url_name.transform(lambda x: range(1, len(x) + 1))
    return df

df = course_structure.fillna(method='bfill')
df = df.groupby('chapter').apply(rolling_count)
idx = df.groupby(['chapter', 'block'])['count'].transform(max) == df['count']
df = df.ix[idx]

data = [go.Bar(x=df['count'], y=[course_id.replace('__', '/').replace('_', '.')]*len(df), 
               orientation='h', hoverinfo='y',
              marker=dict(color=df.category.apply(lambda x: colors[x]).values))]
layout = go.Layout(
    xaxis=dict(tickfont=dict(size=8), showgrid=False),
    yaxis=dict(showticklabels=False),
    barmode='stack', 
    width=850,
    height=50,
    margin=go.Margin(b=10, t=0, l=100)
)
fig = go.Figure(data=data, layout=layout)
py.iplot(fig)


# In[8]:

def query_nstudents(cs, course_id = course_id):
    """Query and calculate number of students viewed the video, attempted the problem"""
    query = """
    SELECT video_id, Count(distinct username) As nstudents, max(position) As length
    FROM [%s.video_stats_day]
    Group By video_id""" % course_id
    videos = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    
    # nattempts not working because of extremely large attempts for some entries
    query = """
    SELECT problem_url_name, Count(distinct user_id) As nstudents, Sum(item.correct_bool) As ncorrect
    FROM [%s.problem_analysis]
    Group By problem_url_name""" % course_id
    problems = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    
    graded_problems = cs[cs.category=='graded_problem'].merge(problems, left_on='url_name', right_on='problem_url_name')
    self_tests = cs[cs.category=='self_test'].merge(problems, left_on='url_name', right_on='problem_url_name')
    videos =  cs[cs.category=='video'].merge(videos, left_on='url_name', right_on='video_id')
    
    return videos, graded_problems, self_tests


# In[9]:

videos, problems, self_tests = query_nstudents(cs = course_structure)


# ### Video activity

# In[10]:

def compute_color(serie):
    color = []
    choices = ['rgb(166, 206, 227)', 'rgb(31, 120, 180)', 'rgb(178, 223, 138)', 'rgb(51, 160, 44)', 
               'rgb(251, 154, 153)', 'rgb(227, 26, 28)', 'rgb(253, 191, 111)', 'rgb(255, 127, 0)', 'rgb(202, 178, 214)']
    for i in range(len(serie)):
        color += [choices[i]] * int(serie.values[i])
    return color

data = [go.Bar(x = videos.name, y = videos.length, 
                marker=dict(color=compute_color(cs_chapter.video.dropna(0))), name='length')]
layout = go.Layout(title='Length of videos (seconds)', xaxis=dict(showticklabels=False), 
                   yaxis=dict(showgrid=False), height=180, width=640, margin=go.Margin(t=25, b=25))
fig = go.Figure(data=data, layout=layout)
py.iplot(fig)

data = [go.Bar(x = videos.name, y = videos.nstudents, 
                marker=dict(color=compute_color(cs_chapter.video.dropna(0))), name='nstudents')]
layout = go.Layout(title='Number of students watched', xaxis=dict(showticklabels=False), 
                   yaxis=dict(showgrid=False), height=180, width=640, margin=go.Margin(t=25, b=25))
fig = go.Figure(data=data, layout=layout)
py.iplot(fig)


# ### Graded problem activity

# In[11]:

data = [go.Bar(x = problems.name, y = problems.ncorrect / problems.nstudents,
                marker=dict(color=compute_color(cs_chapter.graded_problem.dropna(0))), name='% correct')]
layout = go.Layout(title='% correct attempts', xaxis=dict(showticklabels=False), 
                   yaxis=dict(showgrid=False), height=180, width=800, margin=go.Margin(t=25, b=25))
fig = go.Figure(data=data, layout=layout)
py.iplot(fig)

data = [go.Bar(x = problems.name, y = problems.nstudents, 
                marker=dict(color=compute_color(cs_chapter.graded_problem.dropna(0))), name='nstudents')]
layout = go.Layout(title='Number of students attempted', xaxis=dict(showticklabels=False), 
                   yaxis=dict(showgrid=False), height=180, width=800, margin=go.Margin(t=25, b=25))
fig = go.Figure(data=data, layout=layout)
py.iplot(fig)


# In[12]:

def query_least(course_id=course_id):
    query = """
    Select sub.problem_name As problem_name, pa.user_id As user_id, pa.item.response As response
    From 
    (SELECT problem_nid, problem_id, problem_name, avg_problem_pct_score FROM [{0}.course_problem] 
    Order By avg_problem_pct_score 
    Limit 10) sub
    Left Join [{0}.problem_analysis] pa 
    On sub.problem_id = pa.problem_url_name
    Order By sub.problem_nid""".format(course_id)

    answers = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    # print 'Correct answers:'
    correct =  answers.groupby('problem_name').response.value_counts().unstack('problem_name').idxmax().to_dict()
    count = answers.groupby('problem_name').response.count()
    
    answers.response = answers.response.apply(lambda x: x.replace('[', '').replace(']', '').split(', '))
    rows = []
    _ = answers.apply(lambda row: [rows.append([row['problem_name'], row['user_id'], choice]) 
                             for choice in row.response], axis=1)
    answers_new = pd.DataFrame(rows, columns=answers.columns)#.set_index(['name', 'opponent'])
    answers_pct = answers_new.groupby('problem_name').response.value_counts().unstack('problem_name').divide(count, axis=1)
    #answers_count = answers_new.groupby('problem_name').response.value_counts(normalize=True).unstack('problem_name')
    return correct, answers_pct


# ** Ten least successful graded problems **

# In[13]:

correct, answers_pct = query_least()


# In[14]:

def correct_color(col):
    return ['rgb(44,162,95)' if x in correct[answers_pct.columns[col]] else 'rgb(49,130,189)' for x in answers_pct.index]

fig = tls.make_subplots(rows=2, cols=5, subplot_titles=tuple(answers_pct.columns), print_grid=False)
for i in range(5):
    quesion = answers_pct.ix[:, i].dropna()
    fig.append_trace(go.Bar(x=quesion.index, y=quesion, 
                            marker=dict(color=correct_color(i)), showlegend=False), 1, i+1)
for i in range(5):
    quesion = answers_pct.ix[:, i+5].dropna()
    fig.append_trace(go.Bar(x=quesion.index, y=quesion, 
                            marker=dict(color=correct_color(i+5)), showlegend=False), 2, i+1)  
    
for i in range(1, answers_pct.shape[1]+1):
    fig['layout']['xaxis%s' % i].update(tickangle=45, tickfont=dict(size=10))
    fig['layout']['yaxis%s' % i].update(showgrid=False)

    fig['layout'].update(height=450, width=800, margin=go.Margin(t=25))  
py.iplot(fig)


# ### Overall engagement:
# ** Learner type **
# - Registered: those registered in the course
# - Sampled: those with sum_dt >= 5 min
# - Learned: those with sum_dt >= 30 min
# - Passed: those whose grade is more than 50%
# - Verified: those purchased the verified certificate
# 
# ** sum_dt **: Total elapsed time (in seconds) spent by user on this course, based on time difference of consecutive events, with 5 min max cutoff, from tracking logs

# In[15]:

def query_pc(course_id = course_id):     
    """
    Query person_course;
    Example course_id: 'UBCx__Marketing1x__3T2015'
    """
    query =     """Select user_id, is_active, mode, certified, grade,
    ndays_act, sum_dt, nproblem_check, nplay_video, nforum_posts
    From [%s.person_course]
    Where ndays_act >= 1""" % course_id           
    person_course = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    return person_course 


# In[16]:

def compute_srp(pc):
    pc_sampled = pc[pc.sum_dt>=300].copy()
    pc_learned = pc[pc.sum_dt>=1800].copy()
    pc_passed = pc[pc.certified].copy()
    
    pc_sampled['category'] = 'Sampled'
    pc_learned['category'] = 'Learned'
    pc_passed['category'] = 'Passed'
    srp = pd.concat([pc_sampled, pc_learned, pc_passed])
    srp_agg = srp.groupby('category').agg({'nplay_video': np.median, 
                             'nproblem_check': np.median, 'ndays_act': np.median, 
                             'sum_dt': np.median, 'nforum_posts': lambda x: (x > 0).sum()})
    srp_agg = srp_agg.reindex(index = ['Passed', 'Learned', 'Sampled'])
    
    return srp_agg


# In[17]:

pc = query_pc()
srp_agg = compute_srp(pc)


# In[18]:

def plot_pls(df, course_id=course_id, title=None):
    """
    Plot students' behavior: median_grade, median ndays_act, # students posted
    grouped by passing vs. explored vs. viewed;
    Example course_id: 'UBCx/Climate101x/3T2015'
    """
    query =     """SELECT Count(*) As Registered, 
    Sum(Case When sum_dt >= 300 Then 1 Else 0 End) As Sampled,
    Sum(Case When sum_dt >= 1800 Then 1 Else 0 End) As Learned, 
    Sum(Case When certified=1 Then 1 Else 0 End) As Passed, 
    Sum(Case When mode='verified' Then 1 Else 0 End) As Verified
    FROM [%s.person_course]""" % course_id
    stats =  pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    # print stats


    trace1 = go.Bar(x=stats.values[0], y=stats.columns, orientation='h', name='# of conversion')
    trace2 = go.Bar(x=df.index, y=df.nplay_video, name='median video plays')
    trace3 = go.Bar(x=df.index, y=df.nproblem_check, name='median problem checs')
    trace4 = go.Bar(x=df.index, y=df.sum_dt/3600, name='median sum_dt (H)')
    trace5 = go.Bar(x=df.index, y=df.ndays_act, name='median days active')
    trace6 = go.Bar(x=df.index, y=df.nforum_posts, name='# students posted')

    fig = tls.make_subplots(rows=1, cols=5, shared_xaxes=True, print_grid=False)
    fig.append_trace(trace1, 1, 1)
    fig.append_trace(trace2, 1, 2)
    fig.append_trace(trace3, 1, 2)
    fig.append_trace(trace4, 1, 3)
    fig.append_trace(trace5, 1, 4)
    fig.append_trace(trace6, 1, 5)

    fig['layout'].update(barmode='stack', height=300, width=850, margin=go.Margin(t=40), showlegend=False, title=title)
    fig['layout']['xaxis1'].update(title='# students', showgrid=False,
                                  titlefont=dict(size=12), tickfont=dict(size=10))
    fig['layout']['xaxis2'].update(title='median events', showgrid=False, 
                                   titlefont=dict(size=12), tickfont=dict(size=10))
    fig['layout']['xaxis3'].update(title='median sum_dt (H)', showgrid=False,
                                  titlefont=dict(size=12), tickfont=dict(size=10))
    fig['layout']['xaxis4'].update(title='median days active', showgrid=False,
                                  titlefont=dict(size=12), tickfont=dict(size=10))
    fig['layout']['xaxis5'].update(title='# students posted', showgrid=False,
                                  titlefont=dict(size=12), tickfont=dict(size=10))
    fig['layout']['yaxis1'].update(autorange='reversed', showgrid=False, tickfont=dict(size=10))
    fig['layout']['yaxis2'].update(showgrid=False, tickfont=dict(size=10))
    fig['layout']['yaxis3'].update(showgrid=False, tickfont=dict(size=10))
    fig['layout']['yaxis4'].update(showgrid=False, tickfont=dict(size=10))
    fig['layout']['yaxis5'].update(showgrid=False, tickfont=dict(size=10))

    py.iplot(fig)


# In[19]:

plot_pls(srp_agg, title="Student's engagement passed vs. learned vs. sampled")


# ###  Students' engagement grouped by goals from entry survey
# ** Entry survey**
# 
# ** What are your main reasons for taking this course? (Choose all that apply) **
# 1. Develop my understanding of an area related to my current studies or job.
# 2. Learn something interesting, challenging, or fun.
# 3. Earn credentials.
# 4. Decide whether to pursue education or a career in this topic area.
# 5. Share an experience with friends who are taking this course.
# 6. Please or impress other people.
# 7. Learn more about MOOCs.
# 8. Deep theoretical understanding of the topic.
# 9. Gain practical knowledge and useful skills.
# 10. Practice and improve my English.
# 11. Other or not sure.

# In[20]:

def query_entry(course_id = course_id):
    """
    Query entry survey;
    Example course_id: 'UBCx__Marketing1x__3T2015'
    """
    query = "Select * From [%s.entry_survey_mapped]" % course_id
    survey = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    survey = survey.drop_duplicates('user_id', keep='last').ix[:, 11:]
    survey.columns = survey.columns.str.replace("s_", "")
    return survey

def query_exit(course_id = course_id):
    """
    Query exit survey;
    Example course_id: 'UBCx__Marketing1x__3T2015'
    """   
    query = "Select * From [%s.exit_survey_mapped]" % course_id
    survey = pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    survey = survey.drop_duplicates('user_id', keep='last').ix[:, 11:]
    survey.columns = survey.columns.str.replace("s_", "")
    return survey


# In[21]:

exit = query_exit()
entry = query_entry()
surveys = pd.merge(entry, exit, on='user_id', how='inner')
# pc = query_pc()
merged = entry.merge(pc, on='user_id')


# In[22]:

def compute_pcstats(merged, selections, start):
    """
    Compute students' behavior: median events (nplay_video, nproblem_check, nforum_posts)
    meidan ndays_act, median grade, pct_passing grouped by goals;
    merged: entry survey merged with person_course
    selections: a list of all the goals from entry survey
    start: index of the first column for this question in the entry survey
    """
    df = []
    rm = []
    counts = []
    pct_passing = []
    selections2 = selections[:]
    merged_count = len(merged)*0.05
    for i in range(len(selections2)):
        group_count = merged.ix[:, i+start].notnull().sum()

        if group_count > merged_count:
            counts.append(group_count)
            pct_passing.append(merged.ix[merged.ix[:, i+start].notnull(),'certified'].sum() / group_count)
            df.append(pd.DataFrame(data={'Selection': selections[i],
                                         'grade': merged.ix[merged.ix[:, i+start].notnull(), 'grade'],
                                         'nforum_posts': merged.ix[merged.ix[:, i+start].notnull(), 'nforum_posts'],
                                         'ndays_act': merged.ix[merged.ix[:, i+start].notnull(), 'ndays_act'], 
                                         'nproblem_check': merged.ix[merged.ix[:, i+start].notnull(), 'nproblem_check'],
                                         'nplay_video': merged.ix[merged.ix[:, i+start].notnull(), 'nplay_video']
                                         }))
        else:
            rm.append(selections2[i]) 
    result = pd.concat(df)
    #result.fillna(0, inplace=True)
    for s in rm:
        selections2.remove(s)
    median = result.groupby('Selection').median().ix[selections2, :]    
    return median, counts, pct_passing 


# In[23]:

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
merged1 = merged[merged.is_active==1]
median, counts, pct_passing = compute_pcstats(merged1, selections, 10)
# median


# In[24]:

def plot_pcstats(median, counts, pct_passing, title=None):
    """
    Plot students' behavior: median events (nplay_video, nproblem_check, nforum_posts)
    meidan ndays_act, median grade, pct_passing grouped by goals;
    counts: number of students for each goal
    """
    trace1 = go.Bar(x=median.index, y=median.nplay_video, name='video plays', showlegend=True)
    trace2 = go.Bar(x=median.index, y=median.nproblem_check, name='problem checks', showlegend=True)
    trace3 = go.Bar(x=median.index, y=median.nforum_posts, name='forum posts', showlegend=True)
    trace4 = go.Bar(x=median.index, y=median.ndays_act, name='days active', showlegend=False)
    trace5 = go.Bar(x=median.index, y=median.grade, name='grade', showlegend=False)
    trace6 = go.Bar(x=median.index, y=pct_passing, name='pct passing', showlegend=False)
    trace7 = go.Bar(x=median.index, y=counts, name='# of students', showlegend=False)

    fig = tls.make_subplots(rows=5, cols=1, print_grid=False,
                              subplot_titles=('median events', 'median days active', 
                                              'median grade', 'pct passing', '# of students'))

    fig.append_trace(trace1, 1, 1)
    fig.append_trace(trace2, 1, 1)
    fig.append_trace(trace3, 1, 1)
    fig.append_trace(trace4, 2, 1)
    fig.append_trace(trace5, 3, 1)
    fig.append_trace(trace6, 4, 1)
    fig.append_trace(trace7, 5, 1)
    
    fig['layout']['xaxis1'].update(tickfont=dict(size=8))
    fig['layout']['xaxis2'].update(tickfont=dict(size=8))
    fig['layout']['xaxis3'].update(tickfont=dict(size=8))
    fig['layout']['xaxis4'].update(tickfont=dict(size=8))
    fig['layout']['xaxis5'].update(tickfont=dict(size=8))
    
    fig['layout']['yaxis1'].update(showgrid=False)
    fig['layout']['yaxis2'].update(showgrid=False)
    fig['layout']['yaxis3'].update(showgrid=False)
    fig['layout']['yaxis4'].update(showgrid=False)
    fig['layout']['yaxis5'].update(showgrid=False)

    fig['layout'].update(barmode='stack', title=title, font=dict(size=12),
                         height=920, width=600, margin=go.Margin(t=80, b=120))
    py.iplot(fig)    


# In[25]:

plot_pcstats(median, counts, pct_passing, title="Students' engagement by goals")


# ### From exit survey: grouped by goals in the entry survey
# ** Exit survey**
# 
# ** Are you likely to...: (Choose all that apply) **
# 1. Recommend this course to a friend
# 2. Take a more advance course on this topic
# 3. Revisit course components in future
# 4. Seek to increase my involvement in this topic (work opportunities, books, etc.)
# 5. Change my habits regarding climate change

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


# In[27]:

matrix = {'Q2_2_1': [], 'Q2_2_2': [], 'Q2_2_3': [], 'Q2_2_4': [], 'Q2_2_5': []}
col_names = ['Recommend', 'Advanced', 'Revisit', 'Involvement', 'Habbits']

likely = compute_heatmap(surveys, selections, matrix, 10, col_names)
#likely


# In[28]:

def heatmap(df, title=None, width=700):
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
        yaxis=dict(autorange='reversed'), 
        width=width, height=500,
        margin=go.Margin(l=100, b=120),
        title=title
    )

    fig = go.Figure(data=data, layout=layout)
    py.iplot(fig)


# In[29]:

heatmap(likely.T, title='Exit survey feedback grouped by goals')


# ** Were your goals for taking the course met? **
# - Yes and more, the course exceeded my expectations.
# - Yes, my goals were met.
# - My goals were somewhat met.

# In[30]:

def cal_goalsmet(surveys, selections, start, col_name):
    """
    Calculate stats for goalsmet question;
    surveys: entry survey merged with exit survey
    selections: a list of all the goals from entry survey
    start: index of the first column for question about goals in the merged survey
    col_name: name of goalsmet column, e.g. 'Q2_1'
    """
    
    df = []
    rm = []
    selections2 = selections[:]
    survey_count = len(surveys)*0.05
    for i in range(len(selections)):
        group_count = surveys.ix[:, i+start].notnull().sum()
        if group_count > survey_count:
            df.append(pd.DataFrame(data={'Selection': selections[i],
                                         'goalsmet': surveys.ix[surveys.ix[:, i+start].notnull(), col_name]}))
        else:
            rm.append(selections2[i])
    for s in rm:
        selections2.remove(s)
    goalsmet = pd.concat(df)
    goalsmet = goalsmet.groupby('Selection').goalsmet.value_counts(normalize=True).unstack('Selection').fillna(0).T
    col_order = ['Yes and more, the course exceeded my expectations.',
                'Yes, my goals were met.', 'My goals were somewhat met.']
    goalsmet = goalsmet.ix[selections2, col_order]
    goalsmet.loc['Overall'] = surveys[col_name].value_counts(normalize=True)[col_order]
    
    return goalsmet


# In[31]:

def stacked_bar(df, names, barmode, title=None, width=600):
    """
    Plot stacked or overlay bar graph given the dataframe
    names: names for all the traces
    """
    data = []
    colors = ['rgb(77, 175, 74)', 'rgb(255, 127, 0)', 'rgb(55, 126, 184)', 'rgb(228, 26, 28)']
    for i in range(0, df.shape[1]):
        data.append(go.Bar(x=df.index, y=df.ix[:, i], marker=dict(color=colors[i]), name=names[i]))
        
    layout = go.Layout(
    width=width,
    margin=go.Margin(b=150),
    barmode=barmode,
    title=title)
    
    fig = go.Figure(data=data, layout=layout)
    py.iplot(fig)


# In[32]:

names = ['Exceeded expection', 'Goals met', 'Goals somewhat met']
goals_met = cal_goalsmet(surveys, selections, 10, 'Q2_1')
stacked_bar(goals_met, names, 'stack', title='Percent goals met grouped by goals', width=800)


# ** Which of the following components of the course were you very satisfied with? (Choose all that apply) **
# - Challenge level
# - Workload
# - Pace
# - Depth
# - Instructor's communication
# - Instructor's responsiveness
# - Instructor's knowledge of the subject matter

# In[33]:

satisfy = {'Q4_1_1': [], 'Q4_1_2': [], 'Q4_1_3': [], 'Q4_1_4': [], 'Q4_1_5': [], 'Q4_1_6': [], 'Q4_1_7': []}
col_names = ['challenge', 'workload', 'pace', 'depth', 'communication', 'responsiveness', 'knowledge']
satisfy = compute_heatmap(surveys, selections, satisfy, 10, col_names)
#satisfy


# In[34]:

heatmap(satisfy.T, title='Percent satisfaction of course components grouped by goals')

